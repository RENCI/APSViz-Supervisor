# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    This module creates and workflows APSViz processes as defined in the database.

    Author: Phil Owen, RENCI.org
"""

import time
import os
import json
import datetime as dt

from src.supervisor.job_create import JobCreate
from src.supervisor.job_find import JobFind
from src.common.pg_impl import PGImplementation
from src.common.logger import LoggingUtil
from src.common.job_enums import JobType, JobStatus
from src.common.utils import Utils


class JobSupervisor:
    """
    Class for the APSViz supervisor

    """

    def __init__(self):
        """
        inits the class

        """
        # get the app version
        self.app_version: str = os.getenv('APP_VERSION', 'Version number not set')

        # get the environment this instance is running on
        self.system: str = os.getenv('SYSTEM', 'System name not set')

        # get the log level and directory from the environment.
        log_level, log_path = LoggingUtil.prep_for_logging()

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Supervisor.Jobs", level=log_level, line_format='medium', log_file_path=log_path)

        # init the list of pending runs. this stores all job details of the run
        self.run_list: list = []

        # load the base configuration params
        self.k8s_base_config: dict = Utils.get_base_config()

        # init the running count of active runs
        self.run_count: int = 0

        # init the k8s job configuration storage
        self.k8s_job_configs: dict = {}

        # specify the DB to get a connection
        # note the extra comma makes this single item a singleton tuple
        db_names: tuple = ('asgs',)

        # assign utility objects
        self.util_objs: dict = {'create': JobCreate(), 'k8s_find': JobFind(), 'pg_db': PGImplementation(db_names, _logger=self.logger),
                                'utils': Utils(self.logger, self.system, self.app_version)}

        # init the run params to look for list
        self.required_run_params = ['supervisor_job_status', 'downloadurl', 'adcirc.gridname', 'instancename', 'stormnumber',
                                    'physical_location']

        # debug options
        self.debug_options: dict = {'pause_mode': True, 'fake_job': False}

        # init the last time a run completed
        self.last_run_time = dt.datetime.now()

        # declare ready
        self.logger.info('The APSViz Job Supervisor:%s (%s) has initialized...', self.app_version, self.system)

    def get_job_configs(self) -> dict:
        """
        gets the job configurations

        :return: Dict, baseline run params
        """
        # get all the job parameter definitions
        db_data = self.util_objs['pg_db'].get_job_defs()

        # init the return
        job_config_data: dict = {}

        # make sure we got a list of config data items
        if isinstance(db_data, list):
            # for each step in the workflow
            for workflow_item in db_data:
                # get the workflow type name
                workflow_type = list(workflow_item.keys())[0]

                # get the data looking like something we are used to
                job_config_data[workflow_type] = {list(x)[0]: x.get(list(x)[0]) for x in workflow_item[workflow_type]}

                # fix the arrays for each job def. they come in as a string
                for item in job_config_data[workflow_type].items():
                    item[1]['COMMAND_LINE'] = json.loads(item[1]['COMMAND_LINE'])
                    item[1]['COMMAND_MATRIX'] = json.loads(item[1]['COMMAND_MATRIX'])
                    item[1]['PARALLEL'] = [JobType(x) for x in json.loads(item[1]['PARALLEL'])] if item[1]['PARALLEL'] is not None else None

        # return the config data
        return job_config_data

    def run(self):
        """
        endless loop processing run requests

        :return: nothing
        """
        # init counter that indicates how many times nothing was launched.
        # used to slow down the checks that look for work
        no_activity_counter: int = 0

        # until the end of time
        while True:
            # get the incomplete runs from the database
            self.get_incomplete_runs()

            # reset the activity flag
            no_activity: bool = True

            # for each run returned from the database
            for run in self.run_list:
                # catch cleanup exceptions
                try:
                    # skip this job if it is complete
                    if run['job-type'] == JobType.COMPLETE:
                        self.handle_job_complete(run)

                        # continue processing
                        continue

                    # or an error
                    if run['job-type'] == JobType.ERROR:
                        self.handle_job_error(run)

                        # continue processing runs
                        continue
                except Exception:
                    # report the exception
                    self.logger.exception("Cleanup exception detected, id: %s", run['id'])

                    msg = 'Exception caught. Terminating run.'

                    # send the message
                    self.util_objs['utils'].send_slack_msg(run['id'], msg, 'slack_issues_channel', run['debug'], run['instance_name'])

                    # remove the run
                    self.run_list.remove(run)

                    # continue processing runs
                    continue

                # catch handling the run exceptions
                try:
                    # handle the run
                    no_activity = self.handle_run(run)
                except Exception:
                    # report the exception
                    self.logger.exception("Run handler exception detected, id: %s", run['id'])

                    # prepare the DB status
                    run['status_prov'] += ', Run handler error detected'
                    self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

                    # delete the k8s job if it exists
                    job_del_status = self.util_objs['create'].delete_job(run)

                    # if there was a job error
                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error("Error failed %s run. Run ID: %s, Job type: %s, job delete status: %s", run['physical_location'], run['id'],
                                          run['job-type'], job_del_status)

                    # set error conditions
                    run['job-type'] = JobType.ERROR
                    run['status'] = JobStatus.ERROR

                    # continue processing runs
                    continue

            # output the current number of runs in progress if there are any
            if self.run_count != len(self.run_list):
                # save the new run count
                self.run_count = len(self.run_list)
                msg = f'There {"are" if self.run_count != 1 else "is"} {self.run_count} ' \
                      f'run{"s" if self.run_count != 1 else ""} in progress.'
                self.logger.info(msg)

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1

                # check to see if it has been too long for a run
                self.last_run_time = self.util_objs['utils'].check_last_run_time(self.last_run_time)
            else:
                # clear the counter
                no_activity_counter = 0

                # reset the last run timer
                self.last_run_time = dt.datetime.now()

            # check for something to do after a period of time
            if no_activity_counter >= self.k8s_base_config.get("MAX_NO_ACTIVITY_COUNT"):
                # set the sleep timeout
                sleep_timeout = self.k8s_base_config.get("POLL_LONG_SLEEP")

                # try again at this poll rate
                no_activity_counter = self.k8s_base_config.get("MAX_NO_ACTIVITY_COUNT") - 1
            else:
                # set the sleep timeout
                sleep_timeout = self.k8s_base_config.get("POLL_SHORT_SLEEP")

            self.logger.debug("All active run checks complete. Sleeping for %s minutes.", sleep_timeout / 60)

            # wait for the next check for something to do
            time.sleep(sleep_timeout)

    def handle_job_error(self, run: dict):
        """
        handles the job state when it is marked as in error

        :param run:
        :return:
        """
        # does this run have a final staging step
        if 'final-staging' not in self.k8s_job_configs[run['workflow_type']]:
            self.logger.error("Error detected for a %s run of type %s. Run id: %s", run['physical_location'], run['workflow_type'], run['id'])
            run['status_prov'] += f", error detected in a {run['physical_location']} run of type {run['workflow_type']}. No cleanup occurred."

            # set error conditions
            run['job-type'] = JobType.COMPLETE
            run['status'] = JobStatus.ERROR
        # if this was a final staging run that failed force complete
        elif 'final-staging' in run:
            self.logger.error("Error detected for a %s run in final staging with run id: %s", run['physical_location'], run['id'])
            run['status_prov'] += f", error detected for a {run['physical_location']} run in final staging. An incomplete cleanup may have occurred."

            # set error conditions
            run['job-type'] = JobType.COMPLETE
            run['status'] = JobStatus.ERROR
        # else try to clean up
        else:
            self.logger.error(f"Error detected for a {run['physical_location']} run. About to clean up of intermediate files. Run id: %s", run['id'])
            run['status_prov'] += ', error detected'

            # set the type to clean up
            run['job-type'] = JobType.FINAL_STAGING
            run['status'] = JobStatus.NEW

        # report the issue
        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

    def handle_job_complete(self, run: dict):
        """
        handles the job state when it is marked complete

        :param run:
        :return:
        """
        # get the run duration
        duration = Utils.get_run_time_delta(run)

        # update the run provenance in the DB
        run['status_prov'] += f', run complete {duration}'
        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

        # init the type of run
        run_type = f"APS ({run['workflow_type']})"

        # add a comment on overall pass/fail
        if run['status_prov'].find('error') == -1:
            msg = f"*{run['physical_location']} {run_type} run completed successfully {duration}*"
            emoticon = ':100:'

        else:
            msg = f"*{run['physical_location']} {run_type} run completed unsuccessfully {duration}*"
            emoticon = ':boom:'
            self.util_objs['utils'].send_slack_msg(run['id'], f"{msg}\nRun provenance: {run['status_prov']}.", 'slack_issues_channel', run['debug'],
                                                   run['instance_name'], emoticon)
        # send the message
        self.util_objs['utils'].send_slack_msg(run['id'], msg, 'slack_status_channel', run['debug'], run['instance_name'], emoticon)

        # send something to the log to indicate complete
        self.logger.info("%s complete.", run['id'])

        # remove the run
        self.run_list.remove(run)

    def get_base_command_line(self, run: dict, job_type: JobType) -> (list, bool):
        """
        gets the command lines for each run type
        note: use this to keep a pod running after command_line and command_matrix for the job have
        been set to '[""]' in the DB also note that the supervisor should be terminated prior to
        killing the job to avoid data directory removal (if that matters)
        command_line_params = ['/bin/sh', '-c', 'while true; do date; sleep 3600; done']
        update public."ASGS_Mon_supervisor_config" set command_line='[""]', command_matrix='[""]'
        where id=;

        :param run: the run parameters
        :param job_type:
        :return: a list of the command line parameters
        """
        # init the returns
        command_line_params = None
        extend_output_path = False

        # get the proper job configs
        job_configs = self.k8s_job_configs[run['workflow_type']]

        # is this a staging job array
        if job_type == JobType.STAGING:
            command_line_params = ['--inputURL', run['downloadurl'], '--isHurricane', run['stormnumber'], '--outputDir']
            extend_output_path = True

        # is this a hazus job array
        elif job_type == JobType.HAZUS:
            command_line_params = ['--downloadurl', run['downloadurl'], '--datadir', job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id'])]

        # is this an adcirc2cog_tiff job array
        elif job_type == JobType.ADCIRC2COG_TIFF:
            command_line_params = ['--inputDIR', job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input', '--outputDIR',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + job_configs[job_type]['SUB_PATH'], '--inputFile']

        # is this a geotiff2cog job array
        elif job_type == JobType.GEOTIFF2COG:
            command_line_params = ['--inputDIR', job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/cogeo', '--finalDIR',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + job_configs[job_type][
                                       'SUB_PATH'], '--inputParam']

        # is this a geo server load job array
        elif job_type == JobType.LOAD_GEO_SERVER:
            command_line_params = ['--instanceId', str(run['id'])]

        # is this a geo server load s3 job array
        elif job_type == JobType.LOAD_GEO_SERVER_S3:
            command_line_params = ['--instanceId', str(run['id']), '--HECRAS_URL', run['downloadurl']]

        # is this a final staging job array
        elif job_type == JobType.FINAL_STAGING:
            command_line_params = ['--inputDir', job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + job_configs[job_type]['SUB_PATH'],
                                   '--outputDir', job_configs[job_type]['DATA_MOUNT_PATH'] + job_configs[job_type]['SUB_PATH'], '--tarMeta',
                                   str(run['id'])]

        # is this an obs mod ast job
        elif job_type == JobType.OBS_MOD_AST:
            thredds_url = run['downloadurl'] + '/fort.63.nc'
            thredds_url = thredds_url.replace('fileServer', 'dodsC')

            # create the additional command line parameters
            command_line_params = [thredds_url, run['gridname'],
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + job_configs[job_type][
                                       'ADDITIONAL_PATH'], str(run['id'])]

        # is this an ast run harvester job
        elif job_type == JobType.AST_RUN_HARVESTER:
            thredds_url = run['downloadurl'] + '/fort.63.nc'
            thredds_url = thredds_url.replace('fileServer', 'dodsC')

            # [["--noaa", "--nowcast"], ["--noaa"], ["--ndbc", "--nowcast"], ["--ndbc"], ["--contrails_rivers", "--nowcast"], ["--contrails_rivers"],
            #  ["--contrails_coastal", "--nowcast"], ["--contrails_coastal"]]

            # create the additional command line parameters
            command_line_params = [thredds_url, job_configs[job_type]['DATA_MOUNT_PATH'] + job_configs[job_type]['SUB_PATH']]

        # is this an adcirc time to cog converter job array
        elif job_type == JobType.ADCIRCTIME_TO_COG:
            command_line_params = ['--inputDIR', job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input', '--outputDIR',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + job_configs[job_type]['SUB_PATH'], '--finalDIR',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + job_configs[job_type][
                                       'SUB_PATH'], '--inputFile']

        # is this a collaborator data sync job
        elif job_type == JobType.COLLAB_DATA_SYNC:
            command_line_params = ['--run_id', str(run['id']), '--physical_location', str(run['physical_location'])]

        # is this an adcirc to kalpana cog job
        elif job_type == JobType.ADCIRC_TO_KALPANA_COG:
            command_line_params = ['--modelRunID', str(run['id'])]

        # is this a timeseries DB ingest job
        elif job_type == JobType.TIMESERIESDB_INGEST:
            command_line_params = ['--modelRunID', str(run['id'])]

        # return the command line and extend the path flag
        return command_line_params, extend_output_path

    def handle_run(self, run: dict) -> bool:
        """
        handles the run processing

        :param run: the run parameters
        :return: boolean run activity indicator
        """
        # init the activity flag
        no_activity: bool = True

        # get the proper job configs
        job_configs = self.k8s_job_configs[run['workflow_type']]

        # work the current state
        if run['status'] == JobStatus.NEW:
            # set the activity flag
            no_activity = False

            # create a list of jobs to create
            job_type_list: list = [run['job-type']]

            # append any parallel jobs if they exist
            if 'PARALLEL' in job_configs[run['job-type']] and job_configs[run['job-type']]['PARALLEL']:
                job_type_list.extend(job_configs[run['job-type']]['PARALLEL'])

            # create jobs form items in the list
            for job_type in job_type_list:
                # get the data by the download url
                command_line_params, extend_output_path = self.get_base_command_line(run, job_type)

                # create a new run configuration for the step
                self.k8s_create_run_config(run, job_type, command_line_params, extend_output_path)

                # execute the k8s job run
                job_id = self.util_objs['create'].execute(run, job_type)

                # did we not get a job_id
                if job_id is not None:
                    # set the current status
                    run['status'] = JobStatus.RUNNING
                    run['status_prov'] += f", {job_type.value} running"
                    self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

                    self.logger.info("A %s job was created. Run ID: %s, Job type: %s", run['physical_location'], run['id'], job_type)
                else:
                    # set the error status
                    run['status'] = JobStatus.ERROR

                    self.logger.info("A %s job was not created. Run ID: %s, Job type: %s", run['physical_location'], run['id'], job_type)

                # if the next job is complete there is no reason to keep adding more jobs
                if job_configs[job_type.value]['NEXT_JOB_TYPE'] == JobType.COMPLETE.value:
                    break

        # if the job is running check the status
        if run['status'] == JobStatus.RUNNING and run['status'] != JobStatus.ERROR:
            # set the activity flag
            no_activity = False

            # find the job, get the status
            job_found, job_status, pod_status = self.util_objs['k8s_find'].find_job_info(run)

            # check the job status, report any issues
            if not job_found:
                self.logger.error("Error: A %s job not found. Run ID: %s, Job type: %s", run['physical_location'], run['id'], run['job-type'])
            elif job_status.startswith('Timeout'):
                self.logger.error("Error: A %s job has timed out. Run ID: %s, Job type: %s", run['physical_location'], run['id'], run['job-type'])
            elif job_status.startswith('Failed'):
                self.logger.error("Error: A %s job has failed. Run ID: %s, Job type: %s", run['physical_location'], run['id'], run['job-type'])
                run['status_prov'] += f", {run['job-type'].value} failed"
            elif job_status.startswith('Complete'):
                self.logger.info("A %s job has completed. Run ID: %s, Job type: %s", run['physical_location'], run['id'], run['job-type'])

            # if the job was found
            if job_found:
                # did the job timeout (presumably waiting for resources) or failed
                if job_status.startswith('Timeout') or job_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.util_objs['create'].delete_job(run)

                    # set error conditions
                    run['status'] = JobStatus.ERROR
                # did the job and pod succeed
                elif job_status.startswith('Complete') and not pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.util_objs['create'].delete_job(run)

                    # was there an error on the job
                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error("Error: A failed %s job detected. Run status %s. Run ID: %s, Job type: %s, job delete status: %s, "
                                          "pod status: %s", run['physical_location'], run['status'], run['id'], run['job-type'], job_del_status,
                                          pod_status)

                        # set error conditions
                        run['status'] = JobStatus.ERROR
                    else:
                        # complete this job and setup for the next job
                        run['status_prov'] += f", {run['job-type'].value} complete"
                        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

                        # prepare for next stage
                        run['job-type'] = JobType(run[run['job-type'].value]['run-config']['NEXT_JOB_TYPE'])

                        # if the job type is not in the run then let it be created
                        if run['job-type'] not in run or run['job-type'] == JobType.STAGING:
                            # this bit is mostly for troubleshooting when the steps have been set
                            # into a loop back to staging. if so, remove all other job types that
                            # may have run
                            for i in run.copy():
                                if isinstance(i, JobType) and i is not JobType.STAGING:
                                    run.pop(i)

                            # set the job to new
                            run['status'] = JobStatus.NEW

                # was there a failure. remove the job and declare failure
                elif pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.util_objs['create'].delete_job(run)

                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error("Error: A failed %s job and/or pod detected. Run status: %s. Run ID: %s, Job type: %s, job delete status: "
                                          "%s, pod status: %s.", run['physical_location'], run['status'], run['id'], run['job-type'], job_del_status,
                                          pod_status)

                    # set error conditions
                    run['status'] = JobStatus.ERROR
            else:
                self.logger.error("Error: A %s job not found: Run ID: %s, Run status: %s, Job type: %s", run['physical_location'], run['id'],
                                  run['status'], run['job-type'])

                # set error condition
                run['status'] = JobStatus.ERROR

        # send out the error status if an error was detected
        if run['status'] == JobStatus.ERROR:
            run['job-type'] = JobType.ERROR

        # return to the caller
        return no_activity

    def k8s_create_run_config(self, run: dict, job_type: JobType, command_line_params: list, extend_output_path: bool = False):
        """
        Creates the configuration details for a job from the database

        :param run:
        :param job_type:
        :param command_line_params:
        :param extend_output_path:

        :return: nothing
        """
        # get the job type config
        config = self.get_job_configs()[run['workflow_type']][job_type]

        # load the config with the info from the config file
        config['JOB_NAME'] += str(run['id']).lower()
        config['DATA_VOLUME_NAME'] += str(run['id']).lower()
        config['COMMAND_LINE'].extend(command_line_params)

        # tack on any additional paths if requested
        if extend_output_path:
            config['SUB_PATH'] = '/' + str(run['id']) + config['SUB_PATH']
            config['COMMAND_LINE'].extend([config['DATA_MOUNT_PATH'] + config['SUB_PATH'] + config['ADDITIONAL_PATH']])

        self.logger.debug("Job command line. Run ID: %s, Job type: %s, Command line: %s", run['id'], job_type, config['COMMAND_LINE'])

        # save these params in the run info
        run[job_type] = {'run-config': config}

    def check_input_params(self, run_info: dict) -> (str, str, bool):
        """
        Checks the run data to insure we have all the necessary info to start a run

        :param run_info:
        :return: list of required items that weren't found
        """
        # if there was an instance name use it
        if 'instancename' in run_info:
            instance_name = run_info['instancename']
        else:
            instance_name = None

        # if there is a special k8s download url in the data use it.
        if 'post.opendap.renci_tds-k8.downloadurl' in run_info:
            # use the service name and save it for the run. force the apsviz thredds url -> https:
            run_info['downloadurl'] = run_info['post.opendap.renci_tds-k8.downloadurl'].replace('http://apsviz-thredds', 'https://apsviz-thredds')

        # interrogate and set debug mode
        debug_mode = ('supervisor_job_status' in run_info and run_info['supervisor_job_status'].startswith('debug'))

        # get the workflow type
        if 'workflow_type' in run_info:
            workflow_type = run_info['workflow_type']
        # if there is no workflow type default to ASGS legacy runs
        else:
            workflow_type = 'ASGS'

        # get the physical location of the cluster that initiated the run
        if 'physical_location' in run_info:
            physical_location = run_info['physical_location']
        else:
            physical_location = ''

        # if the storm number doesn't exist default it
        if 'stormnumber' not in run_info:
            run_info['stormnumber'] = 'NA'

        # loop through the params and return the ones that are missing
        return f"{', '.join([run_param for run_param in self.required_run_params if run_param not in run_info])}", instance_name, debug_mode, \
            workflow_type, physical_location

    def check_for_duplicate_run(self, new_run_id: str) -> bool:
        """
        checks to see if this run is already in progress

        :param new_run_id:
        :return:
        """

        # init found flag, presume not found
        ret_val: bool = False

        # loop through the current run list
        for item in self.run_list:
            # was it found
            if item['id'] == new_run_id:
                # set the flag
                ret_val = True

        # return to the caller
        return ret_val

    def get_incomplete_runs(self):
        """
        get the list of instances that need processing

        :return: nothing
        """
        # init the storage for the new runs
        runs = None

        # init the debug flag
        debug_mode: bool = False

        # get the latest job definitions
        self.k8s_job_configs = self.get_job_configs()

        # make sure we got the config to continue
        if self.k8s_job_configs is not None:
            # check to see if we are in pause mode
            runs = self.check_pause_status()

            # did we find anything to do
            if runs is not None:
                # add the runs to the list
                for run in runs:
                    # save the run id that was provided by the DB run.properties data
                    run_id = run['run_id']

                    # check for a duplicate run
                    if not self.check_for_duplicate_run(run_id):
                        # make sure all the needed params are available. instance name and debug mode
                        # are handled here because they both affect messaging and logging.
                        missing_params_msg, instance_name, debug_mode, workflow_type, physical_location = self.check_input_params(run['run_data'])

                        # check the run params to see if there is something missing
                        if len(missing_params_msg) > 0:
                            # update the run status everywhere
                            self.util_objs['pg_db'].update_job_status(run_id, f"Error - Run lacks the required run properties "
                                                                              f"({missing_params_msg}).")
                            self.logger.error("Error: A %s run lacks the required run properties (%s): %s", physical_location, missing_params_msg,
                                              run_id)
                            self.util_objs['utils'].send_slack_msg(run_id, f"Error - Run lacks the required run properties ({missing_params_msg}) "
                                                                           f"for a {physical_location} run.", 'slack_issues_channel', debug_mode,
                                                                   instance_name)

                            # continue processing the remaining runs
                            continue

                        # if this is a new run
                        if run['run_data']['supervisor_job_status'].startswith('new'):
                            job_prov = f'New {physical_location} APS ({workflow_type})'
                        # if we are in debug mode
                        elif run['run_data']['supervisor_job_status'].startswith('debug'):
                            job_prov = f'New debug {physical_location} APS ({workflow_type})'
                        # ignore the entry as it is not in a legit "start" state. this may just
                        # be an existing or completed run.
                        else:
                            self.logger.info("Error: Unrecognized %s run command %s for Run ID %s", physical_location,
                                             run['run_data']['supervisor_job_status'], run_id)
                            continue

                        # get the first job for this workflow type
                        first_job = self.util_objs['pg_db'].get_first_job(workflow_type)

                        # did we get a job type
                        if first_job is not None:
                            # get the first job name into a type
                            job_type = JobType(first_job)
                        else:
                            self.logger.info("Error: Could not find the first %s job in the %s workflow for run id: %s", physical_location,
                                             workflow_type, run_id)
                            continue

                        # add the new run to the list
                        self.run_list.append({'id': run_id, 'workflow_type': workflow_type, 'stormnumber': run['run_data']['stormnumber'],
                                              'debug': debug_mode, 'fake-jobs': self.debug_options['fake_job'], 'job-type': job_type,
                                              'status': JobStatus.NEW, 'status_prov': f'{job_prov} run accepted',
                                              'downloadurl': run['run_data']['downloadurl'], 'gridname': run['run_data']['adcirc.gridname'],
                                              'instance_name': run['run_data']['instancename'], 'run-start': dt.datetime.now(),
                                              'physical_location': physical_location})

                        # update the run status in the DB
                        self.util_objs['pg_db'].update_job_status(run_id, f'{job_prov} run accepted')

                        # notify Slack
                        self.util_objs['utils'].send_slack_msg(run_id, f'{job_prov} run accepted.', 'slack_status_channel', debug_mode,
                                                               run['run_data']['instancename'], ':rocket:')
                    else:
                        # update the run status in the DB
                        self.util_objs['pg_db'].update_job_status(run_id, 'Duplicate run rejected.')

                        # notify Slack
                        self.util_objs['utils'].send_slack_msg(run_id, 'Duplicate run rejected.', 'slack_status_channel', debug_mode,
                                                               run['run_data']['instancename'], ':boom:')

    def check_pause_status(self) -> dict:
        """
        checks to see if we are in pause mode. if the system isn't, get new runs.

        :return: returns dict of newly discovered runs
        """
        # init the return value
        runs = None

        # get the flag that indicates we are pausing the handling of new run requests
        pause_mode = os.path.exists(os.path.join(os.path.dirname(__file__), '../', '../', str('pause')))

        # are we toggling pause mode
        if pause_mode != self.debug_options['pause_mode']:
            # save the new pause mode
            self.debug_options['pause_mode'] = pause_mode

            # let everyone know pause mode was toggled
            self.util_objs['utils'].send_slack_msg(None, f'Application is now {"paused" if pause_mode else "active"}.', 'slack_status_channel')

        # get all the new runs if system is not in pause mode
        if not pause_mode:
            # get the new runs
            runs = self.util_objs['pg_db'].get_new_runs()

        # return to the caller
        return runs
