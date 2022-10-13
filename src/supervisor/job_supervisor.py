# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
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
from src.supervisor.job_create import JobCreate
from src.supervisor.job_find import JobFind
from src.common.pg_utils import PGUtils
from src.common.logger import LoggingUtil
from src.common.job_enums import JobType, JobStatus
from src.common.utils import Utils


class APSVizSupervisor:
    """
    Class for the APSViz supervisor

    """

    def __init__(self):
        """
        inits the class
        """
        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.APSVizSupervisor", line_format='medium')

        # get the environment this instance is running on
        self.system = os.getenv('SYSTEM', 'System name not set')

        # the list of pending runs. this stores all job details of the run
        self.run_list = []

        # set the polling parameter values
        self.polling_params = {'poll_short_sleep': 10, 'poll_long_sleep': 60, 'max_no_activity_count': 60, 'run_count': 0}

        # load the run configuration params
        self.k8s_config: dict = {}

        # utility objects
        self.util_objs: dict = {'create': JobCreate(), 'k8s_find': JobFind(), 'pg_db': PGUtils(), 'utils': Utils(self.logger, self.system)}

        # init the run params to look for list
        self.required_run_params = ['supervisor_job_status', 'downloadurl', 'adcirc.gridname', 'instancename', 'forcing.stormname']

        # debug options
        self.debug_options: dict = {'pause_mode': True, 'fake_job': False}

        # declare ready
        self.logger.info('K8s Supervisor (%s) has started...', self.system)

    def get_job_config(self) -> dict:
        """
        gets the run's job configuration

        :return: Dict, baseline run params
        """
        # get all the job parameter definitions
        db_data = self.util_objs['pg_db'].get_job_defs()

        # init the return
        config_data = None

        # make sure we got a list of config data items
        if isinstance(db_data, list):
            # get the data looking like something we are used to
            config_data = {list(x)[0]: x.get(list(x)[0]) for x in db_data}

            # fix the arrays for each job def. they come in as a string
            for item in config_data.items():
                item[1]['COMMAND_LINE'] = json.loads(item[1]['COMMAND_LINE'])
                item[1]['COMMAND_MATRIX'] = json.loads(item[1]['COMMAND_MATRIX'])
                item[1]['PARALLEL'] = [JobType(x) for x in json.loads(item[1]['PARALLEL'])] if item[1]['PARALLEL'] is not None else None

        # return the config data
        return config_data

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
                        self.logger.error("Error failed job. Run ID: %s, Job type: %s, job delete status: %s", run['id'], run['job-type'],
                                          job_del_status)

                    # set error conditions
                    run['job-type'] = JobType.ERROR
                    run['status'] = JobStatus.ERROR

                    # continue processing runs
                    continue

            # output the current number of runs in progress if there are any
            if self.polling_params["run_count"] != len(self.run_list):
                # save the new run count
                self.polling_params['run_count'] = len(self.run_list)
                msg = f'There {"are" if self.polling_params["run_count"] != 1 else "is"} {self.polling_params["run_count"]} ' \
                      f'run{"s" if self.polling_params["run_count"] != 1 else ""} in progress.'
                self.logger.info(msg)

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1
            else:
                no_activity_counter = 0

            # check for something to do after a period of time
            if no_activity_counter >= self.polling_params['max_no_activity_count']:
                # set the sleep timeout
                sleep_timeout = self.polling_params['poll_long_sleep']

                # try again at this poll rate
                no_activity_counter = self.polling_params['max_no_activity_count'] - 1
            else:
                # set the sleep timeout
                sleep_timeout = self.polling_params['poll_short_sleep']

            self.logger.debug("All active run checks complete. Sleeping for %s minutes.", sleep_timeout / 60)

            # wait for the next check for something to do
            time.sleep(sleep_timeout)

    def handle_job_error(self, run):
        """
        handles the job state when it is marked as in error

        :param run:
        :return:
        """
        # if this was a final staging run that failed force complete
        if 'final-staging' in run:
            self.logger.error("Error detected in final staging for run id: %s", run['id'])
            run['status_prov'] += ", error detected in final staging. An incomplete cleanup may have occurred"

            # set error conditions
            run['job-type'] = JobType.COMPLETE
            run['status'] = JobStatus.ERROR
        # else try to clean up
        else:
            self.logger.error("Error detected: About to clean up of intermediate files. Run id: %s", run['id'])
            run['status_prov'] += ', error detected'

            # set the type to clean up
            run['job-type'] = JobType.FINAL_STAGING
            run['status'] = JobStatus.NEW

        # report the issue
        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

    def handle_job_complete(self, run):
        """
        handles the job state when it is marked complete

        :param run:
        :return:
        """
        run['status_prov'] += ', run complete'
        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

        # init the type of run
        run_type = 'APS'

        # add a comment on overall pass/fail
        if run['status_prov'].find('error') == -1:
            msg = f'*{run_type} run completed successfully* :100:'
        else:
            msg = f"*{run_type} run completed unsuccessfully* :boom:"
            self.util_objs['utils'].send_slack_msg(run['id'], f"{msg}\nRun provenance: {run['status_prov']}.", 'slack_issues_channel', run['debug'],
                                                   run['instance_name'])
        # send the message
        self.util_objs['utils'].send_slack_msg(run['id'], msg, 'slack_status_channel', run['debug'], run['instance_name'])

        # send something to log to indicate complete
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

        # is this a staging job array
        if job_type == JobType.STAGING:
            command_line_params = ['--inputURL', run['downloadurl'], '--isHurricane', run['forcing.stormname'], '--outputDir']
            extend_output_path = True

        # is this a hazus job array
        elif job_type == JobType.HAZUS:
            command_line_params = [run['downloadurl'], self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id'])]

        # is this an adcirc2cog_tiff job array
        elif job_type == JobType.ADCIRC2COG_TIFF:
            command_line_params = ['--inputDIR', self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input', '--outputDIR',
                                   self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[job_type]['SUB_PATH'],
                                   '--inputFile']

        # is this a geotiff2cog job array
        elif job_type == JobType.GEOTIFF2COG:
            command_line_params = ['--inputDIR', self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/cogeo', '--finalDIR',
                                   self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[job_type][
                                       'SUB_PATH'], '--inputParam']

        # is this a geo server load job array
        elif job_type == JobType.LOAD_GEO_SERVER:
            command_line_params = ['--instanceId', str(run['id'])]

        # is this a final staging job array
        elif job_type == JobType.FINAL_STAGING:
            command_line_params = ['--inputDir',
                                   self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[job_type]['SUB_PATH'],
                                   '--outputDir', self.k8s_config[job_type]['DATA_MOUNT_PATH'] + self.k8s_config[job_type]['SUB_PATH'], '--tarMeta',
                                   str(run['id'])]

        # is this an obs mod ast job
        elif job_type == JobType.OBS_MOD_AST:
            thredds_url = run['downloadurl'] + '/fort.63.nc'
            thredds_url = thredds_url.replace('fileServer', 'dodsC')

            # create the additional command line parameters
            command_line_params = [thredds_url, run['gridname'],
                                   self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[job_type][
                                       'ADDITIONAL_PATH'], str(run['id'])]

        # is this an ast run harvester job
        elif job_type == JobType.AST_RUN_HARVESTER:
            thredds_url = run['downloadurl'] + '/fort.63.nc'
            thredds_url = thredds_url.replace('fileServer', 'dodsC')

            # create the additional command line parameters
            command_line_params = [thredds_url, self.k8s_config[job_type]['DATA_MOUNT_PATH'] + self.k8s_config[job_type]['SUB_PATH']]

        # is this an adcirc time to cog converter job array
        elif job_type == JobType.ADCIRCTIME_TO_COG:
            command_line_params = ['--inputDIR', self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input', '--outputDIR',
                                   self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[job_type]['SUB_PATH'],
                                   '--finalDIR',
                                   self.k8s_config[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[job_type][
                                       'SUB_PATH'], '--inputFile']

        # return the command line and extend the path flag
        return command_line_params, extend_output_path

    def handle_run(self, run) -> bool:
        """
        handles the run processing

        :param run: the run parameters
        :return: boolean run activity indicator
        """
        # init the activity flag
        no_activity: bool = True

        # work the current state
        if run['status'] == JobStatus.NEW:
            # set the activity flag
            no_activity = False

            # create a list of jobs to create
            job_type_list: list = [run['job-type']]

            # append any parallel jobs if they exist
            if self.k8s_config[run['job-type']]['PARALLEL']:
                job_type_list.extend(self.k8s_config[run['job-type']]['PARALLEL'])

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

                    self.logger.info("Job created. Run ID: %s, Job type: %s", run['id'], job_type)
                else:
                    # set the error status
                    run['status'] = JobStatus.ERROR

                    self.logger.info("Job was not created. Run ID: %s, Job type: %s", run['id'], job_type)

                # if the next job is complete there is no reason to keep adding more jobs
                if self.k8s_config[job_type.value]['NEXT_JOB_TYPE'] == JobType.COMPLETE.value:
                    break

        # if the job is running check the status
        if run['status'] == JobStatus.RUNNING and run['status'] != JobStatus.ERROR:
            # set the activity flag
            no_activity = False

            # find the job, get the status
            job_found, job_status, pod_status = self.util_objs['k8s_find'].find_job_info(run)

            # check the job status, report any issues
            if not job_found:
                self.logger.error("Job not found. Run ID: %s, Job type: %s", run['id'], run['job-type'])
            elif job_status.startswith('Timeout'):
                self.logger.error("Job has timed out. Run ID: %s, Job type: %s", run['id'], run['job-type'])
            elif job_status.startswith('Failed'):
                self.logger.error("Job has failed. Run ID: %s, Job type: %s", run['id'], run['job-type'])
                run['status_prov'] += f", {run['job-type'].value} failed"
            elif job_status.startswith('Complete'):
                self.logger.info("Job has completed. Run ID: %s, Job type: %s", run['id'], run['job-type'])

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
                        self.logger.error("Error failed job. Run status %s. Run ID: %s, Job type: %s, job delete status: %s, pod status: %s",
                                          run['status'], run['id'], run['job-type'], job_del_status, pod_status)

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
                        self.logger.error(
                            "Error failed job and/or pod. Run status: %s. Run ID: %s, Job type: %s, job delete status: %s, pod status: %s.",
                            run['status'], run['id'], run['job-type'], job_del_status, pod_status)

                    # set error conditions
                    run['status'] = JobStatus.ERROR
            else:
                self.logger.error("Error job not found: Run status: %s, Run ID: %s, Job type: %s", run['status'], run['id'], run['job-type'])

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
        config = self.get_job_config()[job_type]

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

        # loop through the params and return the ones that are missing
        return f"{', '.join([run_param for run_param in self.required_run_params if run_param not in run_info])}", instance_name, debug_mode

    def get_incomplete_runs(self):
        """
        get the list of instances that need processing

        :return: nothing
        """
        # init the storage for the new runs
        runs = None

        # get the latest job definitions
        self.k8s_config = self.get_job_config()

        # make sure we got the config to continue
        if self.k8s_config is not None:
            # check to see if we are in pause mode
            runs = self.check_pause_status(runs)

            # did we find anything to do
            if runs is not None:
                # add this run to the list
                for run in runs:
                    # save the run id that was provided by the DB run.properties data
                    run_id = run['run_id']

                    # make sure all the needed params are available. instance name and debug mode are handled here
                    # because they both affect messaging and logging.
                    missing_params_msg, instance_name, debug_mode = self.check_input_params(run['run_data'])

                    # check the run params to see if there is something missing
                    if len(missing_params_msg) > 0:
                        # update the run status everywhere
                        self.util_objs['pg_db'].update_job_status(run_id, f"Error - Run lacks the required run properties ({missing_params_msg}).")
                        self.logger.error("Error - Run lacks the required run properties (%s): %s", missing_params_msg, run_id)
                        self.util_objs['utils'].send_slack_msg(run_id, f"Error - Run lacks the required run properties ({missing_params_msg})",
                                                               'slack_issues_channel', debug_mode, instance_name)

                        # continue processing the remaining runs
                        continue

                    # if this is a new run
                    if run['run_data']['supervisor_job_status'].startswith('new'):
                        job_prov = 'New APS'
                        job_type = JobType.STAGING
                    # if we are in debug mode
                    elif run['run_data']['supervisor_job_status'].startswith('debug'):
                        job_prov = 'New debug'
                        job_type = JobType.STAGING
                    # ignore the entry as it is not in a legit "start" state. this may just
                    # be an existing or completed run.
                    else:
                        self.logger.info("Error - Unrecognized run command %s for %s", run['run_data']['supervisor_job_status'], run_id)
                        continue

                    # add the new run to the list
                    self.run_list.append({'id': run_id, 'forcing.stormname': run['run_data']['forcing.stormname'], 'debug': debug_mode,
                                          'fake-jobs': self.debug_options['fake_job'], 'job-type': job_type, 'status': JobStatus.NEW,
                                          'status_prov': f'{job_prov} run accepted', 'downloadurl': run['run_data']['downloadurl'],
                                          'gridname': run['run_data']['adcirc.gridname'], 'instance_name': run['run_data']['instancename']})

                    # update the run status in the DB
                    self.util_objs['pg_db'].update_job_status(run_id, f"{job_prov} run accepted")

                    # notify Slack
                    self.util_objs['utils'].send_slack_msg(run_id, f'{job_prov} run accepted.', 'slack_status_channel', debug_mode,
                                                           run['run_data']['instancename'])

    def check_pause_status(self, runs) -> dict:
        """
        checks to see if we are in pause mode. if the system isn't, get new runs.

        :param runs: the run parameters
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