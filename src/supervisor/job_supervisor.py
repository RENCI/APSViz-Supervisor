# BSD 3-Clause All rights reserved.
#
# SPDX-License-Identifier: BSD 3-Clause

"""
    This module creates and workflows iRODS-K8s processes as defined in the database.

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
from src.common.job_enums import JobType, JobStatus, DBType
from src.common.utils import Utils


class JobSupervisor:
    """
    Class for the iRODS K8s supervisor

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
        self.logger = LoggingUtil.init_logging("iRODS.Supervisor.Jobs", level=log_level, line_format='medium', log_file_path=log_path)

        # init the list of pending runs. this stores all job details of the run
        self.run_list: list = []

        # load the base configuration params
        self.sv_config: dict = Utils.get_base_config()

        # init the running count of active runs
        self.run_count: int = 0

        # init the k8s job configuration storage
        self.k8s_job_configs: dict = {}

        # specify the DB to get a connection
        # note the extra comma makes this single item a singleton tuple
        db_names: tuple = ('irods-sv',)

        # assign utility objects
        self.util_objs: dict = {'create': JobCreate(), 'k8s_find': JobFind(), 'pg_db': PGImplementation(db_names, _logger=self.logger),
                                'utils': Utils(self.logger, self.system, self.app_version)}

        # init the run params to look for a list
        self.required_run_params = ['workflow-type']

        # debug options
        self.debug_options: dict = {'pause_mode': True, 'fake_job': False}

        # init the last time a run completed
        self.last_run_time = dt.datetime.now()

        # declare ready
        self.logger.info('The iRODS K8s Job Supervisor:%s (%s) has initialized...', self.app_version, self.system)

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
                    item[1]['PORT_RANGE'] = json.loads(item[1]['PORT_RANGE']) if item[1]['PORT_RANGE'] is not None else None
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
                        self.handle_run_complete(run)

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

            # was there any activity'--run_dir', job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['id'])
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
            if no_activity_counter >= self.sv_config.get("MAX_NO_ACTIVITY_COUNT"):
                # set the sleep timeout
                sleep_timeout = self.sv_config.get("POLL_LONG_SLEEP")

                # try again at this poll rate
                no_activity_counter = self.sv_config.get("MAX_NO_ACTIVITY_COUNT") - 1
            else:
                # set the sleep timeout
                sleep_timeout = self.sv_config.get("POLL_SHORT_SLEEP")

            self.logger.debug("All active run checks complete. Sleeping for %s seconds.", sleep_timeout)

            # wait for the next check for something to do
            time.sleep(sleep_timeout)

    def handle_job_error(self, run: dict):
        """
        handles the job state when it is marked as in error

        :param run:
        :return:
        """
        # does this run have a final staging step?
        if 'final-staging' not in self.k8s_job_configs[run['workflow_type']]:
            self.logger.error("Error detected for a run of type %s. Run id: %s", run['workflow_type'], run['id'])
            run['status_prov'] += f", error detected in a run of type {run['workflow_type']}. No cleanup occurred."

            # set error conditions
            run['job-type'] = JobType.COMPLETE
            run['status'] = JobStatus.ERROR
        # if this was a final staging run that failed force complete
        elif 'final-staging' in run:
            self.logger.error("Error detected for a run in final staging with run id: %s", run['id'])
            run['status_prov'] += ", error detected for a run in final staging. An incomplete cleanup may have occurred."

            # set error conditions
            run['job-type'] = JobType.COMPLETE
            run['status'] = JobStatus.ERROR
        # else try to clean up
        else:
            self.logger.error("Error detected for a run. About to clean up of intermediate files. Run id: %s", run['id'])
            run['status_prov'] += ', error detected'

            # set the type to clean up
            run['job-type'] = JobType.FINAL_STAGING
            run['status'] = JobStatus.NEW

        # report the issue
        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

    def handle_run_complete(self, run: dict):
        """
        handles the completion of a run

        :param run:
        :return:
        """
        # clean up any jobs/services that may be lingering
        status_prov: str = self.util_objs['create'].clean_up_jobs_and_svcs(run)

        # add the status to the provenance if there were any services removed
        if status_prov != '':
            # save anything that was cleaned up
            run['status_prov'] += f', {status_prov}'

        # get the run duration
        duration = Utils.get_run_time_delta(run)

        # update the run provenance in the DB
        run['status_prov'] += f', run complete {duration}'
        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

        # send something to the log to indicate complete
        self.logger.info("Run ID: %s is complete.", run['id'])

        # remove the run
        self.run_list.remove(run)

    def get_base_command_line(self, run: dict, job_type: JobType) -> (list, bool):
        """
        gets the command lines for each workflow step type

        :param run: the run parameters
        :param job_type:
        :return: a list of the command line parameters
        """
        # init the returns
        command_line_params = None
        extend_output_path = False

        # get the proper job configs
        job_configs = self.k8s_job_configs[run['workflow_type']]

        # is this a staging job?
        if job_type == JobType.STAGING:
            command_line_params = ['--run_id', str(run['id']), '--run_dir',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['request_group']), '--step_type', 'initial',
                                   '--workflow_type', run['workflow_type']]

        # is this a database job?
        elif job_type == JobType.DATABASE:
            command_line_params = ''

        # is this a consumer job?
        elif job_type == JobType.CONSUMER:
            command_line_params = ''

        # is this a consumer job?
        elif job_type == JobType.CONSUMERSECONDARY:
            command_line_params = ''

        # is this a consumer job?
        elif job_type == JobType.CONSUMERTERTIARY:
            command_line_params = ''

        # is this a provider job?
        elif job_type == JobType.PROVIDER:
            command_line_params = ''

        # is this a provider job?
        elif job_type == JobType.PROVIDERSECONDARY:
            command_line_params = ''

        # is this a forensics job?
        elif job_type == JobType.FORENSICS:
            command_line_params = ['--run_id', str(run['id']), '--run_dir',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['request_group'])]

        # is this a final staging job?
        elif job_type == JobType.FINAL_STAGING:
            command_line_params = ['--run_id', str(run['id']), '--run_dir',
                                   job_configs[job_type]['DATA_MOUNT_PATH'] + '/' + str(run['request_group']), '--step_type', 'final']

        # is this a tester job?
        elif job_type == JobType.TESTER:
            command_line_params = ''

        # unknown job type
        else:
            self.logger.error('Error: Unrecognized job type %s.', job_type)

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

            # create a list of parallel jobs to create
            job_type_list: list = [run['job-type']]

            # append any parallel jobs if they exist
            if 'PARALLEL' in job_configs[run['job-type']] and job_configs[run['job-type']]['PARALLEL']:
                job_type_list.extend(job_configs[run['job-type']]['PARALLEL'])

            # create jobs from items in the list
            for job_type in job_type_list:
                # get the data by the download url
                command_line_params, extend_output_path = self.get_base_command_line(run, job_type)

                # create a new run configuration for the step
                self.k8s_create_run_config(run, job_type, command_line_params, extend_output_path)

                # execute the k8s job run
                job_id = self.util_objs['create'].execute(run, job_type)

                # did we not get a job_id?
                if job_id is not None:
                    # set the current status
                    run['status'] = JobStatus.RUNNING
                    run['status_prov'] += f", {job_type.value} running"
                    self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

                    self.logger.info("A job was created. Run ID: %s, Job type: %s", run['id'], job_type)
                else:
                    # set the error status
                    run['status'] = JobStatus.ERROR

                    self.logger.info("A job was not created. Run ID: %s, Job type: %s", run['id'], job_type)

                # if the next job is complete, there is no reason to keep adding more jobs
                if job_configs[job_type.value]['NEXT_JOB_TYPE'] == JobType.COMPLETE.value:
                    break

        # if the job is running, check the status
        if run['status'] == JobStatus.RUNNING and run['status'] != JobStatus.ERROR:
            # set the activity flag
            no_activity = False

            # find the job, get the status
            job_found, job_status, pod_status = self.util_objs['k8s_find'].find_job_info(run)

            # check the job status, report any issues
            if not job_found:
                self.logger.error("Error: A job not found. Run ID: %s, Job type: %s", run['id'], run['job-type'])
            elif job_status.startswith('Timeout'):
                self.logger.error("Error: A job has timed out. Run ID: %s, Job type: %s", run['id'], run['job-type'])
            elif job_status.startswith('Failed'):
                self.logger.error("Error: A job has failed. Run ID: %s, Job type: %s", run['id'], run['job-type'])
                run['status_prov'] += f", {run['job-type'].value} failed"
            elif job_status.startswith('Complete'):
                self.logger.info("A job has completed. Run ID: %s, Job type: %s", run['id'], run['job-type'])

            # if the job was found
            if job_found:
                # if this is a server process job set it to complete, so it moves to the next step
                if self.util_objs['create'].is_server_process(run[run['job-type']]['run-config']):
                    job_status = 'Complete'

                # did the job timeout (presumably waiting for resources) or failed
                if job_status.startswith('Timeout') or job_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.util_objs['create'].delete_job(run)

                    # set error conditions
                    run['status'] = JobStatus.ERROR
                # did the job and the pod succeed?
                elif job_status.startswith('Complete') and not pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.util_objs['create'].delete_job(run)

                    # was there an error on the job?
                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error("Error: A failed job detected. Run status %s. Run ID: %s, Job type: %s, job delete status: %s, "
                                          "pod status: %s", run['status'], run['id'], run['job-type'], job_del_status, pod_status)

                        # set error conditions
                        run['status'] = JobStatus.ERROR
                    else:
                        if self.util_objs['create'].is_server_process(run[run['job-type']]['run-config']):
                            status_msg = 'configuring'
                        else:
                            status_msg = 'complete'

                        # complete this job and setup for the next job
                        run['status_prov'] += f", {run['job-type'].value} {status_msg}"
                        self.util_objs['pg_db'].update_job_status(run['id'], run['status_prov'])

                        # prepare for the next stage
                        run['job-type'] = JobType(run[run['job-type'].value]['run-config']['NEXT_JOB_TYPE'])

                        # if the job type is not in the run then declare it new
                        if run['job-type'] not in run:
                            # set the job to new
                            run['status'] = JobStatus.NEW

                # was there a failure? remove the job and declare failure
                elif pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.util_objs['create'].delete_job(run)

                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error("Error: A failed job and/or pod detected. Run status: %s. Run ID: %s, Job type: %s, job delete status: "
                                          "%s, pod status: %s.", run['status'], run['id'], run['job-type'], job_del_status, pod_status)

                    # set error conditions
                    run['status'] = JobStatus.ERROR
            else:
                self.logger.error("Error: A job not found: Run ID: %s, Run status: %s, Job type: %s", run['id'], run['status'], run['job-type'])

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

        # get the run id into a string for all
        run_id = str(run['id'])

        # load the config with the info from the config file
        config['JOB_NAME'] += run_id
        config['DATA_VOLUME_NAME'] += run_id
        config['COMMAND_LINE'].extend(command_line_params)

        # tack on any additional paths if requested
        if extend_output_path:
            config['SUB_PATH'] = f"'/'{run_id}{config['SUB_PATH']}"
            config['COMMAND_LINE'].extend(f"{config['DATA_MOUNT_PATH']}{config['SUB_PATH']}{config['ADDITIONAL_PATH']}")

        self.logger.debug("Job command line. Run ID: %s, Job type: %s, Command line: %s", run_id, job_type, config['COMMAND_LINE'])

        # save these params in the run info
        run[job_type] = {'run-config': config}

        # add in the default parameters to be filled in later
        run[job_type].update({'job-config': {'job': None, 'sv-details': None, 'job_id': None, 'service': None, 'svc_id': None}})

    def check_input_params(self, run_info: dict) -> (str, str, bool):
        """
        Checks the run data to insure we have all the necessary info to start a run

        :param run_info:
        :return: list of required items that weren't found
        """
        # get debug mode if available
        debug_mode = ('supervisor_job_status' in run_info and run_info['supervisor_job_status'].startswith('debug'))

        # get the workflow type
        if 'workflow-type' in run_info['request_data']:
            workflow_type = run_info['request_data']['workflow-type']
        # if there is no workflow type specified, then default to empty
        else:
            workflow_type = ''

        # get the DB image and type
        if 'db-image' in run_info['request_data'] and 'db-type' in run_info['request_data']:
            # is it a valid DB image and type?
            if run_info['request_data']['db-image'] != '' and str(run_info['request_data']['db-type']).upper() in DBType.__members__:
                db_image = run_info['request_data']['db-image']
                db_type = run_info['request_data']['db-type']
            # else use the default. this should never happen if the UI was used for the request
            else:
                db_image = 'postgres:14.11'
                db_type = DBType.POSTGRESQL
        # else use the default. this should never happen if the UI was used for the request
        else:
            db_image = 'postgres:14.11'
            db_type = DBType.POSTGRESQL

        # get the name:version for the OS image
        if 'os-image' in run_info['request_data']:
            os_image = run_info['request_data']['os-image']
        # if there is no os image specified, then type default to empty
        else:
            os_image = ''

        # get the name:version for the test image
        if 'test-image' in run_info['request_data']:
            test_image = run_info['request_data']['test-image']
        # if there is no test image specified, then default to empty
        else:
            test_image = ''

        # get the name:version for the test image
        if 'request_group' in run_info:
            request_group = run_info['request_group']
        # if there is no test image specified, then default to empty
        else:
            request_group = ''

        # get the package directory for the test. if none is specified, the startup scripts will choose to install "irods-server"
        if 'package-dir' in run_info['request_data']:
            package_dir = run_info['request_data']['package-dir']
        # if there is no test image specified, then default to empty
        else:
            package_dir = ''

        # loop through the params and return the ones that are missing
        return (f"{', '.join([run_param for run_param in self.required_run_params if run_param not in run_info['request_data']])}", debug_mode,
                workflow_type, db_image, db_type, os_image, test_image, request_group, package_dir)

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
            # was it found?
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
        if self.k8s_job_configs is not None and len(self.k8s_job_configs) > 0:
            # check to see if we are in pause mode
            runs = self.check_pause_status()

            # did we find anything to do?
            if runs is not None:
                # add the runs to the list
                for run in runs:
                    # save the run id that was provided by the DB run.properties data
                    run_id = run['run_id']

                    # check for a duplicate run
                    if not self.check_for_duplicate_run(run_id):
                        # make sure all the needed params are available. instance name and debug mode
                        # are handled here because they both affect messaging and logging.
                        missing_params_msg, debug_mode, workflow_type, db_image, db_type, os_image, test_image, request_group, package_dir = (
                            self.check_input_params(run['run_data']))

                        # check the run params to see if there is something missing
                        if len(missing_params_msg) > 0:
                            # update the run status everywhere
                            self.util_objs['pg_db'].update_job_status(run_id, f"Error - Run lacks the required run properties "
                                                                              f"({missing_params_msg}).")

                            # continue processing the remaining runs
                            continue

                        # if this is a new run
                        if run['run_data']['supervisor_job_status'].startswith('new'):
                            job_prov = f'New run accepted for {request_group}'
                        # if we are in debug mode
                        elif run['run_data']['supervisor_job_status'].startswith('debug'):
                            job_prov = 'New debug run accepted for {request_group}'
                        # ignore the entry as it is not in a legit "start" state. this may just
                        # be an existing or completed run.
                        else:
                            self.logger.info("Error: Unrecognized run command")
                            continue

                        # get the first job for this workflow type
                        first_job = self.util_objs['pg_db'].get_first_job(workflow_type)

                        # did we get a job type?
                        if first_job is not None:
                            # get the first job name into a type
                            job_type = JobType(first_job)
                        else:
                            self.logger.info("Error: Could not find the first job in the %s workflow for run id: %s", workflow_type, run_id)
                            continue

                        # scan the run for job characteristics
                        workflow_jobs: dict = self.get_workflow_jobs(workflow_type)

                        # add the new run to the list
                        self.run_list.append(
                            {'id': run_id, 'debug': debug_mode, 'workflow_type': workflow_type, 'db_image': db_image, 'db_type': db_type,
                             'os_image': os_image, 'test_image': test_image, 'fake-jobs': self.debug_options['fake_job'], 'job-type': job_type,
                             'status': JobStatus.NEW, 'status_prov': f'{job_prov}', 'run-start': dt.datetime.now(), 'request_group': request_group,
                             'package_dir': package_dir, 'workflow_jobs': workflow_jobs})

                        # update the run status in the DB
                        self.util_objs['pg_db'].update_job_status(run_id, f'{job_prov}')
                    else:
                        # update the run status in the DB
                        self.util_objs['pg_db'].update_job_status(run_id, 'Duplicate run rejected.')

    def get_workflow_jobs(self, workflow_type: str) -> dict:
        """
        gets all the jobs used in a workflow run

        :return:
        """
        # init the return
        ret_val: dict = {}

        # get the workflow characteristics
        workflow_config: dict = self.k8s_job_configs[workflow_type]

        # loop through the job types and assign their inclusion in the workflow
        for item in JobType:
            # is the job type in the workflow
            if item in workflow_config:
                # mark it as included in the workflow
                ret_val[item] = True
            # else
            else:
                # mark it as not in the workflow
                ret_val[item] = False

        # return to the caller
        return ret_val

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

            # build the message
            msg: str = f'The application is now {"paused" if pause_mode else "active"}.'

            # log pause mode was toggled
            self.logger.info(msg)

        # get all the new runs if the system is not in pause mode
        if not pause_mode:
            # get the new runs
            runs = self.util_objs['pg_db'].get_new_runs()

        # return to the caller
        return runs
