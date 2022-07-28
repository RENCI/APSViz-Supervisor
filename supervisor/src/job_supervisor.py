# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

import time
import os
import logging
import slack
import json
from supervisor.src.job_create import JobCreate
from supervisor.src.job_find import JobFind
from postgres.src.pg_utils import PGUtils
from common.logger import LoggingUtil
from common.job_enums import JobType, JobStatus


class APSVizSupervisor:
    """
    Class for the APSViz supervisor. this tool runs processes as defined in the database.
    """

    def __init__(self):
        """
        inits the class
        """
        # the list of pending runs. this stores all job details of the run
        self.run_list = []

        # set DB the polling values
        self.POLL_SHORT_SLEEP = 30
        self.POLL_LONG_SLEEP = 120

        # load the run configuration params
        self.k8s_config: dict = {}

        # create a job creator object
        self.k8s_create = JobCreate()

        # create a job status finder object
        self.k8s_find = JobFind()

        # create the postgres access object
        self.pg_db = PGUtils()

        # init the run params to look for list
        self.required_run_params = ['supervisor_job_status', 'downloadurl', 'adcirc.gridname', 'instancename']

        # flag for pause mode
        self.pause_mode = True

        # counter for current runs
        self.run_count = 0

        # get the log level and directory from the environment.
        # level comes from the container dockerfile, path comes from the k8s secrets
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the log dir if it does not exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        # get the environment this instance is running on
        self.system = os.getenv('SYSTEM', 'System name not set')

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.APSVizSupervisor", level=log_level, line_format='medium', log_file_path=log_path)

        # instantiate slack connectivity
        self.slack_client = slack.WebClient(token=os.getenv('SLACK_ACCESS_TOKEN'))
        self.slack_channel = os.getenv('SLACK_CHANNEL')

        # declare ready
        self.logger.info(f'K8s Supervisor ({self.system}) has started...')

    def get_config(self) -> dict:
        """
        gets the run configuration

        :return: Dict, baseline run params
        """
        # get all the job parameter definitions
        db_data = self.pg_db.get_job_defs()

        # get the data looking like we are used to
        config_data = {list(x)[0]: x.get(list(x)[0]) for x in db_data}

        # fix the arrays for each job def.
        # they come in as a string
        for item in config_data.items():
            item[1]['COMMAND_LINE'] = json.loads(item[1]['COMMAND_LINE'])
            item[1]['COMMAND_MATRIX'] = json.loads(item[1]['COMMAND_MATRIX'])

        # return the config data
        return config_data

    def run(self):
        """
        endless loop finding process requests to create and run k8s jobs

        :return:
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
                    if run['job-type'] == JobType.complete:
                        run['status_prov'] += ', Run complete'
                        self.pg_db.update_job_status(run['id'], run['status_prov'])

                        status_prov = run['status_prov'].lower()

                        # get the type of run
                        run_type = ('APS' if status_prov.find('hazus-singleton') == -1 else 'HAZUS-SINGLETON')

                        # add a comment on overall pass/fail
                        if run['status_prov'].find('Error') == -1:
                            msg = f'*{run_type} run completed successfully*.'
                        else:
                            msg = f"*{run_type} run completed unsuccessfully*.\nRun provenance: {run['status_prov']}."

                        # send the message
                        self.send_slack_msg(run['id'], msg, run['debug'], run['instance_name'])

                        # send something to log to indicate complete
                        self.logger.info(f"{run['id']} complete.")

                        # remove the run
                        self.run_list.remove(run)

                        # continue processing
                        continue
                    # or an error
                    elif run['job-type'] == JobType.error:
                        # if this was a final staging run that failed force complete
                        if 'final-staging' in run:
                            self.logger.error(f"Error detected in final staging for run id: {run['id']}")
                            run['status_prov'] += ', Error detected in final staging. An incomplete cleanup may have occurred'

                            # set error conditions
                            run['job-type'] = JobType.complete
                            run['status'] = JobStatus.error
                        # else try to clean up
                        else:
                            self.logger.error(f"Error detected: About to clean up of intermediate files. Run id: {run['id']}")
                            run['status_prov'] += ', Error detected'

                            # set the type to clean up
                            run['job-type'] = JobType.final_staging
                            run['status'] = JobStatus.new

                        # report the issue
                        self.pg_db.update_job_status(run['id'], run['status_prov'])

                        # continue processing runs
                        continue
                except Exception as e_main:
                    # report the exception
                    self.logger.exception(f"Cleanup exception detected, id: {run['id']}, exception: {e_main}")

                    msg = f'Exception {e_main} caught. Terminating run.'

                    # send the message
                    self.send_slack_msg(run['id'], msg, run['debug'], run['instance_name'])

                    # remove the run
                    self.run_list.remove(run)

                    # continue processing runs
                    continue

                # catch handling the run exceptions
                try:
                    # handle the run
                    no_activity = self.handle_run(run)
                except Exception as e:
                    # report the exception
                    self.logger.exception(f"Run handler exception detected, id: {run['id']}, exception: {e}")

                    # prepare the DB status
                    run['status_prov'] += ', Run handler error detected'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])

                    # delete the k8s job if it exists
                    job_del_status = self.k8s_create.delete_job(run)

                    # if there was a job error
                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error(f"Error failed job. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, job delete status: {job_del_status}")

                    # set error conditions
                    run['job-type'] = JobType.error
                    run['status'] = JobStatus.error

                    # continue processing runs
                    continue

            # output the current number of runs in progress if there are any
            if self.run_count != len(self.run_list):
                # save the new run count
                self.run_count = len(self.run_list)
                self.logger.info(f'There {"are" if self.run_count != 1 else "is"} {self.run_count} run{"s" if self.run_count != 1 else ""} in progress.')

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1
            else:
                no_activity_counter = 0

            # check for something to do after a period of time
            if no_activity_counter >= 10:
                # set the sleep timeout
                sleep_timeout = self.POLL_LONG_SLEEP

                # try again at this poll rate
                no_activity_counter = 9
            else:
                # set the sleep timeout
                sleep_timeout = self.POLL_SHORT_SLEEP

            self.logger.debug(f"All active run checks complete. Sleeping for {sleep_timeout / 60} minutes.")

            # wait for the next check for something to do
            time.sleep(sleep_timeout)

    def get_base_command_line(self, run) -> (list, bool):
        """
        gets the command lines for each run type
        note: use this to keep a pod running after command_line and command_matrix for the job have been set to '[""]' in the DB
            also note that the supervisor should be terminated prior to killing the job to avoid data directory removal (if that matters)
            command_line_params = ['/bin/sh', '-c', 'while true; do date; sleep 3600; done']
            update public."ASGS_Mon_supervisor_config" set command_line='[""]', command_matrix='[""]' where id=;

        :param run:
        :return:
        """
        # init the returns
        command_line_params = None
        extend_output_path = False

        # is this a staging job array
        if run['job-type'] == JobType.staging:
            command_line_params = ['--inputURL', run['downloadurl'], '--outputDir']
            extend_output_path = True

        # is this a hazus job array
        elif run['job-type'] == JobType.hazus:
            command_line_params = [run['downloadurl']]

        # is this a hazus-singleton job array
        elif run['job-type'] == JobType.hazus_singleton:
            command_line_params = [run['downloadurl']]

        # is this an obs_mod job array
        elif run['job-type'] == JobType.obs_mod:
            thredds_url = run['downloadurl'] + '/fort.63.nc'
            thredds_url = thredds_url.replace('fileServer', 'dodsC')

            # create the additional command line parameters
            command_line_params = ['--instanceId', str(run['id']),
                                   '--inputURL', thredds_url, '--grid', run['gridname'],
                                   '--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'] + self.k8s_config[run['job-type']]['ADDITIONAL_PATH'],
                                   '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[run['job-type']]['ADDITIONAL_PATH']]

        # is this a geo tiff job array
        elif run['job-type'] == JobType.run_geo_tiff:
            command_line_params = ['--inputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input',
                                   '--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--inputFile']

        # is this a mbtiles zoom 0-10 job array
        elif run['job-type'] == JobType.compute_mbtiles_0_10:
            command_line_params = ['--inputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/tiff',
                                   '--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--inputFile']

        # is this an adcirc2cog_tiff job array
        elif run['job-type'] == JobType.adcirc2cog_tiff:
            command_line_params = ['--inputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input',
                                   '--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--inputFile']

        # is this a geotiff2cog job array
        elif run['job-type'] == JobType.geotiff2cog:
            command_line_params = ['--inputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/cogeo',
                                   '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--inputParam']

        # is this a geo server load job array
        elif run['job-type'] == JobType.load_geo_server:
            command_line_params = ['--instanceId', str(run['id'])]

        # is this a final staging job array
        elif run['job-type'] == JobType.final_staging:
            command_line_params = ['--inputDir', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--outputDir', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--tarMeta', str(run['id'])]

        # is this an obs mod ast job array
        # ./ execute_APSVIZ_pipeline.sh
        elif run['job-type'] == JobType.obs_mod_ast:
            thredds_url = run['downloadurl'] + '/fort.63.nc'
            thredds_url = thredds_url.replace('fileServer', 'dodsC')

            # create the additional command line parameters
            command_line_params = [thredds_url,
                                   run['gridname'],
                                   self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[run['job-type']]['ADDITIONAL_PATH'],
                                   str(run['id'])]

        # is this an adcirc time to cog converter job array
        elif run['job-type'] == JobType.adcirctime_to_cog:
            command_line_params = ['--inputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/input',
                                   '--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/' + 'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                   '--inputFile']

        return command_line_params, extend_output_path

    def handle_run(self, run) -> bool:
        """
        handles the run processing

        :param run:
        :return:
        """
        # init the activity flag
        no_activity: bool = True

        # is this a staging job
        # if run['job-type'] == JobType.staging:
        # work the current state
        if run['status'] == JobStatus.new:
            # set the activity flag
            no_activity = False

            # get the data by the download url
            command_line_params, extend_output_path = self.get_base_command_line(run)

            # create the job configuration for a new run
            self.k8s_create_job_obj(run, command_line_params, extend_output_path)

            # execute the k8s job run
            self.k8s_create.execute(run)

            # set the current status
            run['status'] = JobStatus.running
            run['status_prov'] += f", {run['job-type'].value} running"
            self.pg_db.update_job_status(run['id'], run['status_prov'])

            self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
        elif run['status'] == JobStatus.running and run['status'] != JobStatus.error:
            # set the activity flag
            no_activity = False

            # find the job, get the status
            job_found, job_status, pod_status = self.k8s_find.find_job_info(run)

            # if the job status is empty report it and continue
            if not job_found:
                self.logger.error(f"Job not found. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif job_status.startswith('Timeout'):
                self.logger.error(f"Job has timed out. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif job_status.startswith('Failed'):
                self.logger.error(f"Job has failed. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif job_status.startswith('Complete'):
                self.logger.debug(f"Job has completed. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")

            # if the job was found
            if job_found:
                # did the job timeout (presumably waiting for resources) or failed
                if job_status.startswith('Timeout') or job_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.k8s_create.delete_job(run)

                    # set error conditions
                    run['status'] = JobStatus.error
                # did the job and pod succeed
                elif job_status.startswith('Complete') and not pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.k8s_create.delete_job(run)

                    # was there an error on the job
                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error(f"Error failed job: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job delete status: {job_del_status}, pod status: {pod_status}.")

                        # set error conditions
                        run['status'] = JobStatus.error
                    else:
                        # set the stage status
                        run['status_prov'] += f", {run['job-type'].value} complete"
                        self.pg_db.update_job_status(run['id'], run['status_prov'])

                        # prepare for next stage
                        run['job-type'] = JobType(run[run['job-type'].value]['run-config']['NEXT_JOB_TYPE'])
                        run['status'] = JobStatus.new
                # was there a failure. remove the job and declare failure
                elif pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_del_status = self.k8s_create.delete_job(run)

                    if job_del_status == '{}' or job_del_status.find('Failed') != -1:
                        self.logger.error(f"Error failed job and/or pod: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job delete status: {job_del_status}, pod status: {pod_status}.")

                    # set error conditions
                    run['status'] = JobStatus.error
            else:
                self.logger.error(f"Error job not found: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}")

                # set error conditions
                run['status'] = JobStatus.error

        # send out the error status on error
        if run['status'] == JobStatus.error:
            self.send_slack_msg(run['id'], f"failed in {run['job-type']}.", run['debug'], run['instance_name'])
            run['job-type'] = JobType.error

        # return to the caller
        return no_activity

    def k8s_create_job_obj(self, run: dict, command_line_params: list, extend_output_path: bool = False):
        """
        Creates the details for a job from the database

        :return:
        """
        # create a new configuration if this is a new run
        if run['status'] == JobStatus.new:
            # get the config
            config = self.get_config()[run['job-type']]

            # load the config with the info from the config file
            config['JOB_NAME'] += str(run['id']).lower()
            config['DATA_VOLUME_NAME'] += str(run['id']).lower()
            config['SSH_VOLUME_NAME'] += str(run['id']).lower()
            config['COMMAND_LINE'].extend(command_line_params)

            # tack on any additional paths if requested
            if extend_output_path:
                config['SUB_PATH'] = '/' + str(run['id']) + config['SUB_PATH']
                config['COMMAND_LINE'].extend([config['DATA_MOUNT_PATH'] + config['SUB_PATH'] + config['ADDITIONAL_PATH']])

            self.logger.debug(f"Job command line. Run ID: {run['id']}, Job type: {run['job-type']}, Command line: {config['COMMAND_LINE']}")

            # save these params in the run info
            run[run['job-type']] = {'run-config': config}

    def send_slack_msg(self, run_id, msg, debug_mode=False, instance_name=None):
        """
        sends a msg to the Slack channel

        :param run_id:
        :param msg:
        :param debug_mode:
        :param instance_name:
        :return:
        """
        # init the final msg
        final_msg = f"APSViz Supervisor ({self.system}) - "

        # if there was an instance name use it
        final_msg += '' if instance_name is None else 'Instance name: {instance_name}, '

        # add the run id and msg
        final_msg += msg if run_id is None else f'Run ID: {run_id} {msg}'

        # log the message if in debug mode
        if debug_mode:
            self.logger.info(final_msg)
        # else send the message to slack
        else:
            self.slack_client.chat_postMessage(channel=self.slack_channel, text=final_msg)

    def check_input_params(self, run_info: dict) -> (str, str, bool):
        """
        Checks the run data to insure we have all the necessary info to start a run

        :param run_info:
        :return:
        """
        # if there was an instance name use it
        if 'instancename' in run_info:
            instance_name = run_info['instancename']
        else:
            instance_name = None

        # should we set debug mode
        if 'supervisor_job_status' in run_info and run_info['supervisor_job_status'].startswith('debug'):
            debug_mode = True
        else:
            debug_mode = False

        # if there is a special k8s download url in the data use it.
        if 'post.opendap.renci_tds-k8.downloadurl' in run_info:
            # use only the service name and save it for the run. also forcing the apsviz thredds url to be https:
            run_info['downloadurl'] = run_info['post.opendap.renci_tds-k8.downloadurl'].replace('http://apsviz-thredds', 'https://apsviz-thredds')

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
        self.k8s_config = self.get_config()

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

                # if there is a message something is missing
                if len(missing_params_msg) > 0:
                    # update the run status everywhere
                    self.pg_db.update_job_status(run_id, f'Error - Run lacks the required run properties ({missing_params_msg}).')
                    self.logger.error(f"Error - Run lacks the required run properties ({missing_params_msg}): {run_id}")
                    self.send_slack_msg(run_id, f'Error - Run lacks the required run properties ({missing_params_msg})', debug_mode, instance_name)

                    # continue processing the remaining runs
                    continue
                # get the run params.
                elif run['run_data']['supervisor_job_status'].startswith('debug'):
                    job_prov = 'New debug'
                    job_type = JobType.staging
                elif run['run_data']['supervisor_job_status'].startswith('hazus'):
                    job_prov = 'New HAZUS-SINGLETON'
                    job_type = JobType.hazus_singleton
                elif run['run_data']['supervisor_job_status'].startswith('new'):
                    job_prov = 'New APS'
                    job_type = JobType.staging
                else:
                    # this is not a new run. ignore the entry as it is not in a legit "start" state.
                    continue

                # add the new run to the list
                self.run_list.append({'id': run_id, 'debug': debug_mode, 'job-type': job_type, 'status': JobStatus.new, 'status_prov': f'{job_prov} run accepted', 'downloadurl': run['run_data']['downloadurl'], 'gridname': run['run_data']['adcirc.gridname'], 'instance_name': run['run_data']['instancename']})

                # update the run status in the DB
                self.pg_db.update_job_status(run_id, f"{job_prov} run accepted")

                # notify Slack
                self.send_slack_msg(run_id, f'{job_prov} run accepted.', debug_mode, run['run_data']['instancename'])

    def check_pause_status(self, runs) -> dict:
        """
        checks to see if we are in pause mode.

        :param runs:
        :return:
        """
        # get the flag that indicates we are pausing the handling of new run requests
        pause_mode = os.path.exists(os.path.join(os.path.dirname(__file__), '../', '../', str('pause')))

        # are we toggling pause mode
        if pause_mode != self.pause_mode:
            # save the new pause mode
            self.pause_mode = pause_mode

            # let everyone know
            self.send_slack_msg(None, f'K8s Supervisor application ({self.system}) is now {"paused" if pause_mode else "active"}.', debug_mode=True)

        # if we are not in pause mode get all the new rune
        if not pause_mode:
            # get the new runs
            runs = self.pg_db.get_new_runs()

            # were there any new runs
            if runs == -1:
                self.logger.debug(f'No new runs found.')
                runs = None

        # return to the caller
        return runs
