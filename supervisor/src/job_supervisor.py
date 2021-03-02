import time
import uuid
import os
import logging
from json import load
from enum import Enum

from supervisor.src.job_create import JobCreate
from supervisor.src.job_find import JobFind
# from postgres.src.pg_utils import PGUtils
from common.logging import LoggingUtil


class JobStatus(int, Enum):
    """
    Class that stores the job status constants
    """
    # new job status
    new = 1

    # file staging statuses
    staging_running = 10
    staging_complete = 20

    # obs_mod supp statuses
    obs_mod_running = 30
    obs_mod_complete = 40

    # obs_mod geo tiff statuses
    run_geo_tiff_running = 50
    run_geo_tiff_complete = 60

    # geo tiff to mbtile statuses
    compute_mbtiles_running = 70
    compute_mbtiles_complete = 80

    # load geoserver statuses
    load_geo_server_running = 90
    load_geo_server_complete = 100

    # final staging operations statuses
    final_staging_running = 110
    final_staging_complete = 120

    # overall status indicators
    warning = 9999
    error = -1


class JobType(str, Enum):
    """
    Class that stores the job type constants
    """
    staging = 'staging',
    obs_mod = 'obs-mod',
    run_geo_tiff = 'run-geo-tiff',
    compute_mbtiles = 'compute-mbtiles',
    load_geo_server = 'load-geo-server',
    final_staging = 'final-staging',
    error = 'error'
    other_1 = 'TBD'
    complete = 'complete'


class APSVizSupervisor:
    """
    Class for the APSViz supervisor

    """

    # the list of pending runs. this stores all job details of the run
    run_list = []

    def __init__(self):
        """
        inits the class
        """
        # load the run configuration params
        self.k8s_config: dict = self.get_config()

        # create a job creator object
        self.k8s_create = JobCreate()

        # create a job status finder object
        self.k8s_find = JobFind()

        # create the postgres access object
        # self.pg_db = PGUtils()

        # get the log level and directory from the environment.
        # level comes from the container dockerfile, path comes from the k8s secrets
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the dir if it does not exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.APSVizSupervisor", level=log_level, line_format='medium', log_file_path=log_path)

    # TODO: make this a common function
    @staticmethod
    def get_config() -> dict:
        """
        gets the run configuration

        :return: Dict, baseline run params
        """

        # get the supervisor config file path/name
        config_name = os.path.join(os.path.dirname(__file__), '..', 'supervisor_config.json')

        # open the config file
        with open(config_name, 'r') as json_file:
            # load the config items into a dict
            data: dict = load(json_file)

        # return the config data
        return data

    def run(self):
        """
        endless loop finding task requests and create k8s jobs to complete them
        :return:
        """
        # get the incomplete runs from the database
        # TODO: put this in the correct place below in the while loop after testing
        self.get_incomplete_runs()

        # set counter that indicates nothing was done
        no_activity_counter: int = 0

        # until the end of time
        while True:
            # reset the activity flag
            no_activity: bool = True

            # for each run returned from the database
            for run in self.run_list:
                # init the state
                # job_status: str = 'Init'
                # job_pod_status: str = 'Init'

                config = self.k8s_config[run['job-type']]

                # skip this job if it is complete
                if run['job-type'] == JobType.complete:
                    continue
                # is this a staging job
                elif run['job-type'] == JobType.staging:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record?
                        command_line_params = ['--inputURL', 'http://tds.renci.org:8080/thredds/fileServer/2021/nam/2021010500/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/', '--outputDir']

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params, True)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # set the current status
                        run['status'] = JobStatus.staging_running

                        self.logger.info(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.staging_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.info(f"Job complete. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                            # set the next stage and stage status
                            run['job-type'] = JobType.obs_mod
                            run['status'] = JobStatus.new

                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set error conditions
                            run['job-type'] = JobType.error
                            run['status'] = JobStatus.error

                # is this a obs_mod job
                elif run['job-type'] == JobType.obs_mod:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # create the additional command line parameters
                        command_line_params = ['--outputDir', config['DATA_MOUNT_PATH'] + '/' + str(run['id']) + config['SUB_PATH'] + config['ADDITIONAL_PATH'], '--inputURL', config['DATA_MOUNT_PATH'] + '/' + str(run['id'])]

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # move to the next stage
                        run['status'] = JobStatus.obs_mod_running

                        self.logger.info(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.obs_mod_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.info(f"Job complete. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                            # set the next stage and stage status
                            run['job-type'] = JobType.load_geo_server
                            run['status'] = JobStatus.new

                        # TODO: how should this be handled?
                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set error conditions
                            run['job-type'] = JobType.error
                            run['status'] = JobStatus.error

                # is this a geo tiff job array
                elif run['job-type'] == JobType.run_geo_tiff:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # create the additional command line parameters
                        command_line_params = ['--outputDir', config['DATA_MOUNT_PATH'] + '/' + str(run['id']) + '/tiff', '--inputFile']

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # move to the next stage
                        run['status'] = JobStatus.run_geo_tiff_running

                        self.logger.info(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.run_geo_tiff_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.info(f"Job complete. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                            # set the next stage and stage status
                            run['job-type'] = JobType.compute_mbtiles
                            run['status'] = JobStatus.new

                        # TODO: how should this be handled?
                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set error conditions
                            run['status'] = JobStatus.error
                            run['job-type'] = JobType.error

                # is this a geo server load job
                elif run['job-type'] == JobType.load_geo_server:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record?
                        command_line_params = ['--instanceId', str(run['id'])]

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # set the current status
                        run['status'] = JobStatus.load_geo_server_running

                        self.logger.info(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.load_geo_server_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.info(f"Job complete. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                            # set the type to complete
                            run['job-type'] = JobType.final_staging
                            run['status'] = JobStatus.new

                        # TODO: how should this be handled?
                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set error conditions
                            run['job-type'] = JobType.error
                            run['status'] = JobStatus.error

                # is this a final staging job
                elif run['job-type'] == JobType.final_staging:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record?
                        command_line_params = ['--inputDir', '/data/' + str(run['id']), '--outputDir', '/data/final', '--tarMeta', str(run['id'])]

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params, False)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # set the current status
                        run['status'] = JobStatus.final_staging_running

                        self.logger.info(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.final_staging_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.info(f"Job complete. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                            # set the next stage and stage status
                            run['job-type'] = JobType.complete
                            run['status'] = JobStatus.final_staging_complete

                        # TODO: how should this be handled?
                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set error conditions
                            run['job-type'] = JobType.error
                            run['status'] = JobStatus.error

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1
            else:
                no_activity_counter = 0

            # check for something to do after a period of time
            if no_activity_counter >= 10:
                # set the sleep timeout
                sleep_timeout = config = self.k8s_config['POLL_LONG_SLEEP']

                # try again at this poll rate
                no_activity_counter = 9
            else:
                # set the sleep timeout
                sleep_timeout = self.k8s_config['POLL_SHORT_SLEEP']

            self.logger.debug(f"All active run checks complete. Sleeping for {sleep_timeout/60} minutes.")

            # wait longer for something to do
            time.sleep(sleep_timeout)

    def k8s_create_job_obj(self, run: dict, command_line_params: list, extend_output_path: bool = False):
        """
        Creates the details for an obs_mod-supp job from the database

        :return:
        """
        # TODO: most values to populate this object should come from the database or a configuration file
        if run['status'] == JobStatus.new:
            # get the config
            config = self.k8s_config[run['job-type']]

            # load the config with the info from the config file
            config['JOB_NAME'] += str(run['id'])
            config['DATA_VOLUME_NAME'] += str(run['id'])
            config['SSH_VOLUME_NAME'] += str(run['id'])
            config['COMMAND_LINE'].extend(command_line_params)

            if extend_output_path:
                config['SUB_PATH'] = '/' + str(run['id']) + config['SUB_PATH']
                config['COMMAND_LINE'].extend([config['DATA_MOUNT_PATH'] + config['SUB_PATH'] + config['ADDITIONAL_PATH']])

            # save these params onto the run info
            run[run['job-type']] = {'run-config': config}

    def get_incomplete_runs(self):
        """
        TODO: get the list of instances that need processing
        :return: list of records to process
        """
        # create the SQL. raw SQL calls using the django db model need an ID
        # sql = "SELECT public.get_config_list_json() AS data;"

        # create a new GUID
        uid: str = str(uuid.uuid4())

        # get the data
        # ret_val = self.pg_db.exec_sql(sql)

        # add this run to the list
        # self.run_list.append({'id': 2620, 'job-type': JobType.staging, 'status': JobStatus.new})
        # self.run_list.append({'id': 2620, 'job-type': JobType.obs_mod, 'status': JobStatus.new})
        self.run_list.append({'id': 2620, 'job-type': JobType.run_geo_tiff, 'status': JobStatus.new})
