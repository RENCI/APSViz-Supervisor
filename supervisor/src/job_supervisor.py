import time
import os
import logging
from json import load
from enum import Enum
from supervisor.src.job_create import JobCreate
from supervisor.src.job_find import JobFind
from postgres.src.pg_utils import PGUtils
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

    # geo tiff statuses
    run_geo_tiff_running = 50
    run_geo_tiff_complete = 60

    # mbtile statuses, zoom level 0 to 9
    compute_mbtiles_0_9_running = 70
    compute_mbtiles_0_9_complete = 80

    # mbtile statuses, zoom level 10
    compute_mbtiles_10_running = 80
    compute_mbtiles_10_complete = 90

    # mbtile statuses, zoom level 11
    compute_mbtiles_11_running = 100
    compute_mbtiles_11_complete = 110

    # mbtile statuses, zoom level 12
    compute_mbtiles_12_running = 120
    compute_mbtiles_12_complete = 130

    # load geoserver statuses
    load_geo_server_running = 140
    load_geo_server_complete = 150

    # final staging operations statuses
    final_staging_running = 160
    final_staging_complete = 170

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
    compute_mbtiles_0_9 = 'compute-mbtiles-0-9',
    compute_mbtiles_10 = 'compute-mbtiles-10',
    compute_mbtiles_11 = 'compute-mbtiles-11',
    compute_mbtiles_12 = 'compute-mbtiles-12',
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
        self.pg_db = PGUtils()

        # get the log level and directory from the environment.
        # level comes from the container dockerfile, path comes from the k8s secrets
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))

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
        # set counter that indicates nothing was done
        no_activity_counter: int = 0

        # until the end of time
        while True:
            # get the incomplete runs from the database
            self.get_incomplete_runs()

            # reset the activity flag
            no_activity: bool = True

            # for each run returned from the database
            for run in self.run_list:
                # skip this job if it is complete
                if run['job-type'] == JobType.complete:
                    run['status_prov'] += ', Run complete.'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                    self.run_list.remove(run)
                    continue
                # or an error
                elif run['job-type'] == JobType.error:
                    run['status_prov'] += ', Error detected'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                    run['job-type'] = JobType.complete
                    continue

                # handle the run
                no_activity = self.handle_run(no_activity, run)

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1
            else:
                no_activity_counter = 0

            # check for something to do after a period of time
            if no_activity_counter >= 10:
                # set the sleep timeout
                sleep_timeout = self.k8s_config['POLL_LONG_SLEEP']

                # try again at this poll rate
                no_activity_counter = 9
            else:
                # set the sleep timeout
                sleep_timeout = self.k8s_config['POLL_SHORT_SLEEP']

            self.logger.debug(f"All active run checks complete. Sleeping for {sleep_timeout / 60} minutes.")

            # wait longer for something to do
            time.sleep(sleep_timeout)

    def handle_run(self, no_activity, run):
        """
        handles the run processing

        :param no_activity:
        :param run:
        :return:
        """
        # is this a staging job
        if run['job-type'] == JobType.staging:
            # work the current state
            if run['status'] == JobStatus.new:
                # set the activity flag
                no_activity = False

                # get the data by the download url
                command_line_params = ['--inputURL', run['downloadurl'], '--outputDir']  # '--instanceId', str(run['id']),

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params, True)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # set the current status
                run['status'] = JobStatus.staging_running
                run['status_prov'] += ', Staging running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.staging_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.obs_mod
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Staging complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
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

                # the download URL given is for a file server. obs/mod needs the dodsC access type
                access_type = run['downloadurl'] + '/fort.63.nc'
                access_type = access_type.replace('fileServer', 'dodsC')

                # create the additional command line parameters
                command_line_params = ['--instanceId', str(run['id']),
                                       '--inputURL', access_type, '--grid', run['gridname'],
                                       '--outputDIR',
                                       self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) +
                                       self.k8s_config[run['job-type']]['SUB_PATH'] +
                                       self.k8s_config[run['job-type']]['ADDITIONAL_PATH'],
                                       '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + '/' +
                                       'final' + self.k8s_config[run['job-type']]['ADDITIONAL_PATH']
                                       ]

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # move to the next stage
                run['status'] = JobStatus.obs_mod_running
                run['status_prov'] += ', Obs/Mod running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.obs_mod_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (No) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.run_geo_tiff
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Obs/Mod complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
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
                command_line_params = ['--outputDIR',
                                       self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) +
                                       self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--inputFile']

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # move to the next stage
                run['status'] = JobStatus.run_geo_tiff_running
                run['status_prov'] += ', Geo tiff running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.run_geo_tiff_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.compute_mbtiles_0_9
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Geo tiff complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
                elif job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                    # set error conditions
                    run['status'] = JobStatus.error
                    run['job-type'] = JobType.error

        # is this a mbtiles zoom 0-9 job array
        elif run['job-type'] == JobType.compute_mbtiles_0_9:
            """
            ["maxele.63.tif", "--zlstart", "0", "--zlstop", "9", "--cpu", "4"],
            """

            # work the current state
            if run['status'] == JobStatus.new:
                # set the activity flag
                no_activity = False

                # create the additional command line parameters
                command_line_params = ['--outputDIR',
                                       self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--finalDIR',
                                       self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + '/' +
                                       'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--inputFile'
                                       ]

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # move to the next stage
                run['status'] = JobStatus.compute_mbtiles_0_9_running
                run['status_prov'] += ', Compute mbtiles 0-9 running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.compute_mbtiles_0_9_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.load_geo_server
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Compute mbtiles zoom 0-9 complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
                elif job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                    # set error conditions
                    run['status'] = JobStatus.error
                    run['job-type'] = JobType.error

        # is this a mbtiles part 1 job array
        elif run['job-type'] == JobType.compute_mbtiles_10:
            """
            ["swan_HS_max.63.tif", "--zlstart", "10", "--zlstop", "10", "--cpu", "2"],
            """

            # work the current state
            if run['status'] == JobStatus.new:
                # set the activity flag
                no_activity = False

                # create the additional command line parameters
                command_line_params = ['--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + '/' +
                                       'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--inputFile'
                                       ]

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # move to the next stage
                run['status'] = JobStatus.compute_mbtiles_10_running
                run['status_prov'] += ',Compute mbtiles running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.compute_mbtiles_10_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.load_geo_server
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Compute mbtiles zoom 10 complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
                elif job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                    # set error conditions
                    run['status'] = JobStatus.error
                    run['job-type'] = JobType.error

        # is this a mbtiles part 1 job array
        elif run['job-type'] == JobType.compute_mbtiles_11:
            """
            ["maxwvel.63.tif", "--zlstart", "11", "--zlstop", "11", "--cpu", "4"],
            """

            # work the current state
            if run['status'] == JobStatus.new:
                # set the activity flag
                no_activity = False

                # create the additional command line parameters
                command_line_params = ['--outputDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--finalDIR', self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) + '/' +
                                       'final' + self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--inputFile'
                                       ]

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # move to the next stage
                run['status'] = JobStatus.compute_mbtiles_11_running
                run['status_prov'] += ',Compute mbtiles running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.compute_mbtiles_11_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.load_geo_server
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Compute mbtiles zoom 11 complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
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

                # create the additional command line parameters
                command_line_params = ['--instanceId', str(run['id'])]

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # set the current status
                run['status'] = JobStatus.load_geo_server_running
                run['status_prov'] += ', Load geo server running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.load_geo_server_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the type to complete
                    run['job-type'] = JobType.final_staging
                    run['status'] = JobStatus.new
                    run['status_prov'] += ', Load geo server complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
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

                # create the additional command line parameters
                command_line_params = ['--inputDir',
                                       self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] + '/' +
                                       str(run['id']) +
                                       self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--outputDir',
                                       self.k8s_config[run['job-type']]['DATA_MOUNT_PATH'] +
                                       self.k8s_config[run['job-type']]['SUB_PATH'],
                                       '--tarMeta',
                                       str(run['id'])]

                # create the job configuration for a new run
                self.k8s_create_job_obj(run, command_line_params, False)

                # execute the k8s job run
                self.k8s_create.execute(run)

                # set the current status
                run['status'] = JobStatus.final_staging_running
                run['status_prov'] += ', Final staging running'
                self.pg_db.update_job_status(run['id'], run['status_prov'])

                self.logger.info(f"Job created. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
            elif run['status'] == JobStatus.final_staging_running and run['status'] != JobStatus.error:
                # set the activity flag
                no_activity = False

                # find the job, get the status
                job_status, job_pod_status = self.k8s_find.find_job_info(run)

                # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                if job_status is None and not job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.info(f"Job complete. Run ID: {run['id']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}, Job delete status: {job_status}")

                    # set the next stage and stage status
                    run['job-type'] = JobType.complete
                    run['status'] = JobStatus.final_staging_complete
                    run['status_prov'] += ', Final staging complete'
                    self.pg_db.update_job_status(run['id'], run['status_prov'])
                # was there a failure
                elif job_pod_status.startswith('Failed'):
                    # remove the job and get the final run status
                    job_status = self.k8s_create.delete_job(run)

                    self.logger.error(f"Error: Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                    # set error conditions
                    run['job-type'] = JobType.error
                    run['status'] = JobStatus.error

        # return to the caller
        return no_activity

    def k8s_create_job_obj(self, run: dict, command_line_params: list, extend_output_path: bool = False):
        """
        Creates the details for a job from the database

        :return:
        """
        # create a new configuration if tis is a new run
        if run['status'] == JobStatus.new:
            # get the config
            config = self.get_config()[run['job-type']]

            # load the config with the info from the config file
            config['JOB_NAME'] += str(run['id'])
            config['DATA_VOLUME_NAME'] += str(run['id'])
            config['SSH_VOLUME_NAME'] += str(run['id'])
            config['COMMAND_LINE'].extend(command_line_params)

            # tack on any additional paths if requested
            if extend_output_path:
                config['SUB_PATH'] = '/' + str(run['id']) + config['SUB_PATH']
                config['COMMAND_LINE'].extend([config['DATA_MOUNT_PATH'] + config['SUB_PATH'] + config['ADDITIONAL_PATH']])

            # save these params in the run info
            run[run['job-type']] = {'run-config': config}

    def get_incomplete_runs(self):
        """
        get the list of instances that need processing

        :return: list of records to process
        """
        # storage for runs that are invalid
        invalid_runs: list = []

        # self.run_list.append({'id': 2620, 'job-type': JobType.staging, 'status': JobStatus.new, 'status_prov': 'New, Run accepted', 'downloadurl': 'http://tds.renci.org:8080/thredds/fileServer/2021/nam/2021010500/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast', 'gridname': 'hsofs'})
        #
        # return

        # get the new runs
        runs = self.pg_db.get_new_runs()

        # did we find anything to do
        if runs != -1 and runs is not None:
            # add this run to the list
            for run in runs:
                # continue only if we have everything needed for a run
                if 'downloadurl' in run['data'] and 'adcirc.gridname' in run['data']:
                    # create the new run
                    self.run_list.append(
                        {'id': run['instance_id'], 'job-type': JobType.staging, 'status': JobStatus.new, 'status_prov': 'New, Run accepted', 'downloadurl': run['data']['downloadurl'], 'gridname': run['data']['adcirc.gridname']})

                    # update the run status in the DB
                    self.pg_db.update_job_status(run['instance_id'], 'New, Run accepted')
                else:
                    # update the run status in the DB
                    self.pg_db.update_job_status(run['instance_id'], 'Error - Lacks the required run properties.')
                    invalid_runs.append(run['instance_id'])

            # output some info for the users
            if len(invalid_runs) > 0:
                self.logger.info(f"Run ID(s) {invalid_runs} lack the required run properties. Launch aborted.")

        # debugging only
        """
            SELECT id, key, value, instance_id FROM public."ASGS_Mon_config_item" where instance_id=2620;
            SELECT public.set_config_item(2620, 'supervisor_job_status', 'new');
            SELECT public.get_config_items_json(2620);
            SELECT public.get_supervisor_config_items_json();
            SELECT id, key, value, instance_id 
                FROM public."ASGS_Mon_config_item"
                where key in ('supervisor_job_status', 'downloadurl', 'adcirc.gridname')
                and instance_id in (select id from public."ASGS_Mon_instance" order by id desc)
                order by 4 desc, 2;   
        """
        # self.run_list.append({'id': 2620, 'job-type': JobType.staging, 'status': JobStatus.new, 'status_prov': 'New, Run accepted'})
        # self.run_list.append({'id': 2620, 'job-type': JobType.obs_mod, 'status': JobStatus.new, 'status_prov': 'New, Run accepted'})
        # self.run_list.append({'id': 2620, 'job-type': JobType.run_geo_tiff, 'status': JobStatus.new, 'status_prov': 'New, Run accepted'})
        # self.run_list.append({'id': 2620, 'job-type': JobType.compute_mbtiles_1, 'status': JobStatus.new, 'status_prov': 'New, Run accepted'})
