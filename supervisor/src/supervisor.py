import time
import uuid
import os
from json import load
from enum import Enum
from supervisor.src.job_create import JobCreate
from supervisor.src.job_find import JobFind
from postgres.src.pg_utils import PGUtils


class JobStatus(int, Enum):
    """
    Class that stores the job status constants
    """
    # new job status
    new = 1

    # file staging statuses
    staging_running = 10
    staging_complete = 20

    # adcirc supp statuses
    adcirc_supp_running = 30
    adcirc_supp_complete = 40

    # adcirc geo tiff statuses
    adcirc2geotiff_running = 50
    adcirc2geotiff_complete = 60

    # geo tiff to mbtile statuses
    geotiff2mbtiles_running = 70
    geotiff2mbtiles_complete = 80

    # final operations statuses
    final_running = 90
    final_complete = 100

    warning = 999
    error = -1


class JobType(str, Enum):
    """
    Class that stores the job type constants
    """
    staging = 'staging',
    adcirc_supp = 'adcirc-supp',
    adcirc2geotiff = 'adcirc2geotiff',
    geotiff2mbtiles = 'geotiff2mbtiles',
    final = 'final',
    error = 'error'
    other_1 = 'TBD'


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

    # TODO: make this a common function
    @staticmethod
    def get_config() -> dict:
        """
        gets the run configuration

        :return: Dict, baseline run params
        """

        # get the config file path/name
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
                job_status: str = 'Init'
                job_pod_status: str = 'Init'

                # is this a staging job
                if run['job-type'] == JobType.staging:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record?
                        command_line_params = ['--inputURL', 'http://tds.renci.org:8080/thredds/fileServer/2021/nam/2021010500/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/namforecast/', '--outputDir']

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # set the current status
                        run['status'] = JobStatus.staging_running

                        print(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.staging_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            print(f"Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set the next stage and stage status
                            run['job-type'] = JobType.adcirc_supp
                            run['status'] = JobStatus.new

                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            run['status'] = JobStatus.error

                            print(f"Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            run['job-type'] = JobType.error

                # what type of process is this
                elif run['job-type'] == JobType.adcirc_supp:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record?
                        command_line_params = ['--inputURL', '/data/' + run['id'] + '/staging/input/fort.63.nc', '--outputDir']

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        print(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")

                        # move to the next stage
                        run['status'] = JobStatus.adcirc_supp_running
                    elif run['status'] == JobStatus.adcirc_supp_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            print(f"Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set the next stage and stage status
                            run['job-type'] = JobType.final
                            run['status'] = JobStatus.new

                        # TODO: how should this be handled?
                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            run['status'] = JobStatus.error

                            print(f"Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            run['job-type'] = JobType.error

                # is this a staging job
                elif run['job-type'] == JobType.final:
                    # work the current state
                    if run['status'] == JobStatus.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record?
                        command_line_params = ['--inputDir', '/data/' + run['id'], '--outputDir', '/data/final', '--tarMeta', run['id']]

                        # create the job configuration for a new run
                        self.k8s_create_job_obj(run, command_line_params, False)

                        # execute the k8s job run
                        self.k8s_create.execute(run)

                        # set the current status
                        run['status'] = JobStatus.final_running

                        print(f"Job created. Job ID: {run[run['job-type']]['job-config']['job_id']}, Job type: {run['job-type']}")
                    elif run['status'] == JobStatus.final_running and run['status'] != JobStatus.error:
                        # set the activity flag
                        no_activity = False

                        # find the job, get the status
                        job_status, job_pod_status = self.k8s_find.find_job_info(run)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1 and not job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            print(f"Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            # set the next stage and stage status
                            run['status'] = JobStatus.final_complete

                        # TODO: how should this be handled?
                        elif job_pod_status.startswith('Failed'):
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(run)

                            run['status'] = JobStatus.error

                            print(f"Run status {run['status']}. Run ID: {run['id']}, Job type: {run['job-type']}, Job ID: {run[run['job-type']]['job-config']['job_id']}, job status: {job_status}, pod status: {job_pod_status}.")

                            run['job-type'] = JobType.error

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1
            else:
                no_activity_counter = 0

            # TODO: put these counts in the config file
            # check for something to do after a period of time
            if no_activity_counter >= 10:
                # wait longer for something to do
                time.sleep(120) # 120?

                # try once to see if there is something
                no_activity_counter = 9
            else:
                time.sleep(30)

    def k8s_create_job_obj(self, run: dict, command_line_params: list, extend_output_path: bool = True):
        """
        Creates the details for an adcirc-supp job from the database

        :return:
        """
        # TODO: most values to populate this object should come from the database or a configuration file
        if run['status'] == JobStatus.new:
            # get the config
            config = self.k8s_config[run['job-type']]

            # load the config with the info from the config file
            config['JOB_NAME'] += run['id']
            config['VOLUME_NAME'] += run['id']
            config['COMMAND_LINE'].extend(command_line_params)

            if extend_output_path:
                config['SUB_PATH'] = '/' + run['id'] + config['SUB_PATH']
                config['COMMAND_LINE'].extend([config['MOUNT_PATH'] + config['SUB_PATH'] + config['ADDITIONAL_PATH']])

            # save these params onto the run info
            run[run['job-type']] = {'run-config': config}

    def get_incomplete_runs(self) -> list:
        """
        TODO: get the list of instances that need processing
        :return: list of records to process
        """
        # create the SQL. raw SQL calls using the django db model need an ID
        sql = "SELECT public.get_config_list_json() AS data;"

        # create a new GUID
        uid: str = str(uuid.uuid4())

        # get the data
        #ret_val = self.pg_db.exec_sql(sql)

        # add this run to the list
        self.run_list.append({'id': uid, 'job-type': JobType.staging, 'status': JobStatus.new})
        #self.run_list.append({'id': uid, 'job-type': JobType.adcirc_supp, 'status': JobStatus.new})
        #self.run_list.append({'id': 'fcac6383-5043-4996-a0ee-91436245d032', 'job-type': JobType.final, 'status': JobStatus.new})