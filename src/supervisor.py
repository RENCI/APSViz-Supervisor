import time
import uuid
import os
from json import load
from enum import Enum
from src.k8s_job_create import K8sJobCreate
from src.k8s_job_find import K8sJobFind


# define the docker worker container images
class Image(str, Enum):
    adcirc_supp = 'phillipsowen/adcirc_supp'
    other_1 = "TBD"


class JobStage(int, Enum):
    """
    Class that stores the job stages
    """
    new = 1
    adcirc_supp_running = 2
    adcirc_supp_complete = 3
    warning = 99
    error = -1


class JobType(str, Enum):
    """
    Class that stores the job types
    """
    adcirc_supp = 'adcirc-supp'
    other_1 = 'TBD'


class APSVizSupervisor:
    """
    Class for the APSViz supervisor

    """

    # TODO: debug purposes only. this saves job details of the run
    saved_job_details = None

    def __init__(self):
        """
        inits the class
        """
        # load the run configuration params
        self.k8s_config: dict = self.get_config()

        # create a joj creator object
        self.k8s_create = K8sJobCreate()

        # create a job status finder object
        self.k8s_find = K8sJobFind()

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
        runs: list = self.get_incomplete_runs()

        # set counter that indicates nothing was done
        no_activity_counter: int = 0

        # until the end of time
        while True:
            # reset the activity flag
            no_activity: bool = True

            # for each run returned from the database
            for run in runs:
                # what type of process is this
                if run['type'] == JobType.adcirc_supp:
                    # save the current state of this job run
                    new_stage: JobStage = run['stage']

                    # work the current state
                    if run['stage'] == JobStage.new:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record
                        # create the job configuration for a new run
                        job_details = self.k8s_create_job_obj(JobStage.new, JobType.adcirc_supp)

                        # TODO: debug purposes only
                        self.saved_job_details = job_details

                        # execute the k8s job run
                        job_run_id = self.k8s_create.execute(job_details)

                        # move to the next stage
                        new_stage = JobStage.adcirc_supp_running

                        print(f'Job created. Job ID: {job_run_id}')
                    elif run['stage'] == JobStage.adcirc_supp_running:
                        # set the activity flag
                        no_activity = False

                        # TODO: this should be generated from a DB record
                        # create the job configuration for a new run
                        job_details = self.saved_job_details

                        # find the job, get the status
                        job_run_id, job_status, job_pod_status = self.k8s_find.find_job_info(job_details)

                        # if the job status is not active (!=1) it is complete or dead. either way it gets removed
                        if job_status != 1:
                            # remove the job and get the final run status
                            job_status = self.k8s_create.delete_job(job_details)

                            # save the state
                            state: str = 'deleted'

                            # set the next stage
                            new_stage = JobStage.adcirc_supp_complete
                        # TODO: this could be a real error state or the server is just busy
                        # TODO if a pod doesnt start within N minutes delete the job?
                        elif job_pod_status.startswith('Pending'):
                            # save the state
                            state: str = "Pod is " + job_pod_status

                            new_stage = JobStage.error
                        elif job_pod_status.startswith('Running'):
                            # save the state
                            state: str = "Pod is " + job_pod_status
                        else:
                            # save the state
                            state: str = 'is unknown'

                        print(f'Job state {state}. Job ID: {job_run_id}, job status: {job_status}, pod status: {job_pod_status}.')

                    # TODO: save the info back to the database
                    run['stage'] = new_stage

            # was there any activity
            if no_activity:
                # increment the counter
                no_activity_counter += 1
            else:
                no_activity_counter = 0

            # check for something to do after a period of time
            if no_activity_counter >= 10:
                # wait longer for something to do
                time.sleep(120)

                # try once to see if there is something
                no_activity_counter = 9
            else:
                time.sleep(5)

    def k8s_create_job_obj(self, job_stage: JobStage, job_type: JobType):
        """
        Creates the details for an adcirc-supp job from the database

        :return:
        """
        # TODO: most values to populate this object should come from the database or a configuration file
        if job_stage == JobStage.new:
            # create a guid
            uid: str = str(uuid.uuid4())

            # get the config
            config = self.k8s_config[job_type.value]

            # load the config with the ifo from the config file
            config['JOB_NAME'] += uid
            config['VOLUME_NAME'] += uid
            config['IMAGE'] = Image.adcirc_supp.value
            config['MOUNT_PATH'] += uid
        else:
            # return the saved copy for testing
            config = self.saved_job_details

        # return the job configuration
        return config

    @staticmethod
    def get_incomplete_runs() -> list:
        """
        TODO: get the incomplete runs from the database
        :return: list of records to process
        """
        return [{'type': 'adcirc-supp', 'stage': JobStage.new}]
