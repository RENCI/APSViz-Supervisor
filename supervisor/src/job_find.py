import os
import logging

from json import load
from kubernetes import client, config
from common.logging import LoggingUtil


class JobFind:
    """
    Class that uses the k8s API to find a job's details
    """

    def __init__(self):
        """
        inits the class
        """
        # load the run configuration params
        self.k8s_config: dict = self.get_config()

        # get the log level and directory from the environment
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the dir if it does not exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.JobFind", level=log_level, line_format='medium', log_file_path=log_path)

    @staticmethod
    def get_config() -> dict:
        """
        gets the run configuration

        :return: Dict, baseline run params
        """

        # get the config file path/name
        config_name = os.path.join(os.path.dirname(__file__), '..', 'base_config.json')

        # open the config file
        with open(config_name, 'r') as json_file:
            # load the config items into a dict
            data: dict = load(json_file)

        # return the config data
        return data

    def find_job_info(self, run) -> (int, str):
        # load the baseline cluster params
        job_details = run[run['job-type']]['job-config']['job-details']
        job_name = run[run['job-type']]['run-config']['JOB_NAME']

        # load the k8s configuration
        try:
            # first try to get the config if this is running on the cluster
            config.load_incluster_config()
        except config.ConfigException:
            try:
                # else get the local config
                config.load_kube_config(context=job_details['CLUSTER'])
            except config.ConfigException:
                raise Exception("Could not configure kubernetes python client")

        # create the API hooks
        api_instance = client.BatchV1Api()
        core_api = client.CoreV1Api()

        # init the status storage
        job_status: int = 0
        pod_status: str = ''

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_details['NAMESPACE'])

        # get the pod status
        pods = core_api.list_namespaced_pod(namespace=job_details['NAMESPACE'])

        # for each item returned
        for job in jobs.items:
            # is this the one that was launched
            if job.metadata.labels['job-name'] == job_name:
                self.logger.info(f'Found running job: {job_name}, controller-uid: {job.metadata.labels["controller-uid"]}, status: {job.status.active}')

                # get the job status
                job_status = job.status.active

                # get the container status
                for pod in pods.items:
                    if pod.metadata.name.startswith(run[run['job-type']]['run-config']["JOB_NAME"]):
                        pod_status = str(pod.status.phase)

                        # no need to continue
                        break

                # no need to continue
                break

        # return the job controller uid, job status and pod status
        return job_status, pod_status
