import os
from json import load
from kubernetes import client, config


class K8sJobFind:
    """
    Class that uses the k8s API to find a job's details
    """

    def __init__(self):
        """
        inits the class
        """
        # load the run configuration params
        self.k8s_config: dict = self.get_config()

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

    def find_job_info(self, job_details) -> (str, int, str):
        # load the baseline config params
        job_config = self.k8s_config

        # add the job configuration details
        job_config['job_details'] = job_details

        # load the k8s configuration
        try:
            # first try to get the config if this is running on the cluster
            config.load_incluster_config()
        except config.ConfigException:
            try:
                # else get the local config
                config.load_kube_config(context=job_config['client']['CONTEXT'])
            except config.ConfigException:
                raise Exception("Could not configure kubernetes python client")

        # create the API hooks
        api_instance = client.BatchV1Api()
        core_api = client.CoreV1Api()

        # init the status storage
        job_id: str = ''
        job_status: int = 0
        pod_status: str = ''

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_config['client']['NAMESPACE'])

        # get the pod status
        pods = core_api.list_namespaced_pod(namespace=job_config['client']['NAMESPACE'])

        # for each item returned
        for job in jobs.items:
            # is this the one that was launched
            if job.metadata.labels['job-name'] == job_config['job_details']['JOB_NAME']:
                print(f'Found job: {job_config["job_details"]["JOB_NAME"]}, controller-uid: {job.metadata.labels["controller-uid"]}, status: {job.status.active}')

                # save the job_id
                job_id = str(job.metadata.labels["controller-uid"])

                # get the job status
                job_status = job.status.active

                # get the container status
                for pod in pods.items:
                    if pod.metadata.name.startswith(job_config["job_details"]["JOB_NAME"]):
                        pod_status = str(pod.status.phase)

                        # no need to continue
                        break

                # no need to continue
                break

        # return the job controller uid, job status and pod status
        return job_id, job_status, pod_status
