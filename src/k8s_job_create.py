import time
import os
import uuid
from json import load
from kubernetes import client, config
from src.k8s_job_find import K8sJobFind


class K8sJobCreate:
    """
    Class that uses the k8s API to create, run and delete a job
    """

    def __init__(self):
        """
        inits the class
        """
        # load the run configuration params
        self.k8s_config: dict = self.get_config()

    @staticmethod
    def create_job_object(job_config) -> client.V1Job:
        """
        creates a k8s job description object

        :return: client.V1Job, the job description object
        """

        # configure the volume mount for the container
        volume_mount = client.V1VolumeMount(
            name=job_config['job_details']['VOLUME_NAME'],
            mount_path=job_config['job_details']['MOUNT_PATH'])

        # configure a persistent claim
        persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{job_config["client"]["PVC_CLAIM"]}')

        # configure the volume claim
        volume = client.V1Volume(
            name=job_config['job_details']['VOLUME_NAME'],
            persistent_volume_claim=persistent_volume_claim)

        # configure the pod template container
        container = client.V1Container(
            name=job_config['job_details']['JOB_NAME'],
            image=job_config['job_details']['IMAGE'],
            command=job_config['job_details']['COMMAND_LINE'],
            volume_mounts=[volume_mount],
            image_pull_policy='IfNotPresent')

        # create and configure a spec section for the container
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": job_config['job_details']['JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[volume]))

        # create the specification of job deployment
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=1)

        # instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_config['job_details']['JOB_NAME']),
            spec=spec)

        # return the job object to the caller
        return job

    @staticmethod
    def create_job(job, job_config) -> str:
        """
        creates the k8s job

        :param job: client.V1Job, the job description object
        :param job_config: the configuration details
        :return: str the job id
        """
        # create the API hooks
        api_instance = client.BatchV1Api()

        # create the job
        api_instance.create_namespaced_job(
            body=job,
            namespace=job_config['client']['NAMESPACE'])

        # init the return storage
        job_id: str = ''

        # wait a period of time for the next check
        time.sleep(job_config['client']['POLL_SLEEP'])

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_config['client']['NAMESPACE'])

        # for each item returned
        for job in jobs.items:
            # is this the one that was launched
            if job.metadata.labels['app'] == job_config['job_details']['JOB_NAME']:
                # print(f'Found job: {job_config["job_details"]["JOB_NAME"]}, controller-uid: {job.metadata.labels["controller-uid"]}, status: {job.status.active}')

                # save job id
                job_id = str(job.metadata.labels["controller-uid"])

                # no need to continue looking
                break

        # return the job controller uid
        return job_id

    def delete_job(self, job_details) -> str:
        """
        deletes the k8s job

        :param job_details: the job configuration details
        :return:
        """

        # load the baseline config params
        job_config = self.k8s_config

        # add the job configuration details
        job_config['job_details'] = job_details

        # create an API hook
        api_instance = client.BatchV1Api()

        # remove the job
        api_response = api_instance.delete_namespaced_job(
            name=job_config['job_details']['JOB_NAME'],
            namespace=job_config['client']['NAMESPACE'],
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5))

        # return the final status of the job
        return str(api_response.status)

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

    def execute(self, job_details) -> str:
        """
        Executes the k8s job run
        :return: the job ID
        """

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

        # create the job object
        job = self.create_job_object(job_config)

        # create and launch the job
        job_id = self.create_job(job, job_config)

        # return the job id to the caller
        return job_id


if __name__ == '__main__':
    # create the job creation class
    job_handler = K8sJobCreate()

    # create a guid
    uid: str = str(uuid.uuid4())

    # load the job configuration params
    run_details = {
        'JOB_NAME': 'test-job-' + uid,
        'VOLUME_NAME': 'test-volume-' + uid,
        'IMAGE': 'test image',
        'COMMAND_LINE': [],
        'MOUNT_PATH': '/data/test_dir-' + uid}

    # execute the k8s job run
    job_run_id = job_handler.execute(run_details)

    print(f'Job {job_run_id} created.')

    # create the job finder object
    k8s_find = K8sJobFind()

    # find the run
    job_run_id, job_status, job_pod_status = k8s_find.find_job_info(run_details)

    # init the return value
    ret_val: int = 0

    # whats it look like
    if job_status != 1:
        # remove the job and get the final run status
        job_status = job_handler.delete_job(run_details)
        print(f'Job status {job_status}. Job ID: {job_run_id}, pod status: {job_pod_status}')
        ret_val = 1
    elif job_pod_status.startswith('pending'):
        # remove the job and get the final run status
        print(f'Job status {job_status}. Job ID: {job_run_id}, pod status: {job_pod_status}')
        ret_val = 1
    else:
        print(f'Job run started. Job ID: {job_run_id}, job status: {job_status}, pod status: {job_pod_status}')

    exit(ret_val)
