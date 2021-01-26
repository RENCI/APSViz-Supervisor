import time
import os
import uuid
from json import load
from kubernetes import client, config


class JobCreate:
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
    def create_job_object(run, job_details) -> client.V1Job:
        """
        creates a k8s job description object

        :return: client.V1Job, the job description object
        """

        # configure the volume mount for the container
        volume_mount = client.V1VolumeMount(
            name=run[run['job-type']]['run-config']['VOLUME_NAME'],
            mount_path=run[run['job-type']]['run-config']['MOUNT_PATH'])

        # configure a persistent claim
        persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{job_details["client"]["PVC_CLAIM"]}')

        # configure the volume claim
        volume = client.V1Volume(
            name=run[run['job-type']]['run-config']['VOLUME_NAME'],
            persistent_volume_claim=persistent_volume_claim)

        # configure the pod template container
        container = client.V1Container(
            name=run[run['job-type']]['run-config']['JOB_NAME'],
            image=run[run['job-type']]['run-config']['IMAGE'],
            command=run[run['job-type']]['run-config']['COMMAND_LINE'],
            volume_mounts=[volume_mount],
            image_pull_policy='IfNotPresent')

        # create and configure a spec section for the container
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": run[run['job-type']]['run-config']['JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[volume]))

        # create the specification of job deployment
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=1)

        # instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=run[run['job-type']]['run-config']['JOB_NAME']),
            spec=spec)

        # save these params onto the run info
        run[run['job-type']]['job-config'] = {'job': job, 'job-details': job_details, 'job_id': '?'}


    @staticmethod
    def create_job(run) -> str:
        """
        creates the k8s job

        :param job: client.V1Job, the job description object
        :param job_config: the configuration details
        :return: str the job id
        """
        # create the API hooks
        api_instance = client.BatchV1Api()

        job_data = run[run['job-type']]['job-config']
        job_details = job_data['job-details']['client']
        run_details = run[run['job-type']]['run-config']

        # create the job
        api_instance.create_namespaced_job(
            body=job_data['job'],
            namespace=job_details['NAMESPACE'])

        # init the return storage
        job_id: str = ''

        # wait a period of time for the next check
        time.sleep(job_data['job-details']['client']['POLL_SLEEP'])

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_details['NAMESPACE'])

        # for each item returned
        for job in jobs.items:
            # is this the one that was launched
            if job.metadata.labels['app'] == run_details['JOB_NAME']:
                # print(f'Found job: {job_config["job_details"]["JOB_NAME"]}, controller-uid: {job.metadata.labels["controller-uid"]}, status: {job.status.active}')

                # save job id
                job_id = str(job.metadata.labels["controller-uid"])

                # no need to continue looking
                break

        # return the job controller uid
        return job_id

    def delete_job(self, run) -> str:
        """
        deletes the k8s job

        :param job_details: the job configuration details
        :return:
        """

        job_data = run[run['job-type']]['job-config']
        job_details = job_data['job-details']['client']
        run_details = run[run['job-type']]['run-config']

        # create an API hook
        api_instance = client.BatchV1Api()

        # remove the job
        api_response = api_instance.delete_namespaced_job(
            name=run_details['JOB_NAME'],
            namespace=job_details['NAMESPACE'],
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

    def execute(self, run) -> str:
        """
        Executes the k8s job run

        :return: the job ID
        """

        # load the baseline config params
        job_details = self.k8s_config

        # load the k8s configuration
        try:
            # first try to get the config if this is running on the cluster
            config.load_incluster_config()
        except config.ConfigException:
            try:
                # else get the local config
                config.load_kube_config(context=job_details['client']['CONTEXT'])
            except config.ConfigException:
                raise Exception("Could not configure kubernetes python client")

        # create the job object
        self.create_job_object(run, job_details)

        # create and launch the job
        job_id = self.create_job(run)

        # save these params onto the run info
        run[run['job-type']]['job-config']['job_id'] = job_id


if __name__ == '__main__':
    # create the job creation class. debug purposes only.
    job_handler = JobCreate()

    # create a guid
    uid: str = str(uuid.uuid4())

    # load the job configuration params
    run_details = {
        'JOB_NAME': 'test-job-' + uid,
        'VOLUME_NAME': 'test-volume-' + uid,
        'IMAGE': 'test image',
        'COMMAND_LINE': ["python", "execute_APSVIZ_pipeline.py", '--urljson', 'data1.json'],
        'MOUNT_PATH': '/data/test_dir-' + uid}

    # execute the k8s job run
    job_run_id = job_handler.execute(run_details)

    print(f'Job {job_run_id} created.')

    from job_find import JobFind

    # create the job finder object
    k8s_find = JobFind()

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
