from kubernetes import client, config
import time
import os
from json import load


class K8sJobCreate:
    """
    Class that uses the k8s API to create, run and delete a job
    """

    # storage for the class configuration
    run_config: dict = {}

    def __init__(self):
        """
        inits the class
        """

        # load the run configuration params
        self.run_config = self.get_config()

    def create_job_object(self) -> client.V1Job:
        """
        creates a k8s job description object

        :return: client.V1Job, the job description object
        """

        # configure the volume mount for the container
        volume_mount = client.V1VolumeMount(
            name=self.run_config['VOLUME_NAME'],
            mount_path=self.run_config['MOUNT_PATH'])

        # configure a persistent claim
        persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{self.run_config["PVC_CLAIM"]}')

        # configure the volume claim
        volume = client.V1Volume(
            name=self.run_config['VOLUME_NAME'],
            persistent_volume_claim=persistent_volume_claim)

        # configure the pod template container
        container = client.V1Container(
            name=self.run_config['BASE_JOB_NAME'],
            image=self.run_config['IMAGE'],
            command=self.run_config['COMMAND_LINE'],
            volume_mounts=[volume_mount],
            image_pull_policy='IfNotPresent')

        # create and configure a spec section for the container
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": self.run_config['BASE_JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[volume]))

        # create the specification of job deployment
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=1)

        # instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=self.run_config['BASE_JOB_NAME']),
            spec=spec)

        # return the job object to the caller
        return job

    def create_job(self, api_instance, job) -> str:
        """
        creates the k8s job

        :param api_instance: client.BatchV1Api, the k8s API object
        :param job: client.V1Job, the job description object
        :return: str the job id
        """

        # create the job
        api_instance.create_namespaced_job(
            body=job,
            namespace=self.run_config['NAMESPACE'])

        print(f"Job created.")

        # init the done flag
        done: bool = False

        # init the job id storage
        job_id: str = ''

        # keep checking until done or fail
        while not done:
            # wait a period of time for the next check
            time.sleep(self.run_config['SLEEP'])

            # get the job run information
            ret = api_instance.list_namespaced_job(namespace=self.run_config['NAMESPACE'])

            # for each item returned
            for i in ret.items:
                # is this the one that was launched
                if i.metadata.labels['app'] == self.run_config['BASE_JOB_NAME']:
                    print(f'Found job: {self.run_config["BASE_JOB_NAME"]}, controller-uid: {i.metadata.labels["controller-uid"]}, status: {i.status.active}')

                    # has the job stopped running
                    if i.status.active != 1:
                        # save job id
                        job_id = i.metadata.labels["controller-uid"]

                        # get out of the loop
                        done = True

        # return the job guid
        return str(job_id)

    def delete_job(self, api_instance) -> str:
        """
        deletes the k8s job

        :param api_instance:
        :return:
        """

        api_response = api_instance.delete_namespaced_job(
            name=self.run_config['BASE_JOB_NAME'],
            namespace=self.run_config['NAMESPACE'],
            body=client.V1DeleteOptions(
                propagation_policy='Foreground',
                grace_period_seconds=5))

        print(f"Job deleted.")

        # return the final status of the job
        return str(api_response.status)

    @staticmethod
    def get_config() -> dict:
        """
        gets the run configuration

        :return: Dict of run params
        """

        # get the config file path/name
        config_name = os.path.join(os.path.dirname(__file__), '..', 'config.json')

        # open the config file
        with open(config_name, 'r') as json_file:
            # load the config items into a dict
            data: dict = load(json_file)

        # return the config data
        return data

    def execute(self) -> (str, str):
        """
        Executes the k8s job run
        :return: the job ID and run status
        """

        # load the k8s configuration
        config.load_kube_config()

        # create an API hook
        batch_v1 = client.BatchV1Api()

        # create the job object
        job = self.create_job_object()

        # create and launch the job
        job_id = self.create_job(batch_v1, job)

        # remove the job, get the run status of the job
        run_status = self.delete_job(batch_v1)

        # return the final status to the caller
        return job_id, run_status,


if __name__ == '__main__':
    # create the job creation class
    job_handler = K8sJobCreate()

    # execute the k8s job run
    job_run_id, job_status = job_handler.execute()

    print(f'Job run complete. Run status: {job_status}, job ID: {job_run_id}')

    exit(0)
