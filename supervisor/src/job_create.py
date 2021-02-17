import time
import os
import logging

from json import load
from kubernetes import client, config
from common.logging import LoggingUtil


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

        # get the log level and directory from the environment
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_dir: int = int(os.getenv('LOG_DIR', os.path.dirname(__file__)))

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.JobCreate", level=log_level, line_format='medium', log_file_path=log_dir)

    @staticmethod
    def create_job_object(run, job_details):
        """
        creates a k8s job description object

        :return: client.V1Job, the job description object
        """

        # configure the data volume mount for the container
        data_volume_mount = client.V1VolumeMount(
            name=run[run['job-type']]['run-config']['DATA_VOLUME_NAME'],
            mount_path=run[run['job-type']]['run-config']['DATA_MOUNT_PATH'])

        # configure the ssh key volume mount for the container
        ssh_volume_mount = client.V1VolumeMount(
            name=run[run['job-type']]['run-config']['SSH_VOLUME_NAME'],
            read_only=True,
            mount_path=run[run['job-type']]['run-config']['SSH_MOUNT_PATH'])

        # configure a persistent claim for the data
        persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{job_details["PVC_CLAIM"]}')

        # configure a secret claim for the secret keys
        ssh_secret_claim = client.V1SecretVolumeSource(
            secret_name=f'{job_details["SECRETS_CLAIM"]}',
            default_mode=0o600)

        # configure the data volume claim
        data_volume = client.V1Volume(
            name=run[run['job-type']]['run-config']['DATA_VOLUME_NAME'],
            persistent_volume_claim=persistent_volume_claim)

        # configure the ssh secret claim
        ssh_volume = client.V1Volume(
            name=run[run['job-type']]['run-config']['SSH_VOLUME_NAME'],
            secret=ssh_secret_claim)

        ssh_username_env = client.V1EnvVar(
            name='SSH_USERNAME',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='ssh-username')))

        ssh_host_env = client.V1EnvVar(
            name='SSH_HOST',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='ssh-host')))

        asgs_db_username_env = client.V1EnvVar(
            name='ASGS_DB_USERNAME',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='asgs-username')))

        asgs_db_password_env = client.V1EnvVar(
            name='ASGS_DB_PASSWORD',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='asgs-password')))

        asgs_db_host_env = client.V1EnvVar(
            name='ASGS_DB_HOST',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='asgs-host')))

        asgs_db_port_env = client.V1EnvVar(
            name='ASGS_DB_PORT',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='asgs-port')))

        asgs_db_database_env = client.V1EnvVar(
            name='ASGS_DB_DATABASE',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='asgs-database')))

        geo_username_env = client.V1EnvVar(
            name='GEOSERVER_USER',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='geo-username')))

        geo_password_env = client.V1EnvVar(
            name='GEOSERVER_PASSWORD',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='geo-password')))

        geo_host_env = client.V1EnvVar(
            name='GEOSERVER_URL',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='geo-host')))

        geo_workspace_env = client.V1EnvVar(
            name='GEOSERVER_WORKSPACE',
            value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(
                name='eds-keys', key='geo-workspace')))

        # configure the pod template container
        container = client.V1Container(
            name=run[run['job-type']]['run-config']['JOB_NAME'],
            image=run[run['job-type']]['run-config']['IMAGE'],
            command=run[run['job-type']]['run-config']['COMMAND_LINE'],
            volume_mounts=[data_volume_mount, ssh_volume_mount],
            image_pull_policy='IfNotPresent',
            env=[ssh_username_env, ssh_host_env, asgs_db_username_env, asgs_db_password_env, asgs_db_host_env, asgs_db_port_env, asgs_db_database_env,
                 geo_username_env, geo_password_env, geo_host_env, geo_workspace_env]
            )

        # create and configure a spec section for the container
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": run[run['job-type']]['run-config']['JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container], volumes=[data_volume, ssh_volume]))

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

    def create_job(self, run) -> str:
        """
        creates the k8s job

        :param run: the run details
        :return: str the job id
        """
        # create the API hooks
        api_instance = client.BatchV1Api()

        job_data = run[run['job-type']]['job-config']
        job_details = job_data['job-details']
        run_details = run[run['job-type']]['run-config']

        # create the job
        api_instance.create_namespaced_job(
            body=job_data['job'],
            namespace=job_details['NAMESPACE'])

        # init the return storage
        job_id: str = ''

        # wait a period of time for the next check
        time.sleep(job_data['job-details']['CREATE_SLEEP'])

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_details['NAMESPACE'])

        # for each item returned
        for job in jobs.items:
            # is this the one that was launched
            if job.metadata.labels['app'] == run_details['JOB_NAME']:
                self.logger.debug(f"Found new job: {run_details['JOB_NAME']}, controller-uid: {job.metadata.labels['controller-uid']}, status: {job.status.active}")

                # save job id
                job_id = str(job.metadata.labels["controller-uid"])

                # no need to continue looking
                break

        # return the job controller uid
        return job_id

    @staticmethod
    def delete_job(run) -> str:
        """
        deletes the k8s job

        :param run: the run configuration details
        :return:
        """

        job_data = run[run['job-type']]['job-config']
        job_details = job_data['job-details']
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

    def execute(self, run):
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
                config.load_kube_config(context=job_details['CLUSTER'])
            except config.ConfigException:
                raise Exception("Could not configure kubernetes python client")

        # create the job object
        self.create_job_object(run, job_details)

        # create and launch the job
        job_id = self.create_job(run)

        # save these params onto the run info
        run[run['job-type']]['job-config']['job_id'] = job_id
