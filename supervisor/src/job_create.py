# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

import time
import os
import logging
import datetime as dt

from json import load
from kubernetes import client, config
from common.logger import LoggingUtil
from common.job_enums import JobType


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
        log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the dir if it does not exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.JobCreate", level=log_level, line_format='medium', log_file_path=log_path)

        # set the resource limit multiplier
        self.limit_multiplier = float(self.k8s_config.get("JOB_LIMIT_MULTIPLIER"))

        # set the job backoff limit
        self.backoffLimit = self.k8s_config.get("JOB_BACKOFF_LIMIT")

        # declare the secret environment variables
        self.secret_env_params: list = [
            {'name': 'LOG_PATH', 'key': 'log-path'},
            {'name': 'ASGS_DB_HOST', 'key': 'asgs-host'},
            {'name': 'ASGS_DB_PORT', 'key': 'asgs-port'},
            {'name': 'ASGS_DB_USERNAME', 'key': 'asgs-username'},
            {'name': 'ASGS_DB_PASSWORD', 'key': 'asgs-password'},
            {'name': 'ASGS_DB_DATABASE', 'key': 'asgs-database'},
            {'name': 'APSVIZ_DB_USERNAME', 'key': 'apsviz-username'},
            {'name': 'APSVIZ_DB_PASSWORD', 'key': 'apsviz-password'},
            {'name': 'APSVIZ_DB_DATABASE', 'key': 'apsviz-database'},
            {'name': 'GEOSERVER_USER', 'key': 'geo-username'},
            {'name': 'GEOSERVER_PASSWORD', 'key': 'geo-password'},
            {'name': 'GEOSERVER_URL', 'key': 'geo-url'},
            {'name': 'GEOSERVER_URL_EXT', 'key': 'geo-url-ext'},
            {'name': 'GEOSERVER_HOST', 'key': 'geo-host'},
            {'name': 'GEOSERVER_PROJ_PATH', 'key': 'geo-proj-path'},
            {'name': 'GEOSERVER_WORKSPACE', 'key': 'geo-workspace'},
            {'name': 'FILESERVER_HOST_URL', 'key': 'file-server-host-url'},
            {'name': 'FILESERVER_OBS_PATH', 'key': 'file-server-obs-path'},
            {'name': 'FILESERVER_CAT_PATH', 'key': 'file-server-cat-path'},
            {'name': 'CONTRAILS_KEY', 'key': 'contrails-key'}
        ]

    # @staticmethod
    def create_job_object(self, run, job_details):
        """
        creates a k8s job description object

        :return: client.V1Job, the job description object
        """

        # save the start time of the job
        run[run['job-type']]['job-start'] = dt.datetime.now()

        # configure the data volume mount for the container
        data_volume_mount = client.V1VolumeMount(
            name=run[run['job-type']]['run-config']['DATA_VOLUME_NAME'],
            mount_path=run[run['job-type']]['run-config']['DATA_MOUNT_PATH'])

        # configure a persistent claim for the data
        data_persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{job_details["DATA_PVC_CLAIM"]}')

        # configure the data volume claim
        data_volume = client.V1Volume(
            name=run[run['job-type']]['run-config']['DATA_VOLUME_NAME'],
            persistent_volume_claim=data_persistent_volume_claim)

        # configure the ssh key volume mount for the container
        ssh_volume_mount = client.V1VolumeMount(
            name=run[run['job-type']]['run-config']['SSH_VOLUME_NAME'],
            read_only=True,
            mount_path=run[run['job-type']]['run-config']['SSH_MOUNT_PATH'])

        # configure a secret claim for the secret keys
        ssh_secret_claim = client.V1SecretVolumeSource(
            secret_name=f'{job_details["SECRETS_CLAIM"]}',
            default_mode=0o777)

        # configure the ssh secret claim
        ssh_volume = client.V1Volume(
            name=run[run['job-type']]['run-config']['SSH_VOLUME_NAME'],
            secret=ssh_secret_claim)

        # declare the volume mounts
        volumes = [data_volume, ssh_volume]
        volume_mounts = [data_volume_mount, ssh_volume_mount]

        # if there is a desire to mount the file server PV
        if run[run['job-type']]['run-config']['FILESVR_VOLUME_NAME']:
            volume_names = run[run['job-type']]['run-config']['FILESVR_VOLUME_NAME'].split(',')
            mount_paths = run[run['job-type']]['run-config']['FILESVR_MOUNT_PATH'].split(',')

            for index, value in enumerate(volume_names):
                # configure the data volume mount for the container
                filesvr_volume_mount = client.V1VolumeMount(
                    name=volume_names[index],
                    mount_path=mount_paths[index])

                # configure a persistent claim for the data
                filesvr_persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=f"{volume_names[index]}")

                # configure the data volume claim
                filesvr_volume = client.V1Volume(
                    name=volume_names[index],
                    persistent_volume_claim=filesvr_persistent_volume_claim)

                # add this to the mounted volumes list
                volumes.append(filesvr_volume)
                volume_mounts.append(filesvr_volume_mount)

        # declare an array for the env declarations
        secret_envs = []

        # duplicate the evn param list
        secret_env_params = self.secret_env_params.copy()

        # load geo can't use the http_proxy values
        # TODO: change this to something configurable
        if run['job-type'] != JobType.load_geo_server:
            # add the proxy values to the env param list
            secret_env_params.extend([{'name': 'http_proxy', 'key': 'http-proxy-url'},
                                      {'name': 'https_proxy', 'key': 'http-proxy-url'},
                                      {'name': 'HTTP_PROXY', 'key': 'http-proxy-url'},
                                      {'name': 'HTTPS_PROXY', 'key': 'http-proxy-url'}])

        # get all the env params into an array
        for item in secret_env_params:
            secret_envs.append(client.V1EnvVar(name=item['name'], value_from=client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='eds-keys', key=item['key']))))

        # init a list for all the containers in this job
        containers: list = []

        # init the restart policy for the job
        restart_policy = 'Never'

        # add on the resources
        for idx, item in enumerate(run[run['job-type']]['run-config']['COMMAND_MATRIX']):
            # get the base command line
            new_cmd_list: list = run[run['job-type']]['run-config']['COMMAND_LINE'].copy()

            # add the command matrix value
            new_cmd_list.extend(item)

            # this is done to make the memory limit "self.limit_multiplier" greater than what is requested
            memory_val_txt = ''.join(x for x in run[run['job-type']]['run-config']['MEMORY'] if x.isdigit())
            memory_unit_txt = ''.join(x for x in run[run['job-type']]['run-config']['MEMORY'] if not x.isdigit())
            memory_limit_val = int(memory_val_txt) + int((int(memory_val_txt) * self.limit_multiplier))
            memory_limit = f'{memory_limit_val}{memory_unit_txt}'

            # use what is defined in the DB if it exists
            if run[run['job-type']]['run-config']['CPUS']:
                cpus = run[run['job-type']]['run-config']['CPUS']
            # this should never happen if the DB is set up properly
            else:
                cpus = '250m'

            # this is done to make sure that cpu limit is some percentage greater than what is created
            cpu_val_txt = ''.join(x for x in cpus if x.isdigit())
            cpus_limit_val = int(cpu_val_txt) + int((int(cpu_val_txt) * self.limit_multiplier))

            # for processes that use a lot of cpu resources set some alternatives
            # TODO: make the restart policy something configurable
            #  - make sure we wait for resources to become available rather than having the job fail.
            #  - avoid giving it slightly higher cpu limits
            # set this to "Never" when troubleshooting pod issues
            if cpus_limit_val >= 2:
                restart_policy = 'OnFailure'
                cpus_limit = cpus
            else:
                # make sure the cpu value is built up properly
                cpu_unit_txt = ''.join(x for x in cpus if not x.isdigit())
                cpus_limit = f'{cpus_limit_val}{cpu_unit_txt}'

            # get the baseline set of container resources
            resources = {'limits': {'cpu': cpus_limit, 'memory': memory_limit, 'ephemeral-storage': '128Mi'}, 'requests': {'cpu': cpus, 'memory': run[run['job-type']]['run-config']['MEMORY'], 'ephemeral-storage': '50Mi'}}

            # if the command line has a '--cpu' in it replace the "~" value with the cpu amount specified when the cpu value is > .5 cpus
            if '--cpu' in new_cmd_list and int(cpu_val_txt)/1000 > .5:
                new_cmd_list = list(map(lambda val: val.replace("~", f"{int(int(cpu_val_txt)/1000)}"), new_cmd_list))

            # remove any empty elements. this becomes important when setting the pod into a loop
            # see get_base_command_line() in the supervisor code
            if '' in new_cmd_list:
                new_cmd_list.remove('')

            # output the command line for debug runs
            if run['debug'] is True:
                self.logger.info(f'command line: {" ".join(new_cmd_list)}')

            # configure the pod template container
            container = client.V1Container(
                name=run[run['job-type']]['run-config']['JOB_NAME'] + '-' + str(idx),
                image=run[run['job-type']]['run-config']['IMAGE'],
                command=new_cmd_list,
                volume_mounts=volume_mounts,
                image_pull_policy='IfNotPresent',
                env=secret_envs,
                resources=resources
                )

            # add the container to the list
            containers.append(container)

        # create a security context for the pod
        # security_context = client.V1PodSecurityContext(run_as_user=1000, fs_group=2000, run_as_group=3000)

        # if there was a node selector found use it
        if run[run['job-type']]['run-config']['NODE_TYPE']:
            # separate the tag and type
            params = run[run['job-type']]['run-config']['NODE_TYPE'].split(':')

            # set the node selector
            node_selector = {params[0]: params[1]}
        else:
            node_selector = None

        # create and configure a spec section for the container
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": run[run['job-type']]['run-config']['JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy=restart_policy, containers=containers, volumes=volumes, node_selector=node_selector)  # , security_context=security_context
        )

        # create the specification of job deployment
        job_spec = client.V1JobSpec(
            template=template,
            backoff_limit=self.backoffLimit,
            ttl_seconds_after_finished=600
            )

        # instantiate the job object
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=run[run['job-type']]['run-config']['JOB_NAME']),
            spec=job_spec
        )

        # save these params onto the run info
        run[run['job-type']]['job-config'] = {'job': job, 'job-details': job_details, 'job_id': '?'}

    def create_job(self, run) -> object:
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

        try:
            # create the job
            api_instance.create_namespaced_job(
                body=job_data['job'],
                namespace=job_details['NAMESPACE'])
        except client.ApiException as ae:
            self.logger.error(f"Error creating job: {run_details['JOB_NAME']}")
            return None

        # init the return storage
        job_id: str = ''

        # wait a period of time for the next check
        time.sleep(job_data['job-details']['CREATE_SLEEP'])

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_details['NAMESPACE'])

        # for each item returned
        for job in jobs.items:
            # is this the one that was launched
            if 'app' in job.metadata.labels and job.metadata.labels['app'] == run_details['JOB_NAME']:
                self.logger.debug(f"Found new job: {run_details['JOB_NAME']}, controller-uid: {job.metadata.labels['controller-uid']}, status: {job.status.active}")

                # save job id
                job_id = str(job.metadata.labels["controller-uid"])

                # no need to continue looking
                break

        # return the job controller uid
        return job_id

    # @staticmethod
    def delete_job(self, run) -> str:
        """
        deletes the k8s job

        :param run: the run configuration details
        :return:
        """
        # if this is a debug run keep the jobs available for interrogation
        # note: a duplicate name collision on the next run could occur
        # if the jobs are not removed before the same run is restarted.
        if not run['debug']:
            job_data = run[run['job-type']]['job-config']
            job_details = job_data['job-details']
            run_details = run[run['job-type']]['run-config']

            # create an API hook
            api_instance = client.BatchV1Api()

            try:
                # remove the job
                api_response = api_instance.delete_namespaced_job(
                    name=run_details['JOB_NAME'],
                    namespace=job_details['NAMESPACE'],
                    body=client.V1DeleteOptions(
                        propagation_policy='Foreground',
                        grace_period_seconds=5))

                # set the return value
                ret_val = api_response.status

            # trap any k8s call errors
            except (client.exceptions.ApiException, Exception) as e:
                ret_val = "Job delete error, job may no longer exist."
                self.logger.error(f'{ret_val}: {e}')
        else:
            ret_val = 'success'

        # return the final status of the job
        return ret_val

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

        # init the return
        job_id = None

        # load the baseline config params
        job_details = self.k8s_config

        # load the k8s configuration
        try:
            # first try to get the config if this is running on the cluster
            config.load_incluster_config()
        except config.ConfigException:
            try:
                # else get the local config. this local config must match the cluster name in your k8s config
                config.load_kube_config(context=job_details['CLUSTER'])
            except config.ConfigException:
                raise Exception("Could not configure kubernetes python client")

        # create the job object
        self.create_job_object(run, job_details)

        # create and launch the job
        job_id = self.create_job(run)

        # save these params onto the run info
        run[run['job-type']]['job-config']['job_id'] = job_id

        # return to the caller
        return job_id
