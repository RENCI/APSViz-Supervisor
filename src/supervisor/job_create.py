# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Methods to create a k8s job

    Author: Phil Owen, RENCI.org
"""

import time

from kubernetes import client, config
from src.common.logger import LoggingUtil
from src.common.job_enums import JobType, JobStatus
from src.common.utils import Utils


class JobCreate:
    """
    Class that uses the k8s API to create, run and delete a job
    """

    def __init__(self):
        """
        inits the K8s job creation class.

        """
        # get the log level and directory from the environment.
        log_level, log_path = LoggingUtil.prep_for_logging()

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Supervisor.JobCreate", level=log_level, line_format='medium', log_file_path=log_path)

        # load the base configuration params
        self.k8s_base_config: dict = Utils.get_base_config()

        # set the resource limit multiplier
        self.limit_multiplier: float = float(self.k8s_base_config.get("JOB_LIMIT_MULTIPLIER"))

        # set the job backoff limit
        self.back_off_limit: int = self.k8s_base_config.get("JOB_BACKOFF_LIMIT")

        # get the time to live seconds after a finished job gets auto removed
        self.job_timeout: int = self.k8s_base_config.get("JOB_TIMEOUT")

        # get the flag that indicates if there are cpu resource limits
        self.cpu_limits: bool = self.k8s_base_config.get("CPU_LIMITS")

        # declare the secret environment variables
        self.secret_env_params: list = [{'name': 'LOG_LEVEL', 'key': 'log-level'}, {'name': 'LOG_PATH', 'key': 'log-path'},
                                        {'name': 'ASGS_DB_HOST', 'key': 'apsviz-host'}, {'name': 'ASGS_DB_PORT', 'key': 'apsviz-port'},
                                        {'name': 'ASGS_DB_USERNAME', 'key': 'apsviz-username'},
                                        {'name': 'ASGS_DB_PASSWORD', 'key': 'apsviz-password'},
                                        {'name': 'ASGS_DB_DATABASE', 'key': 'apsviz-database'}, {'name': 'APSVIZ_DB_HOST', 'key': 'apsviz-host'},
                                        {'name': 'APSVIZ_DB_PORT', 'key': 'apsviz-port'}, {'name': 'APSVIZ_DB_USERNAME', 'key': 'apsviz-username'},
                                        {'name': 'APSVIZ_DB_PASSWORD', 'key': 'apsviz-password'},
                                        {'name': 'APSVIZ_DB_DATABASE', 'key': 'apsviz-database'},
                                        {'name': 'ADCIRC_OBS_DB_HOST', 'key': 'adcirc-obs-host'},
                                        {'name': 'ADCIRC_OBS_DB_PORT', 'key': 'adcirc-obs-port'},
                                        {'name': 'ADCIRC_OBS_DB_USERNAME', 'key': 'adcirc-obs-username'},
                                        {'name': 'ADCIRC_OBS_DB_PASSWORD', 'key': 'adcirc-obs-password'},
                                        {'name': 'ADCIRC_OBS_DB_DATABASE', 'key': 'adcirc-obs-database'},
                                        {'name': 'APSVIZ_GAUGES_DB_HOST', 'key': 'apsviz-gauges-host'},
                                        {'name': 'APSVIZ_GAUGES_DB_PORT', 'key': 'apsviz-gauges-port'},
                                        {'name': 'APSVIZ_GAUGES_DB_DATABASE', 'key': 'apsviz-gauges-database'},
                                        {'name': 'APSVIZ_GAUGES_DB_USERNAME', 'key': 'apsviz-gauges-username'},
                                        {'name': 'APSVIZ_GAUGES_DB_PASSWORD', 'key': 'apsviz-gauges-password'},
                                        {'name': 'APSVIZ_GAUGES_DB_ENGINE', 'key': 'apsviz-gauges-engine'},
                                        {'name': 'GEOSERVER_USER', 'key': 'geo-username'}, {'name': 'GEOSERVER_PASSWORD', 'key': 'geo-password'},
                                        {'name': 'GEOSERVER_URL', 'key': 'geo-url'}, {'name': 'GEOSERVER_URL_EXT', 'key': 'geo-url-ext'},
                                        {'name': 'GEOSERVER_HOST', 'key': 'geo-host'}, {'name': 'GEOSERVER_PROJ_PATH', 'key': 'geo-proj-path'},
                                        {'name': 'GEOSERVER_WORKSPACE', 'key': 'geo-workspace'},
                                        {'name': 'AWS_ACCESS_KEY_ID', 'key': 'aws-access-key-id'},
                                        {'name': 'AWS_SECRET_ACCESS_KEY', 'key': 'aws-secret-access-key'},
                                        {'name': 'FILESERVER_HOST_URL', 'key': 'file-server-host-url'},
                                        {'name': 'FILESERVER_OBS_PATH', 'key': 'file-server-obs-path'},
                                        {'name': 'FILESERVER_CAT_PATH', 'key': 'file-server-cat-path'},
                                        {'name': 'CONTRAILS_KEY', 'key': 'contrails-key'}, {'name': 'NO_PROXY', 'key': 'no-proxy-hosts'},
                                        {'name': 'no_proxy', 'key': 'no-proxy-hosts'}, {'name': 'COG_MOSAIC_HOST', 'key': 'cog-mosaic-host'},
                                        {'name': 'COG_MOSAIC_PORT', 'key': 'cog-mosaic-port'},
                                        {'name': 'COG_MOSAIC_DATABASE', 'key': 'cog-mosaic-database'},
                                        {'name': 'COG_MOSAIC_USERNAME', 'key': 'cog-mosaic-username'},
                                        {'name': 'COG_MOSAIC_PASSWORD', 'key': 'cog-mosaic-password'},
                                        {'name': 'COG_MOSAIC_PATH', 'key': 'cog-mosaic-path'}, {'name': 'PSC_SYNC_URL', 'key': 'psc_sync_url'},
                                        {'name': 'PSC_SYNC_TOKEN', 'key': 'psc_sync_token'},
                                        {'name': 'PSC_SYNC_PROJECTS', 'key': 'psc_sync_projects'}, {'name': 'UI_DATA_URL', 'key': 'ui-data-url'},
                                        {'name': 'AST_IO_RETRY_PAUSE', 'key': 'ast-io-retry-pause'}, {'name': 'SYSTEM', 'key': 'system'}]

    def create_job_object(self, run: dict, job_type: JobType, job_details: dict):
        """
        Creates a k8s job description object

        :param run:
        :param job_type:
        :param job_details:
        :return: client.V1Job, the job description object
        """

        # get a reference to the job type
        run_job = run[job_type]

        # declare the volume mounts
        volumes = [client.V1Volume(name=run_job['run-config']['DATA_VOLUME_NAME'],
                                   persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=f'{job_details["DATA_PVC_CLAIM"]}'))]
        volume_mounts = [client.V1VolumeMount(name=run_job['run-config']['DATA_VOLUME_NAME'], mount_path=run_job['run-config']['DATA_MOUNT_PATH'])]

        # if there is a desire to mount other persistent volumes
        if run_job['run-config']['FILESVR_VOLUME_NAME']:
            mount_paths = run_job['run-config']['FILESVR_MOUNT_PATH'].split(',')

            for index, name in enumerate(run_job['run-config']['FILESVR_VOLUME_NAME'].split(',')):
                # build the mounted volumes list
                volumes.append(client.V1Volume(name=name, persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=name)))
                volume_mounts.append(client.V1VolumeMount(name=name, mount_path=mount_paths[index]))

        # get the ephemeral limit
        if run_job['run-config']['EPHEMERAL'] is not None:
            ephemeral_limit = run_job['run-config']['EPHEMERAL']
        else:
            ephemeral_limit = '128Mi'

        # declare an array for the env declarations
        secret_envs = []

        # duplicate the environment param list
        secret_env_params = self.secret_env_params.copy()

        # load geo can't use the http_proxy values
        if job_type not in (JobType.LOAD_GEO_SERVER, JobType.LOAD_GEO_SERVER_S3):
            # add the proxy values to the env param list
            secret_env_params.extend([{'name': 'http_proxy', 'key': 'http-proxy-url'}, {'name': 'https_proxy', 'key': 'http-proxy-url'},
                                      {'name': 'HTTP_PROXY', 'key': 'http-proxy-url'}, {'name': 'HTTPS_PROXY', 'key': 'http-proxy-url'}])

        # get all the env params into an array
        for item in secret_env_params:
            secret_envs.append(client.V1EnvVar(name=item['name'], value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(name='eds-keys', key=item['key']))))

        # init a list for all the containers in this job
        containers: list = []

        # init the restart policy for the job
        restart_policy = 'Never'

        # add on the resources
        for idx, item in enumerate(run_job['run-config']['COMMAND_MATRIX']):
            # get the base command line
            new_cmd_list: list = run_job['run-config']['COMMAND_LINE'].copy()

            # add the command matrix value
            new_cmd_list.extend(item)

            # this is done to make the memory limit "self.limit_multiplier" greater than what is requested
            memory_val_txt = ''.join(x for x in run_job['run-config']['MEMORY'] if x.isdigit())
            memory_unit_txt = ''.join(x for x in run_job['run-config']['MEMORY'] if not x.isdigit())
            memory_limit_val = int(memory_val_txt) + int((int(memory_val_txt) * self.limit_multiplier))
            memory_limit = f'{memory_limit_val}{memory_unit_txt}'

            # use what is defined in the DB if it exists
            if run_job['run-config']['CPUS']:
                cpus = run_job['run-config']['CPUS']
            # this should never happen if the DB is set up properly
            else:
                cpus = '250m'

            # set this to "Never" when troubleshooting pod issues
            restart_policy = run_job['run-config']['RESTART_POLICY']

            # get the baseline set of container resources
            resources = {'limits': {'memory': memory_limit, 'ephemeral-storage': ephemeral_limit},
                         'requests': {'cpu': cpus, 'memory': run_job['run-config']['MEMORY'], 'ephemeral-storage': '64Mi'}}

            # if there is a cpu limit restriction add it to the resource spec
            if self.cpu_limits:
                # parse the cpu text
                cpu_val_txt = ''.join(x for x in cpus if x.isdigit())
                cpu_unit_txt = ''.join(x for x in cpus if not x.isdigit())

                # this is done to make sure that cpu limit is some percentage greater than what is created
                cpus_limit_val = int(cpu_val_txt) + int((int(cpu_val_txt) * self.limit_multiplier))

                # create the cpu specification
                cpus_limit = f'{cpus_limit_val}{cpu_unit_txt}'

                # append the limit onto the specification
                resources['limits'].update({'cpu': cpus_limit})

            # remove any empty elements. this becomes important when setting the pod into a loop
            # see get_base_command_line() in the job supervisor code
            if '' in new_cmd_list:
                new_cmd_list.remove('')

            # output the command line for debug runs
            if run['debug'] is True:
                self.logger.info('command line: %s', " ".join(new_cmd_list))

            # add the container to the list
            containers.append(client.V1Container(name=run_job['run-config']['JOB_NAME'] + '-' + str(idx), image=run_job['run-config']['IMAGE'],
                                                 command=new_cmd_list, volume_mounts=volume_mounts, image_pull_policy='Always', env=secret_envs,
                                                 resources=resources))

        # save the number of containers in this job/pod for status checking later
        run_job['total_containers'] = len(containers)

        # init the pod node and affinity selectors
        pod_node_selector = None
        pod_affinity_selector = None

        # if there was a node selector found use it (AWS runs)
        if run_job['run-config']['NODE_TYPE']:
            # separate the tag and type
            params = run_job['run-config']['NODE_TYPE'].split(':')

            # set the pod node selector
            pod_node_selector = {params[0]: params[1]}

        # else a run in the on-prem cluster
        # else:
        #     # create a node affinity to avoid using the on-prem k8s-node10 node.
        #     pod_affinity_selector = client.V1Affinity(node_affinity=client.V1NodeAffinity(
        #         required_during_scheduling_ignored_during_execution=client.V1NodeSelector(node_selector_terms=[client.V1NodeSelectorTerm(
        #             match_expressions=[client.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn", values=["k8s-node02",
        #                                                                                                                         "k8s-node10"])])])))

        # create and configure a spec section for the container
        template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels={"app": run_job['run-config']['JOB_NAME']}),
                                            spec=client.V1PodSpec(restart_policy=restart_policy, containers=containers, volumes=volumes,
                                                                  node_selector=pod_node_selector, affinity=pod_affinity_selector))

        # create the specification of job deployment
        job_spec = client.V1JobSpec(template=template, backoff_limit=self.back_off_limit, ttl_seconds_after_finished=self.job_timeout)

        # instantiate the job object
        job = client.V1Job(api_version="batch/v1", kind="Job", metadata=client.V1ObjectMeta(name=run_job['run-config']['JOB_NAME']), spec=job_spec)

        # save these params onto the run info
        run_job['job-config'] = {'job': job, 'job-details': job_details, 'job_id': '?'}

    def create_job(self, run: dict, job_type: JobType) -> object:
        """
        creates the k8s job

        :param run: the run details
        :param job_type:
        :return: str the job id
        """
        # create the API hooks
        api_instance = client.BatchV1Api()

        # get references to places in the config to make things more readable
        job_data = run[job_type]['job-config']
        job_details = job_data['job-details']
        run_details = run[job_type]['run-config']

        # init the return storage
        job_id: str = ''

        if not run['fake-jobs']:
            try:
                # create the job
                api_instance.create_namespaced_job(body=job_data['job'], namespace=job_details['NAMESPACE'])
            except client.ApiException:
                self.logger.exception("Error creating job: %s", run_details['JOB_NAME'])
                return None

            # wait a period of time for the next check
            time.sleep(job_data['job-details']['CREATE_SLEEP'])

            # get the job run information
            jobs = api_instance.list_namespaced_job(namespace=job_details['NAMESPACE'])

            # for each item returned
            for job in jobs.items:
                # is this the one that was launched
                if 'app' in job.metadata.labels and job.metadata.labels['app'] == run_details['JOB_NAME']:
                    self.logger.debug("Found new job: %s, controller-uid: %s, status: %s", run_details['JOB_NAME'],
                                      job.metadata.labels['controller-uid'], job.status.active)

                    # save job id
                    job_id = str(job.metadata.labels["controller-uid"])

                    # no need to continue looking
                    break
        else:
            job_id = 'fake-job-' + job_type

        # return the job controller uid
        return job_id

    # @staticmethod
    def delete_job(self, run: dict) -> str:
        """
        deletes the k8s job

        :param run: the run configuration details
        :return:
        """
        # if this is a debug run or if an error was detected keep the jobs available for interrogation
        # note: a duplicate name collision on the next run could occur if the jobs are not removed
        # before the same run is restarted.
        if not run['debug'] and run['status'] != JobStatus.ERROR:
            job_data = run[run['job-type']]['job-config']
            job_details = job_data['job-details']
            run_details = run[run['job-type']]['run-config']

            # create an API hook
            api_instance = client.BatchV1Api()

            try:
                # remove the job
                api_response = api_instance.delete_namespaced_job(name=run_details['JOB_NAME'], namespace=job_details['NAMESPACE'],
                                                                  body=client.V1DeleteOptions(propagation_policy='Foreground',
                                                                                              grace_period_seconds=5))

                # set the return value
                ret_val = api_response.status

            # trap any k8s call errors
            except Exception:
                ret_val = "Job delete error, job may no longer exist."
                self.logger.exception("%s", ret_val)
        else:
            ret_val = 'success'

        # return the final status of the job
        return ret_val

    def execute(self, run: dict, job_type: JobType):
        """
        Executes the k8s job run

        :param run:
        :param job_type:
        :return: the job ID
        """

        # init the return
        job_id = None

        # load the baseline config params
        job_details = self.k8s_base_config

        # load the k8s configuration
        try:
            # first try to get the config if this is running on the cluster
            config.load_incluster_config()
        except Exception:
            try:
                # else get the local config. this local config must match the cluster name in your k8s config
                config.load_kube_config(context=job_details['CLUSTER'])
            except config.ConfigException as exc:
                raise Exception("Could not configure kubernetes python client") from exc

        # create the job object
        self.create_job_object(run, job_type, job_details)

        # create and launch the job
        job_id = self.create_job(run, job_type)

        # save these params onto the run info
        run[job_type]['job-config']['job_id'] = job_id

        # return to the caller
        return job_id
