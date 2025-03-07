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

    A key to a run object:
        run (a request for a workflow run): houses the details of all the workflow steps (jobs) in a run.
            It also includes the run request gathered from the DB.
        job_details (run[job type]): A workflow step identified by the job type that includes the run and job configs.
        run_config (job_details['run-config']): the parameters used to populate each workflow job/step template.
        job_config = (job_details['job-config']): k8s templates populated with the run-config params for each job/step.
"""
import time

from kubernetes import client, config
from src.common.logger import LoggingUtil
from src.common.job_enums import JobType, JobStatus, DBType
from src.common.utils import Utils


class JobCreate:
    """
    Class that uses the k8s API to create, run and delete a job
    """

    def __init__(self):
        """
        inits the class.

        """
        # get the log level and directory from the environment.
        log_level, log_path = LoggingUtil.prep_for_logging()

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Supervisor.JobCreate", level=log_level, line_format='medium', log_file_path=log_path)

        # load the base configuration params
        self.sv_config: dict = Utils.get_base_config()

        # set the resource limit multiplier
        self.limit_multiplier: float = float(self.sv_config.get("JOB_LIMIT_MULTIPLIER"))

        # set the job backoff limit
        self.back_off_limit: int = self.sv_config.get("JOB_BACKOFF_LIMIT")

        # get the time to live seconds after a finished job gets auto removed
        self.job_timeout: int = self.sv_config.get("JOB_TIMEOUT")

        # get the flag that indicates if there are cpu resource limits
        self.cpu_limits: bool = self.sv_config.get("CPU_LIMITS")

        # declare the secret environment variables
        self.secret_env_params: list = [{'name': 'LOG_LEVEL', 'key': 'log-level'},
                                        {'name': 'LOG_PATH', 'key': 'log-path'},
                                        {'name': 'ASGS_DB_HOST', 'key': 'apsviz-host'},
                                        {'name': 'ASGS_DB_PORT', 'key': 'apsviz-port'},
                                        {'name': 'ASGS_DB_USERNAME', 'key': 'apsviz-username'},
                                        {'name': 'ASGS_DB_PASSWORD', 'key': 'apsviz-password'},
                                        {'name': 'ASGS_DB_DATABASE', 'key': 'apsviz-database'},
                                        {'name': 'APSVIZ_DB_HOST', 'key': 'apsviz-host'},
                                        {'name': 'APSVIZ_DB_PORT', 'key': 'apsviz-port'},
                                        {'name': 'APSVIZ_DB_USERNAME', 'key': 'apsviz-username'},
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
                                        {'name': 'GEOSERVER_USER', 'key': 'geo-username'},
                                        {'name': 'GEOSERVER_PASSWORD', 'key': 'geo-password'},
                                        {'name': 'GEOSERVER_URL', 'key': 'geo-url'},
                                        {'name': 'GEOSERVER_URL_EXT', 'key': 'geo-url-ext'},
                                        {'name': 'GEOSERVER_HOST', 'key': 'geo-host'},
                                        {'name': 'GEOSERVER_PROJ_PATH', 'key': 'geo-proj-path'},
                                        {'name': 'GEOSERVER_WORKSPACE', 'key': 'geo-workspace'},
                                        {'name': 'AWS_ACCESS_KEY_ID', 'key': 'aws-access-key-id'},
                                        {'name': 'AWS_SECRET_ACCESS_KEY', 'key': 'aws-secret-access-key'},
                                        {'name': 'FILESERVER_HOST_URL', 'key': 'file-server-host-url'},
                                        {'name': 'FILESERVER_OBS_PATH', 'key': 'file-server-obs-path'},
                                        {'name': 'FILESERVER_CAT_PATH', 'key': 'file-server-cat-path'},
                                        {'name': 'CONTRAILS_KEY', 'key': 'contrails-key'},
                                        {'name': 'NO_PROXY', 'key': 'no-proxy-hosts'},
                                        {'name': 'no_proxy', 'key': 'no-proxy-hosts'},
                                        {'name': 'COG_MOSAIC_HOST', 'key': 'cog-mosaic-host'},
                                        {'name': 'COG_MOSAIC_PORT', 'key': 'cog-mosaic-port'},
                                        {'name': 'COG_MOSAIC_DATABASE', 'key': 'cog-mosaic-database'},
                                        {'name': 'COG_MOSAIC_USERNAME', 'key': 'cog-mosaic-username'},
                                        {'name': 'COG_MOSAIC_PASSWORD', 'key': 'cog-mosaic-password'},
                                        {'name': 'COG_MOSAIC_PATH', 'key': 'cog-mosaic-path'},
                                        {'name': 'PSC_SYNC_URL', 'key': 'psc_sync_url'},
                                        {'name': 'PSC_SYNC_TOKEN', 'key': 'psc_sync_token'},
                                        {'name': 'PSC_SYNC_PROJECTS', 'key': 'psc_sync_projects'},
                                        {'name': 'UI_DATA_URL', 'key': 'ui-data-url'},
                                        {'name': 'AST_IO_RETRY_PAUSE', 'key': 'ast-io-retry-pause'},
                                        {'name': 'SYSTEM', 'key': 'system'}]

    def execute(self, run: dict, job_type: JobType):
        """
        create and executes a k8s job

        :param run:
        :param job_type:
        :return: the job ID
        """

        # init the return
        job_id = None

        # save a reference to the job details, job configs and run config
        job_details = run[job_type]
        job_config = job_details['job-config']

        # load the k8s configuration
        try:
            # first, try to get the config if this is running on the cluster
            config.load_incluster_config()
        except Exception:
            try:
                # else get the local config. this local config must match the cluster name in your k8s config
                config.load_kube_config(context=self.sv_config['CLUSTER'])
            except config.ConfigException as exc:
                raise Exception("Could not configure kubernetes python client") from exc

        # create the job object
        self.create_job_object(run, job_type)

        # create and create the job
        job_id, svc_id = self.create_job(run, job_type)

        # save these params onto the run info
        job_details['job-config']['job_id'] = job_id
        job_details['job-config']['svc_id'] = svc_id

        # if this is a server process, mark it for cleanup at the end of the workflow
        if self.is_server_process(job_type):
            job_config['server-process'] = True

        # return to the caller
        return job_id

    def create_job_object(self, run: dict, job_type: JobType):
        """
        Creates a k8s job description object

        :param run:
        :param job_type:
        :return: client.V1Job, the job description object
        """

        # get a reference to the run details by run type
        job_details = run[job_type]
        run_config = job_details['run-config']

        # init a list for all the containers in this job
        containers: list = []

        # declare arrays for the volumes and volume mounts
        volume_mounts: list = []
        volumes: list = []

        # declare an array for the env declarations
        secret_envs: list = self.get_env_params(run['workflow_jobs'])

        # declare the shared volumes
        volumes, volume_mounts = self.declare_shared_volume(run['id'], run_config, job_type)

        # if there is an ephemeral volume name specified, mount it
        self.declare_ephemeral_volumes(run['id'], run_config, volume_mounts, volumes)

        # mount the NFS volume if this is a server
        security_context: client.V1PodSecurityContext = self.declare_nfs_volume(job_type, volume_mounts, volumes)

        # get the service configuration
        ports, service = self.create_svc_objects(run, job_type, run_config, secret_envs, volume_mounts, volumes)

        # get the ephemeral limit for the container
        if run_config['POD_EPHEMERAL_LIMIT'] is not None:
            pod_ephemeral_limit = run_config['POD_EPHEMERAL_LIMIT']
        else:
            pod_ephemeral_limit = '128Mi'

        # init the restart policy for the job
        restart_policy: str = 'Never'

        # add on the resources
        for idx, item in enumerate(run_config['COMMAND_MATRIX']):
            # get the base command line
            new_cmd_list: list = run_config['COMMAND_LINE'].copy()

            # add the command matrix value
            new_cmd_list.extend(item)

            # this is done to make the memory limit "self.limit_multiplier" greater than what is requested
            memory_val_txt: str = ''.join(x for x in run_config['MEMORY'] if x.isdigit())
            memory_unit_txt: str = ''.join(x for x in run_config['MEMORY'] if not x.isdigit())
            memory_limit_val: int = int(memory_val_txt) + int((int(memory_val_txt) * self.limit_multiplier))
            memory_limit: str = f'{memory_limit_val}{memory_unit_txt}'

            # use what is defined in the DB if it exists
            if run_config['CPUS']:
                cpus: str = run_config['CPUS']
            # this should never happen if the DB is set up properly
            else:
                cpus: str = '250m'

            # set this to "Never" when troubleshooting pod issues
            restart_policy: str = run_config['RESTART_POLICY']

            # get the baseline set of container resources
            resources: dict = {'requests': {'cpu': cpus, 'memory': run_config['MEMORY'], 'ephemeral-storage': '64Mi'},
                               'limits': {'memory': memory_limit, 'ephemeral-storage': pod_ephemeral_limit}}

            # if there is a cpu limit restriction, add it to the resource spec
            if self.cpu_limits:
                # parse the cpu text
                cpu_val_txt: str = ''.join(x for x in cpus if x.isdigit())
                cpu_unit_txt: str = ''.join(x for x in cpus if not x.isdigit())

                # this is done to make sure that the cpu limit is some percentage greater than what is created
                cpus_limit_val: int = int(cpu_val_txt) + int((int(cpu_val_txt) * self.limit_multiplier))

                # create the cpu specification
                cpus_limit: str = f'{cpus_limit_val}{cpu_unit_txt}'

                # append the limit onto the specification
                resources['limits'].update({'cpu': cpus_limit})

            # remove any empty elements. this becomes important when setting the pod into a loop
            # see get_base_command_line() in the job supervisor code
            if '' in new_cmd_list:
                new_cmd_list.remove('')

            # output the command line for debug runs
            if run['debug'] is True:
                self.logger.info('command line: %s', " ".join(new_cmd_list))

            # get the image name
            image_name: str = self.get_image_name(run, job_type)

            # add the container to the list
            containers.append(client.V1Container(name=job_details['run-config']['JOB_NAME'] + '-' + str(idx), image=image_name, command=new_cmd_list,
                                                 volume_mounts=volume_mounts, image_pull_policy='Always', env=secret_envs, resources=resources,
                                                 ports=ports))

        # save the number of containers in this job/pod for status checking later
        job_details['total_containers']: int = len(containers)

        # init the pod node and affinity selectors
        pod_node_selector = None  # pylint: disable=unused-variable
        pod_affinity_selector = None  # pylint: disable=unused-variable

        # if there was a node selector found, use it (AWS runs)
        if job_details['run-config']['NODE_TYPE']:
            # separate the tag and type
            params: list = run_config['NODE_TYPE'].split(':')

            # set the node selector
            node_selector: dict = {params[0]: params[1]}
        else:
            node_selector: dict = {}
            # create a node affinity to avoid using a list of on-prem k8s nodes.
            # pod_affinity_selector = client.V1Affinity(node_affinity=client.V1NodeAffinity(
            #     required_during_scheduling_ignored_during_execution=client.V1NodeSelector(node_selector_terms=[client.V1NodeSelectorTerm(
            #         match_expressions=[client.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn",
            #         values=["k8s-node02", "k8s-node10"])])])))

        # create and configure a spec section for the container
        pod_template: client.models.v1_pod_template_spec.V1PodTemplateSpec = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": run_config['JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy=restart_policy, containers=containers, volumes=volumes, node_selector=node_selector,
                                  security_context=security_context))

        # create the specification of job deployment, active_deadline_seconds=30
        job_spec: client.models.v1_job_spec.V1JobSpec = client.V1JobSpec(template=pod_template, backoff_limit=self.back_off_limit,
                                                                         ttl_seconds_after_finished=self.job_timeout)

        # instantiate the job object
        job: client.models.v1_job.V1Job = client.V1Job(api_version="batch/v1", kind="Job", metadata=client.V1ObjectMeta(name=run_config['JOB_NAME']),
                                                       spec=job_spec)

        # save these params onto the run info
        job_details['job-config']['job']: client.models.v1_job.V1Job = job
        job_details['job-config']['sv-config']: dict = self.sv_config
        job_details['job-config']['service']: dict = service

    def get_env_params(self, workflow_jobs: dict) -> list:
        """
        gets the environment variables

        :return:
        """
        # init the return environment params
        ret_val: list = []

        # get all the env params into an array
        for item in self.secret_env_params:
            # get the location of the secret
            secret_location = client.V1EnvVarSource(secret_key_ref=client.V1SecretKeySelector(name='eds-keys', key=item['key']))

            # add it to the list
            ret_val.append(client.V1EnvVar(name=item['name'], value_from=secret_location))

        # add in the jobs in this workflow
        for item in workflow_jobs:
            # add it to the list
            ret_val.append(client.V1EnvVar(name=item.name, value=str(workflow_jobs[item])[0]))

        # return the params
        return ret_val

    @staticmethod
    def declare_ephemeral_volumes(run_id: int, run_config: dict, volume_mounts: list, volumes: list):
        """
        Creates an ephemeral volume

        :param run_id:
        :param run_config:
        :param volume_mounts:
        :param volumes:
        :return:
        """
        # if there is a file server mount specified in the job type
        if run_config['FILESVR_VOLUME_NAME']:
            # get all the volume mount paths
            mount_paths: list = run_config['FILESVR_MOUNT_PATH'].split(',')
            vol_sizes: list = run_config['FILESVR_VOLUME_SIZE'].split(',')

            # create volumes and mounts
            for index, name in enumerate(run_config['FILESVR_VOLUME_NAME'].split(',')):
                # if no size is specified, just create a mount to an existing PVC
                if vol_sizes[index] == '0':
                    # create the claim
                    volumes.append(client.V1Volume(name=name, persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=name)))

                    # mount the volume
                    volume_mounts.append(client.V1VolumeMount(name=name, mount_path=mount_paths[index]))
                else:
                    # build the volume claim spec
                    pvc = client.V1PersistentVolumeClaimSpec(access_modes=['ReadWriteOnce'],
                                                             resources=client.V1ResourceRequirements(requests={'storage': vol_sizes[index]}))

                    # build the ephemeral name source
                    ephemeral_source = client.V1EphemeralVolumeSource(volume_claim_template=client.V1PersistentVolumeClaimTemplate(spec=pvc))

                    # build the volume definition
                    volumes.append(client.V1Volume(name=f"{name}-{run_id}", ephemeral=ephemeral_source))

                    # and the volume mounts
                    volume_mounts.append(client.V1VolumeMount(name=f"{name}-{run_id}", mount_path=mount_paths[index]))

    def declare_nfs_volume(self, job_type: JobType, volume_mounts: list, volumes: list) -> client.V1PodSecurityContext:
        """
        Creates a nfs volume mount

        :rtype: object
        :param job_type:
        :param volume_mounts:
        :param volumes:
        :return:
        """
        ret_val: client.V1PodSecurityContext = client.V1PodSecurityContext(None)

        # if this is a server process
        if self.is_os_server_process(job_type):
            # only create the nfs volume/mount for DB services
            if not self.is_os_server_process(job_type):
                # add in the NFS volume
                volumes.append(client.V1Volume(name='nfs-vol', nfs={'server': self.sv_config['NFS_SERVER'], 'path': self.sv_config['NFS_PATH']}))

                # declare the NFS volume mount
                volume_mounts.append(client.V1VolumeMount(name='nfs-vol', mount_path=self.sv_config['NFS_MOUNT']))

                # if this is not a server process with specific reason for not changing the security context
                if not self.is_server_process(job_type):
                    # create a pod security context for the final staging
                    ret_val: client.V1PodSecurityContext = client.V1PodSecurityContext(run_as_group=999, run_as_user=999, fs_group=1049)

        # return to the caller
        return ret_val

    def declare_shared_volume(self, run_id: int, run_config: dict, job_type: JobType) -> (list, list):
        """
        declares the shared volume mount

        :param run_id:
        :param run_config
        :param job_type:

        :return:
        """
        # declare the shared volume
        volumes: list = [client.V1Volume(name=run_config['DATA_VOLUME_NAME'], persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{self.sv_config["DATA_PVC_CLAIM"]}'))]

        # declare the shared volume mount
        volume_mounts: list = [client.V1VolumeMount(name=run_config['DATA_VOLUME_NAME'], mount_path=run_config['DATA_MOUNT_PATH'])]

        # id this is a DB process add in the shared memory volume for /dev/shm
        if self.is_db_server_process(job_type):
            volumes.append(client.V1Volume(name=f"dev-shm-{run_id}", empty_dir={'sizeLimit': '128Mi', 'medium': "Memory"}))

            # and the mount info for it
            volume_mounts.append(client.V1VolumeMount(name=f"dev-shm-{run_id}", mount_path='/dev/shm'))

        # return the default shared volume objects
        return volumes, volume_mounts

    def get_image_name(self, run: dict, job_type: JobType) -> str:
        """
        Gets the image name for certain job types

        :param run:
        :param job_type:
        :return:
        """
        # set the return value default (what is defined in the SV workflow step)
        ret_val: str = run[job_type]['run-config']['IMAGE']

        # is this a DB job?
        if self.is_db_server_process(job_type):
            # use th DB image in the request if assigned.
            if run['db_image']:
                # assign the DB image
                ret_val = run['db_image']
        # else is it an OS job
        elif self.is_os_server_process(job_type):
            # use the OS image in the request if assigned
            if run['os_image']:
                # assign the OS image
                ret_val = f'containers.renci.org/irods/{run["os_image"]}'

        # return the image name
        return ret_val

    @staticmethod
    def is_server_process(job_type: JobType) -> bool:
        """
        determines if the run is a server process using the existence of a port definition

        :param job_type:
        :return:
        """
        ret_val: bool = False

        # is this a server process?
        if job_type in [JobType.OS, JobType.DATABASE]:
            ret_val = True

        # return to the caller
        return ret_val

    @staticmethod
    def is_db_server_process(job_type: JobType) -> bool:
        """
        determines if the run is for a DB server process

        :param job_type:
        :return:
        """
        ret_val: bool = False

        # is this a DB server process?
        if job_type == JobType.DATABASE:
            ret_val = True

        # return to the caller
        return ret_val

    @staticmethod
    def is_os_server_process(job_type: JobType) -> bool:
        """
        determines if the run is for an OS server process

        :param job_type:
        :return:
        """
        ret_val: bool = False

        # is this an OS server process?
        if job_type == JobType.OS:
            ret_val = True

        # return to the caller
        return ret_val

    def create_svc_objects(self, run: dict, job_type: JobType, run_config: dict, secret_envs: list, volume_mounts: list, volumes: list) -> (
            list, client.models.v1_service.V1Service):
        """
        adds config volumes and a network service to the job deployment if desired

        :param run:
        :param job_type:
        :param run_config:
        :param secret_envs:
        :param volume_mounts:
        :param volumes:
        :return:
        """
        # init the output params
        ports: list = []
        service_config = None

        # init the volume info for init scripts
        cfg_map_info = []

        # if this is a deployment that requires network service, triggerd by a port declaration in the run config
        if self.is_server_process(job_type):
            # init the intermediate port details
            ports_config: list = []
            port_list: list = []

            # set the env params and a file system mount for a DB
            if self.is_db_server_process(job_type):
                # is this a postgres DB?
                if run['db_type'] == DBType.POSTGRESQL:
                    # set the environment params
                    secret_envs.append(client.V1EnvVar(name='POSTGRES_USER', value='postgres'))
                    secret_envs.append(client.V1EnvVar(name='POSTGRES_PASSWORD', value='testpassword'))
                    secret_envs.append(client.V1EnvVar(name='PGDATA', value='/var/lib/postgresql/data/db_data'))

                    # set the config map script name and mount
                    cfg_map_info = [['init-irods-pg-db', 'init-irods-pg-db.sh', '/docker-entrypoint-initdb.d/001-init-irods-db.sh']]
                elif run['db_type'] == DBType.MYSQL:
                    # set the environment params
                    secret_envs.append(client.V1EnvVar(name='MYSQL_USER', value='irods'))
                    secret_envs.append(client.V1EnvVar(name='MYSQL_ROOT_PASSWORD', value='testpassword'))
                    secret_envs.append(client.V1EnvVar(name='MYSQL_PASSWORD', value='testpassword'))

                    # set the config map script name and mount
                    cfg_map_info = [['init-irods-mysql-db', 'init-irods-mysql-db.sh', '/docker-entrypoint-initdb.d/001-init-irods-db.sh']]

            # set the env params and a file system mount for an OS
            elif self.is_os_server_process(job_type):
                # set the config map script name and mounts.
                cfg_map_info = [['irods-provider-install', 'irodsProviderInstall.sh', '/irods/irodsProviderInstall.sh'],
                                ['irods-provider-core-install', 'irodsProviderCoreInstall.sh', '/irods/irodsProviderCoreInstall.sh'],
                                ['provider-init', 'providerInit.json', '/irods/providerInit.json'],
                                ['00-irods', '00-irods.conf', '/etc/rsyslog.d/00-irods.conf'], ['irods', 'irods', '/etc/logrotate.d/irods'],
                                ['rsyslog', 'rsyslog.conf', '/etc/rsyslog.conf'], ['50-default', '50-default.conf', '/etc/rsyslog.d/50-default.conf']]

                # get the database service name. it is the same as the job name
                if JobType.DATABASE in run:
                    # save the DB host name
                    db_host_name: str = run[JobType.DATABASE]['run-config']['JOB_NAME']

                    # default the DB params
                    db_port_number: str = '5432'
                    db_driver_name: str = 'PostgreSQL ANSI'

                    # is this is a mysql DB?
                    if run['db_type'] == DBType.MYSQL:
                        # save the port and driver name values
                        db_port_number = '3306'
                        db_driver_name = 'MySQL ANSI'

                    # save the DB configs to environment variables to be used in job init scripts
                    secret_envs.append(client.V1EnvVar(name='DB_HOST_NAME', value=db_host_name))
                    secret_envs.append(client.V1EnvVar(name='DB_PORT_NUM', value=db_port_number))
                    secret_envs.append(client.V1EnvVar(name='DB_DRIVER_NAME', value=db_driver_name))
                    secret_envs.append(client.V1EnvVar(name='DB_TYPE_NAME', value=run['db_type']))

                    # add in the request name which is also the data directory name for the init scripts
                    secret_envs.append(client.V1EnvVar(name='REQUEST_GROUP', value=run['request_group']))

                    # add in the package directory
                    secret_envs.append(client.V1EnvVar(name='PACKAGE_DIR', value=run['package_dir']))

            # loop though all the configs map items defined and create mounts
            for item in cfg_map_info:
                # create a volume for the init script
                volumes.append(client.V1Volume(name=item[0], config_map=client.V1ConfigMapVolumeSource(name='supervisor-scripts', default_mode=511)))

                # mount the DB init script
                volume_mounts.append(client.V1VolumeMount(name=item[0], sub_path=f'{item[1]}', mount_path=item[2]))

            # there can be multiple ranges. go through them
            for port_range in run_config['PORT_RANGE']:
                # get all the ports in a single list
                port_list.extend(list(range(port_range[0], port_range[1] + 1)))

            # create ports on the container for the DB
            ports = [client.V1ContainerPort(name=f'sp-{x}', container_port=x) for x in port_list]

            # create the port configs
            ports_config = [client.V1ServicePort(name=f'db-sp-{x}', port=x, protocol='TCP', target_port=x) for x in port_list]

            # create a service to access the DB
            service_config = client.V1Service(api_version="v1",
                                              metadata=client.V1ObjectMeta(name=run_config['JOB_NAME'], labels={"app": run_config['JOB_NAME']}),
                                              spec=client.V1ServiceSpec(selector={"app": run_config['JOB_NAME']}, ports=ports_config,
                                                                        type='ClusterIP'))

        # return the port and service details
        return ports, service_config

    def create_job(self, run: dict, job_type: JobType) -> (object, object):
        """
        creates a k8s job

        :param run: the run details
        :param job_type:
        :return: str the job id
        """
        # create the API hooks
        job_api = client.BatchV1Api()
        service_api = client.CoreV1Api()

        # get references to places in the config to make things more readable
        job_config = run[job_type]['job-config']
        run_config = run[job_type]['run-config']

        # init the return storage
        job_id: str = ''
        svc_id: str = ''

        if not run['fake-jobs']:
            try:
                # create the job
                job_api.create_namespaced_job(body=job_config['job'], namespace=self.sv_config['NAMESPACE'])

                # create the service
                if self.is_server_process(job_type):
                    service_api.create_namespaced_service(body=job_config['service'], namespace=self.sv_config['NAMESPACE'])

            except client.ApiException:
                self.logger.exception("Error creating job: %s", run_config['JOB_NAME'])
                return None, None

            # wait a period of time for the next check
            time.sleep(self.sv_config['CREATE_SLEEP'])

            # get the job run information
            jobs = job_api.list_namespaced_job(namespace=self.sv_config['NAMESPACE'])

            # for each item returned
            for job in jobs.items:
                # is this the one that was launched?
                if 'app' in job.metadata.labels and job.metadata.labels['app'] == run_config['JOB_NAME']:
                    self.logger.debug("Found new job: %s, controller-uid: %s, status: %s", run_config['JOB_NAME'],
                                      job.metadata.labels['controller-uid'], job.status.active)

                    # save the job id
                    job_id = str(job.metadata.labels["controller-uid"])

                    # no need to continue looking
                    break

            # get the services
            svcs = service_api.list_namespaced_service(namespace=self.sv_config['NAMESPACE'])

            # for each item returned
            for svc in svcs.items:
                # is this the one that was launched?
                if 'app' in svc.metadata.labels and svc.metadata.labels['app'] == run_config['JOB_NAME']:
                    # save service id
                    svc_id = str(svc.metadata.uid)

                    # no need to continue looking
                    break
        # debug option
        else:
            job_id = 'fake-job-' + job_type

        # return the job controller uid
        return job_id, svc_id

    # @staticmethod
    def delete_job(self, run: dict, force: bool = False) -> str:
        """
        deletes the k8s job

        :param run: the run configuration details
        :param force:
        :return:
        """
        # init the return value
        ret_val: str = 'success'

        try:
            # if this is a debug run or if an error was detected, keep the jobs available for interrogation
            # note: a duplicate name collision on the next run could occur if the jobs are not removed
            # before the same run is restarted.
            if not run['debug'] and run['status'] != JobStatus.ERROR:
                job_type = run['job-type']
                job_details = run[job_type]
                run_config = job_details['run-config']

                # create the API hooks
                job_api = client.BatchV1Api()
                service_api = client.CoreV1Api()

                # remove the job if it is not a DB or OS server process. a service removal will be forced in the run cleanup operation
                if not self.is_server_process(job_type) or force:
                    # if this is a server process, kill the service first
                    if self.is_server_process(job_type):
                        # remove the service
                        service_api.delete_namespaced_service(run_config['JOB_NAME'], namespace=self.sv_config['NAMESPACE'],
                                                              body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=1))

                        self.logger.debug('Deleting service: Job name: %s', run_config['JOB_NAME'])

                    # remove the job
                    job_response = job_api.delete_namespaced_job(name=run_config['JOB_NAME'], namespace=self.sv_config['NAMESPACE'],
                                                                 body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=1))

                    self.logger.debug('Deleting job: %s', run_config['JOB_NAME'])

                    # set the return value
                    ret_val = job_response.status

        # trap any k8s call errors
        except Exception:
            ret_val = f"Job delete error, job {run['job-type']} may not exist."
            self.logger.debug("%s", ret_val)

        # return the final status of the job
        return ret_val

    def clean_up_jobs_and_svcs(self, run: dict) -> str:
        """
        iterates through the run steps to find/remove any lingering jobs/services

        :param run:
        """
        # init the return value
        ret_val: str = ', removing any remaining services'

        try:
            # loop through all the workflow steps
            for item in run:
                # determine if this is a service based on the existence of a port definition
                if isinstance(item, JobType):  # and run[item]['job-config']['service'] is not None
                    # set the run type
                    run['job-type'] = item

                    self.logger.debug('Removing job %s for Run ID: %s.', item, run['id'])

                    # delete the k8s job if it exists
                    self.delete_job(run, True)

        except Exception:
            self.logger.exception('Exception: Error during cleanup of jobs/services for Run ID: %s.', run['id'])

        self.logger.info("Stray job cleanup is complete for Run ID: %s.", run['id'])

        # return to the caller
        return ret_val

    def get_volume_size(self, job_type: JobType, vol_name: str):
        """
        gets the volume size based on the job type

        :param job_type:
        :param vol_name:
        :return:
        """
        # set the default size
        ret_val: str = '128Mi'

        # set the size based on the job type
        if self.is_server_process(job_type):
            # check for a specific PVC name to right-size the amount
            if vol_name.startswith('var-lib-pvc'):
                # set the amount
                ret_val = '6000Mi'
            elif vol_name.startswith('tmp-pvc'):
                ret_val = '2Gi'
            else:
                ret_val = '1Gi'
        # return to the caller
        return ret_val
