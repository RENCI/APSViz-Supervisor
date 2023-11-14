# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
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
from src.common.job_enums import JobType, JobStatus
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
        self.logger = LoggingUtil.init_logging("iRODS.Supervisor.JobCreate", level=log_level, line_format='medium', log_file_path=log_path)

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
            # first try to get the config if this is running on the cluster
            config.load_incluster_config()
        except Exception:
            try:
                # else get the local config. this local config must match the cluster name in your k8s config
                config.load_kube_config(context=self.sv_config['CLUSTER'])
            except config.ConfigException as exc:
                raise Exception("Could not configure kubernetes python client") from exc

        # create the job object
        self.create_job_object(run, job_type)

        # create and launch the job
        job_id, svc_id = self.create_job(run, job_type)

        # save these params onto the run info
        job_details['job-config']['job_id'] = job_id
        job_details['job-config']['svc_id'] = svc_id
        run_config = job_details['run-config']

        # if this is a server process mark it for cleanup at the end of the workflow
        if self.is_server_process(run_config):
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

        # declare an array for the env declarations
        secret_envs: list = []

        # duplicate the environment param list
        secret_env_params: list = self.secret_env_params.copy()

        # get all the env params into an array
        for item in secret_env_params:
            secret_envs.append(client.V1EnvVar(name=item['name'], value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(name='irods-keys', key=item['key']))))

        # declare the volumes
        volumes: list = [client.V1Volume(name=run_config['DATA_VOLUME_NAME'], persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=f'{self.sv_config["DATA_PVC_CLAIM"]}'))]

        # declare the volume mounts
        volume_mounts: list = [client.V1VolumeMount(name=run_config['DATA_VOLUME_NAME'], mount_path=run_config['DATA_MOUNT_PATH'])]

        # if there is a desire to mount other persistent volumes
        if run_config['FILESVR_VOLUME_NAME']:
            # get all the volume mount paths
            mount_paths: list = run_config['FILESVR_MOUNT_PATH'].split(',')

            # create volume claims for each volume name
            for index, name in enumerate(run_config['FILESVR_VOLUME_NAME'].split(',')):
                # build the mounted volumes and mounts list
                volumes.append(client.V1Volume(name=name, persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=name)))
                volume_mounts.append(client.V1VolumeMount(name=name, mount_path=mount_paths[index]))

        # get the service configuration
        ports, service = self.create_svc_objects(run, job_type, run_config, secret_envs, volume_mounts, volumes)

        # get the ephemeral limit
        if run_config['EPHEMERAL'] is not None:
            ephemeral_limit = run_config['EPHEMERAL']
        else:
            ephemeral_limit = '128Mi'

        # init a list for all the containers in this job
        containers: list = []

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
            resources: dict = {'limits': {'cpu': cpus, 'memory': memory_limit, 'ephemeral-storage': ephemeral_limit},
                               'requests': {'cpu': cpus, 'memory': run_config['MEMORY'], 'ephemeral-storage': '64Mi'}}

            # if there is a cpu limit restriction add it to the resource spec
            if self.cpu_limits:
                # parse the cpu text
                cpu_val_txt: str = ''.join(x for x in cpus if x.isdigit())
                cpu_unit_txt: str = ''.join(x for x in cpus if not x.isdigit())

                # this is done to make sure that cpu limit is some percentage greater than what is created
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

        # if there was a node selector found use it
        if run_config['NODE_TYPE']:
            # separate the tag and type
            params: list = run_config['NODE_TYPE'].split(':')

            # set the node selector
            node_selector: dict = {params[0]: params[1]}
        else:
            node_selector: dict = {}

        # create and configure a spec section for the container
        template: client.models.v1_pod_template_spec.V1PodTemplateSpec = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": run_config['JOB_NAME']}),
            spec=client.V1PodSpec(restart_policy=restart_policy, containers=containers, volumes=volumes, node_selector=node_selector))

        # create the specification of job deployment, active_deadline_seconds=30
        job_spec: client.models.v1_job_spec.V1JobSpec = client.V1JobSpec(template=template, backoff_limit=self.back_off_limit,
                                                                         ttl_seconds_after_finished=self.job_timeout)

        # instantiate the job object
        job: client.models.v1_job.V1Job = client.V1Job(api_version="batch/v1", kind="Job", metadata=client.V1ObjectMeta(name=run_config['JOB_NAME']),
                                                       spec=job_spec)

        # save these params onto the run info
        job_details['job-config']['job']: client.models.v1_job.V1Job = job
        job_details['job-config']['sv-config']: dict = self.sv_config
        job_details['job-config']['service']: dict = service

    @staticmethod
    def get_image_name(run, job_type):
        """
        Gets the image name for certain job types

        :param run:
        :param job_type:
        :return:
        """
        # set the return value default (what is defined in the DB)
        ret_val: str = run[job_type]['run-config']['IMAGE']

        # is this a DB job
        if job_type in [JobType.PG_DATABASE, JobType.MYSQL_DATABASE]:
            # use th DB image in the request if assigned
            if run['db_image']:
                # assign the DB image
                ret_val = run['db_image']
        # else is it an OS job
        elif job_type in [JobType.PROVIDER, JobType.CONSUMER]:
            # use th OS image in the request if assigned
            if run['os_image']:
                # assign the OS image
                ret_val = f'containers.renci.org/irods/irods-{run["os_image"]}'

        # return the image name
        return ret_val

    @staticmethod
    def is_server_process(run_config: dict) -> bool:
        """
        determines if the run is a server process using the existence of a port definition

        :return:
        """
        ret_val: bool = False

        # is this a server process
        if run_config['PORT_RANGE']:
            ret_val = True

        # return to the caller
        return ret_val

    @staticmethod
    def create_svc_objects(run: dict, job_type: JobType, run_config: dict, secret_envs: list, volume_mounts: list, volumes: list) -> (
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
        db_service_name: str = ''

        # init the volume info for init scripts
        cfg_map_info = []

        # if this is a deployment that requires network service, triggerd by a port declaration in the run config
        if run_config['PORT_RANGE']:
            # init the intermediate port details
            ports_config: list = []
            port_list: list = []

            # set the env params and a file system mount for a postgres DB
            if job_type == JobType.PG_DATABASE:
                # set the environment params
                secret_envs.append(client.V1EnvVar(name='POSTGRES_USER', value='postgres'))
                secret_envs.append(client.V1EnvVar(name='POSTGRES_PASSWORD', value='testpassword'))
                secret_envs.append(client.V1EnvVar(name='PGDATA', value='/var/lib/postgresql/data/db_data'))

                # set the config map script name and mount
                cfg_map_info = [['init-irods-pg-db', 'init-irods-pg-db.sh', '/docker-entrypoint-initdb.d/001-init-irods-db.sh']]

            # set the env params and a file system mount for a MySQL DB
            elif job_type == JobType.MYSQL_DATABASE:
                # set the environment params
                secret_envs.append(client.V1EnvVar(name='MYSQL_USER', value='irods'))
                secret_envs.append(client.V1EnvVar(name='MYSQL_ROOT_PASSWORD', value='testpassword'))
                secret_envs.append(client.V1EnvVar(name='MYSQL_PASSWORD', value='testpassword'))

                # set the config map script name and mount
                cfg_map_info = [['init-irods-mysql-db', 'init-irods-mysql-db.sh', '/docker-entrypoint-initdb.d/001-init-irods-db.sh']]

            # set the env params and a file system mount for a iRODS provider
            elif job_type == JobType.PROVIDER:
                # set the config map script name and mount
                # add these to run on irods logging
                # ['00-irods', '00-irods.conf', '/etc/rsyslog.d/00-irods.conf'],
                # ['irods', 'irods', '/etc/logrotate.d/irods'],
                cfg_map_info = [['irodsinstall', 'irodsInstall.sh', '/irods/irodsInstall.sh'],
                                ['serviceinit', 'serviceInit.json', '/irods/serviceInit.json']]

                # get the database service name. it is the same as the job name
                if JobType.PG_DATABASE in run:
                    db_service_name = run[JobType.PG_DATABASE]['run-config']['JOB_NAME']
                elif JobType.MYSQL_DATABASE in run:
                    db_service_name = run[JobType.MYSQL_DATABASE]['run-config']['JOB_NAME']

                # save the service name to the environment
                secret_envs.append(client.V1EnvVar(name='DB_SERVICE_NAME', value=db_service_name))

            # set the env params and a file system mount for a iRODS consumer
            # elif job_type == JobType.CONSUMER:
            #     # set the config map script name and mount
            #     cfg_map = [['init-irods-mysql-db', '/docker-entrypoint-initdb.d/001-init-irods-db.sh']]

            # loop though all the config map items defined and create mounts
            for item in cfg_map_info:
                # create a volume for the init script
                volumes.append(client.V1Volume(name=item[0], config_map=client.V1ConfigMapVolumeSource(name='supervisor-scripts', default_mode=511)))

                # mount the DB init script
                volume_mounts.append(client.V1VolumeMount(name=item[0], sub_path=f'{item[1]}', mount_path=item[2]))

            # there can be multiple ranges. go through them
            for port_range in run_config['PORT_RANGE']:
                # get all the ports in a single list
                port_list = list(range(port_range[0], port_range[1] + 1))

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
                if run_config['PORT_RANGE'] is not None:
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
                # is this the one that was launched
                if 'app' in job.metadata.labels and job.metadata.labels['app'] == run_config['JOB_NAME']:
                    self.logger.debug("Found new job: %s, controller-uid: %s, status: %s", run_config['JOB_NAME'],
                                      job.metadata.labels['controller-uid'], job.status.active)

                    job_id = str(job.metadata.labels["controller-uid"])

                    # no need to continue looking
                    break

            # get the services
            svcs = service_api.list_namespaced_service(namespace=self.sv_config['NAMESPACE'])

            # for each item returned
            for svc in svcs.items:
                # is this the one that was launched
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
            # if this is a debug run or if an error was detected keep the jobs available for interrogation
            # note: a duplicate name collision on the next run could occur if the jobs are not removed
            # before the same run is restarted.
            if not run['debug'] and run['status'] != JobStatus.ERROR:
                job_details = run[run['job-type']]
                run_config = job_details['run-config']

                # create the API hooks
                job_api = client.BatchV1Api()
                service_api = client.CoreV1Api()

                # remove the job if it is not a server process
                if not self.is_server_process(run_config) or force:
                    # remove the job
                    job_response = job_api.delete_namespaced_job(name=run_config['JOB_NAME'], namespace=self.sv_config['NAMESPACE'],
                                                                 body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=1))
                    # set the return value
                    ret_val = job_response.status

                    # if this is a server process
                    if self.is_server_process(run_config):
                        # remove the service
                        service_api.delete_namespaced_service(run_config['JOB_NAME'], namespace=self.sv_config['NAMESPACE'],
                                                              body=client.V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=1))

        # trap any k8s call errors
        except Exception:
            ret_val = "Job delete error, job may no longer exist."
            self.logger.exception("%s", ret_val)

        # return the final status of the job
        return ret_val

    def clean_up_jobs_and_svcs(self, run: dict):
        """
        iterates through the run steps to find/remove any lingering jobs/services

        :param run:
        """
        try:
            # loop through all the workflow steps
            for item in run:
                # determine if this is a service based on the existence of a port definition
                if isinstance(item, JobType) and run[item]['job-config']['service'] is not None:
                    # set the run type
                    run['job-type'] = item

                    # delete the k8s job if it exists
                    self.delete_job(run, True)
        except Exception:
            self.logger.exception('Error removing lingering jobs/services.')
