# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

import os
import logging
import datetime as dt

from json import load
from kubernetes import client, config
from common.logger import LoggingUtil


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

        self.job_timeout = self.k8s_config.get("JOB_TIMEOUT")

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

    def find_job_info(self, run) -> (bool, str, str):
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
        job_found: bool = False
        job_status: str = ''
        pod_status: str = ''
        container_count: int = 0

        # get the job run information
        jobs = api_instance.list_namespaced_job(namespace=job_details['NAMESPACE'])

        # get the pod status
        pods = core_api.list_namespaced_pod(namespace=job_details['NAMESPACE'])

        # init the job status
        job_status: str = 'Pending'

        # for each item returned
        for job in jobs.items:
            # is this a valid job
            if 'job-name' not in job.metadata.labels:
                self.logger.error(f'Job with no "job-name" label element detected while looking in {job}')
            # is this the one that was launched
            elif job.metadata.labels['job-name'] == job_name:
                # set the job found flag
                job_found = True

                self.logger.debug(f'Found job: {job_name}, controller-uid: {job.metadata.labels["controller-uid"]}, status: {job.status.active}')

                if job.status.active:
                    # set the job status
                    job_status = 'Running'
                # did the job finish
                else:
                    # did the job fail
                    if job.status.failed:
                        job_status = 'Failed'
                        pod_status = 'Failed'
                    # did the job succeed
                    elif job.status.succeeded:
                        pod_status = 'Succeeded'
                        job_status = 'Complete'

                    # TODO: the handling of insufficient resources seems to work without all the pod/container checking.
                    # commenting out for now until i see a container in a pod fail to see what happens and how to address it.
                    # it is my hope that the job restart policy will handle most if not all situations.

                    # # see how long this has been waiting to start (in seconds)
                    # time_diff = (dt.datetime.now() - run[run['job-type']]['job-start'])
                    #
                    # # if this has been inactive for some period of time (presumably waiting for resources)
                    # # if time_diff.total_seconds() > self.job_timeout:
                    # #     self.logger.error(f'Job timeout. Waited {time_diff.total_seconds()} seconds for {job_name}')
                    # #
                    # #     # set the status
                    # #     job_status = 'Timeout'
                    # #
                    # #     # no need to continue
                    # #     break
                    #
                    # # init a list for the jobs' pods
                    # job_pods: list = []
                    #
                    # # get the pods associated to this job
                    # for pod in pods.items:
                    #     # find the pods for this job
                    #     if pod.metadata.name.startswith(run[run['job-type']]['run-config']["JOB_NAME"]):
                    #         job_pods.append(pod)
                    #
                    # # go through the pods for this job and find one that may have succeeded
                    # for pod in job_pods:
                    #     # grab the status
                    #     pod_status = str(pod.status.phase)
                    #     self.logger.debug(f"pod name: {pod.metadata.name}, pod status: {pod_status}, job name: {job_name}")
                    #
                    #     # if there is a pod still working in this job wait for it to finish
                    #     if pod_status.startswith('Pending') or pod_status.startswith("Running"):
                    #         job_status = 'Running'
                    #         break
                    #
                    #     # loop through the container statuses in the pod. if we get
                    #     for status in pod.status.container_statuses:
                    #         if hasattr(status.state.terminated, 'reason'):
                    #             self.logger.debug(f"job name: {job_name}, pod name: {pod.metadata.name}, container status: {status.state.terminated.reason}")
                    #
                    #         # did the container succeed
                    #         if hasattr(status.state.terminated, 'reason') and status.state.terminated.reason.startswith('Completed'):
                    #             container_count += 1
                    #
                    #     # if the run is complete (or failed) then we should have found all the number of containers
                    #     # succeeded match the number requested
                    #     if container_count > 0:
                    #         # did the containers all succeed too
                    #         if container_count == run[run['job-type']]['total_containers']:
                    #             pod_status = 'Succeeded'
                    #             job_status = 'Complete'
                    #         else:
                    #             self.logger.error(f"{job_name} did not have the expected ({run[run['job-type']]['total_containers']}) number of completed containers ({container_count}).")
                    #             pod_status = 'Failed'
                    #

                    # no need to continue if the job was found and interrogated
                    break

        # return the job controller uid, job status and pod status
        return job_found, job_status, pod_status
