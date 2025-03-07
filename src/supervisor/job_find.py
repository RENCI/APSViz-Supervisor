# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Contains methods to find and interrogate a k8s job

    Author: Phil Owen, RENCI.org
"""

from kubernetes import client, config
from src.common.logger import LoggingUtil
from src.common.job_enums import JobType, JobStatus


class JobFind:
    """
    Class that uses the k8s API to find a job's details
    """

    def __init__(self):
        """
        inits the class
        """
        # get the log level and directory from the environment.
        log_level, log_path = LoggingUtil.prep_for_logging()

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Supervisor.JobFind", level=log_level, line_format='medium', log_file_path=log_path)

    def find_job_info(self, run: dict, job_type: JobType = None) -> (bool, str, str):
        """
        method to gather the k8s job information

        :param run:
        :param job_type:
        :return:
        """
        # set the default job type f not passed in
        if job_type is None:
            job_type = run['job-type']

        # load the job name and baseline cluster params
        sv_config = run[job_type]['job-config']['sv-config']
        job_name = run[job_type]['run-config']['JOB_NAME']

        # if this is not a fake job
        if not run['fake-jobs']:
            # load the k8s configuration
            try:
                # first, try to get the config if this is running on the cluster
                config.load_incluster_config()
            except config.ConfigException:
                try:
                    # else get the local config
                    config.load_kube_config(context=sv_config['CLUSTER'])
                except config.ConfigException as exc:
                    raise Exception("Could not configure kubernetes python client") from exc

            # create the API hooks
            api_instance = client.BatchV1Api()

            # init the status storage
            job_found: bool = False
            job_status: str = ''
            pod_status: str = ''

            # get the job run information
            jobs = api_instance.list_namespaced_job(namespace=sv_config['NAMESPACE'])

            # init the job status
            job_status: str = 'Pending'

            # for each item returned
            for job in jobs.items:
                # is this a valid job?
                if 'job-name' not in job.metadata.labels:
                    self.logger.error('Job with no "job-name" label element detected while looking in %s', job)
                # is this the one that was launched?
                elif job.metadata.labels['job-name'] == job_name:
                    # set the job found a flag
                    job_found = True

                    self.logger.debug('Found job: %s, controller-uid: %s, status: %s', job_name, job.metadata.labels["controller-uid"],
                                      job.status.active)

                    if job.status.active:
                        # set the job status
                        job_status = 'Running'
                    # did the job finish
                    else:
                        # did the job fail?
                        if job.status.failed:
                            job_status = 'Failed'
                            pod_status = 'Failed'
                        # did the job succeed?
                        elif job.status.succeeded:
                            pod_status = 'Succeeded'
                            job_status = 'Complete'

                        # no need to continue if the job was found and interrogated
                        break
        # fake jobs get this return status
        else:
            job_found = True
            pod_status = 'Succeeded'
            job_status = 'Complete'

        # return the job controller uid, job status and pod status
        return job_found, job_status, pod_status

    def find_failed_jobs(self, run):
        """
        Check if jobs are running and if they aren't set an error

        :param run:
        :return:
        """
        # init the return value
        ret_val: str = ''

        # get the list of jobs running
        try:
            # loop through all the workflow steps
            for item in run:
                # determine if this is a service based on the existence of the port definition
                if isinstance(item, JobType) and run[item]['job-config']['service'] is not None:
                    # find the job
                    job_found, job_status, _ = self.find_job_info(run, item)

                    # was the job found?
                    if job_found and job_status == 'Failed' and run['job-type'] != JobType.FINAL_STAGING:
                        # set the status of that step to failure
                        run['status'] = JobStatus.ERROR

                        # return an error message
                        ret_val = f", {run[item]['run-config']['job_name']} Job failed"

                        # no need to continue
                        break

        except Exception:
            self.logger.exception('Exception: Error during cleanup of jobs/services for Run ID: %s.', run['id'])

        # return to the caller
        return ret_val
