# BSD 3-Clause License All rights reserved.
#
# SPDX-License-Identifier: BSD 3-Clause License

"""
    Main entry point for the application
"""

from src.supervisor.job_supervisor import JobSupervisor

# create the supervisor
supervisor = JobSupervisor()

try:
    # initiate the polling for work
    supervisor.run()
except Exception:
    # log the reason for the shutdown
    supervisor.logger.exception('The iRODS K8s Job Supervisor (%s) is shutting down...', supervisor.system)
