# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

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
