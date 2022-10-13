# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Main entry point for the application
"""

from supervisor.src.job_supervisor import APSVizSupervisor

# create the supervisor
supervisor = APSVizSupervisor()

try:
    # initiate the polling for work
    supervisor.run()
except Exception:
    # log the reason for the shutdown
    supervisor.logger.exception('K8s Supervisor (%s) is shutting down...', supervisor.system)

# let everyone know the application is shutting down
supervisor.util_objs['utils'].send_slack_msg(None, f'K8s Supervisor ({supervisor.system}) application is shutting down.',
                                             'slack_status_channel')
