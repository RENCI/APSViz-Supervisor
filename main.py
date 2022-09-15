# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Main entry point for application
"""

from supervisor.src.job_supervisor import APSVizSupervisor

# create the supervisor
supervisor = APSVizSupervisor()

# initiate the polling for work
supervisor.run()
