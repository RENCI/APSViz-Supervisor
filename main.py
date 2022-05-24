# SPDX-FileCopyrightText: 2022 Phillips Owen <powen@renci.org>
#
# SPDX-License-Identifier: MIT

from supervisor.src.job_supervisor import APSVizSupervisor

# create the supervisor
supervisor = APSVizSupervisor()

# initiate the polling for work
supervisor.run()
