# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Job supervisor tests.

    Author: Phil Owen, RENCI.org
"""
from src.supervisor.job_supervisor import JobSupervisor
from src.common.job_enums import JobType


def test_command_line():
    # get a reference to the
    sv = JobSupervisor()

    # get the latest job definitions
    sv.k8s_job_configs = sv.get_job_configs()

    # for each workflow type
    for workflow_type in sv.k8s_job_configs:
        # for each workflow job step
        for job_step in sv.k8s_job_configs[workflow_type]:
            # create a dummy run command
            run = {'id': '<RUN ID>', 'workflow_type': workflow_type, 'job-type': job_step,
                   'downloadurl': '<TDS URL>', 'physical_location': '<SITE NAME>', 'gridname': '<GRID NAME>',
                   'forcing.stormname': '<STORM_NAME>'}

            # convert the job step name into a job type enum
            job_type = JobType(job_step)

            # get the base command
            base_cmd = sv.get_base_command_line(run, job_type)

            # create the full commend line
            new_cmd_list: list = sv.k8s_job_configs[workflow_type][job_step]['COMMAND_LINE'].copy()

            # add the command matrix value
            new_cmd_list.extend(base_cmd[0])

            # output for the user
            print(f'\njob_type: {job_type}\ncmd: {new_cmd_list}\ncommand line: {" ".join([str(x) for x in new_cmd_list])}')
