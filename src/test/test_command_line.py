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
    """
    tests getting the job types and prints out the command lines.

    :return:
    """
    # get a reference to the
    sv_cmds = JobSupervisor()

    # get the latest job definitions
    sv_cmds.k8s_job_configs = sv_cmds.get_job_configs()

    # for each workflow type
    for workflow_type, workflow_steps in sv_cmds.k8s_job_configs.items():
        # for each workflow job step
        for job_step in workflow_steps:
            # create a dummy run command from the step definition
            run = {'id': '<RUN ID>', 'workflow_type': workflow_type, 'job-type': job_step,
                   'downloadurl': '<TDS URL>', 'physical_location': '<SITE NAME>', 'gridname': '<GRID NAME>',
                   'forcing.stormnumber': '<STORM_NUMBER>'}

            # convert the job step name into a job type enum
            job_type = JobType(job_step)

            # get the base command
            base_cmd = sv_cmds.get_base_command_line(run, job_type)

            # create the full commend line
            new_cmd_list: list = workflow_steps[job_step]['COMMAND_LINE'].copy()

            # add the command matrix value
            new_cmd_list.extend(base_cmd[0])

            # make sure the commands were returned
            assert len(new_cmd_list) > 0

            # output for the user
            print(f'\njob_type: {job_type}\nDB cmd: {new_cmd_list}\nfinal command line: {" ".join([str(x) for x in new_cmd_list])}')
