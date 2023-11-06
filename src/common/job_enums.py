# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class enums for the project

    Author: Phil Owen, RENCI.org
"""

from enum import Enum


class JobStatus(int, Enum):
    """
    Class that stores the job status constants
    """
    # new run status indicator
    NEW = 1

    # the run is currently active indicator
    RUNNING = 2

    # the run is complete indicator
    COMPLETE = 3

    # something non-fatal happened in the process indicator
    WARNING = 9999

    # something fatal happened in the process indicator
    ERROR = -1


class JobType(str, Enum):
    """
    Class that stores the job type name constants
    """
    STAGING = 'staging'
    DATABASE = 'database'
    PROVIDER = 'provider'
    CONSUMER = 'consumer'
    FORENSICS = 'forensics'
    FINAL_STAGING = 'final-staging'

    ERROR = 'error'
    COMPLETE = 'complete'
