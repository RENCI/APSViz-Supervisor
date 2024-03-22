# BSD 3-Clause All rights reserved.
#
# SPDX-License-Identifier: BSD 3-Clause

"""
    Class enums for the project

    Author: Phil Owen, RENCI.org
"""

from enum import Enum


class JobStatus(int, Enum):
    """
    Enum class that stores the job status constants
    """
    # new run status indicator
    NEW = 1

    # the run is currently the active indicator
    RUNNING = 2

    # the run is the complete indicator
    COMPLETE = 3

    # something non-fatal happened in the process indicator
    WARNING = 9999

    # something fatal happened in the process indicator
    ERROR = -1


class DBType(str, Enum):
    """
    Enum class for the various database types
    """
    POSTGRESQL = "postgres"
    MYSQL = "mysql"


class JobType(str, Enum):
    """
    Enum class that lists the job type name constants
    """
    STAGING = 'staging'
    DATABASE = 'database'
    PROVIDER = 'provider'
    PROVIDERSECONDARY = 'providersecondary'
    CONSUMER = 'consumer'
    CONSUMERSECONDARY = 'consumersecondary'
    CONSUMERTERTIARY = 'consumertertiary'
    FORENSICS = 'forensics'
    FINAL_STAGING = 'final-staging'

    ERROR = 'error'
    COMPLETE = 'complete'
