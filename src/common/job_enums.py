# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
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
    HAZUS = 'hazus'
    LOAD_GEO_SERVER = 'load-geo-server'
    LOAD_GEO_SERVER_S3 = 'load-geo-server-s3'
    FINAL_STAGING = 'final-staging'
    ADCIRC2COG_TIFF = 'adcirc2cog-tiff'
    GEOTIFF2COG = 'geotiff2cog'
    OBS_MOD_AST = 'obs-mod-ast'
    ADCIRCTIME_TO_COG = 'adcirctime-to-cog'
    AST_RUN_HARVESTER = 'ast-run-harvester'
    COLLAB_DATA_SYNC = 'collab-data-sync'
    ADCIRC_TO_KALPANA_COG = 'adcirc-to-kalpana-cog'
    TIMESERIESDB_INGEST = 'timeseriesdb-ingest'

    DATABASE = 'database'
    OS = 'os'
    FORENSICS = 'forensics'

    ERROR = 'error'
    COMPLETE = 'complete'
