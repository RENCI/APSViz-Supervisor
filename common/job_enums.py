# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

from enum import Enum


class JobStatus(int, Enum):
    """
    Class that stores the job status constants
    """
    # new run status indicator
    new = 1

    # the run is currently active indicator
    running = 2

    # the run is complete indicator
    complete = 3

    # something non-fatal happened in the process indicator
    warning = 9999

    # something fatal happened in the process indicator
    error = -1


class JobType(str, Enum):
    """
    Class that stores the job type name constants
    """
    staging = 'staging',
    hazus = 'hazus',
    hazus_singleton = 'hazus-singleton',
    obs_mod = 'obs-mod',
    run_geo_tiff = 'run-geo-tiff',
    compute_mbtiles_0_10 = 'compute-mbtiles-0-10',
    load_geo_server = 'load-geo-server',
    final_staging = 'final-staging',
    adcirc2cog_tiff = 'adcirc2cog-tiff',
    geotiff2cog = 'geotiff2cog',
    obs_mod_ast = 'obs-mod-ast',
    error = 'error',
    other_1 = 'TBD',
    complete = 'complete'
