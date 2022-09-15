# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    uitiliyy methods for the project
"""

import os
from json import load


class Utils:
    """
    Methods that are common to components in the project
    """

    @staticmethod
    def get_config() -> dict:
        """
        gets the run configuration

        :return: Dict, baseline run params
        """

        # get the config file path/name
        config_name = os.path.join(os.path.dirname(__file__), 'base_config.json')

        # open the config file
        with open(config_name, 'r', encoding='utf-8') as json_file:
            # load the config items into a dict
            data: dict = load(json_file)

        # return the config data
        return data
