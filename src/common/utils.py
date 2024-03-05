# BSD 3-Clause All rights reserved.
#
# SPDX-License-Identifier: BSD 3-Clause

"""
    utility methods for the project

    Author: Phil Owen, RENCI.org
"""

import os
import datetime as dt
from json import load


class Utils:
    """
    Methods that are common to components in the project
    """

    def __init__(self, logger, system, app_version):
        """
        Initialization of this class

        """
        # assign the logger
        self.logger = logger

        # assign the system name
        self.system = system

        # assign the system name
        self.app_version = app_version

        # get the config data
        self.k8s_config: dict = Utils.get_base_config()

    @staticmethod
    def get_base_config() -> dict:
        """
        gets the baseline run configuration

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

    @staticmethod
    def get_run_time_delta(run: dict) -> str:
        """
        sets the duration of a job in the run configuration.

        :param run:
        :return:
        """
        # get the time difference
        delta = dt.datetime.now() - run['run-start']

        # get it into minutes and seconds
        minutes = divmod(delta.seconds, 60)

        # return the duration to the caller
        return f'in {minutes[0]} minutes and {minutes[1]} seconds'

    def check_last_run_time(self, last_run_time: dt.datetime) -> dt.datetime:
        """
        checks to see if we have not had a run in an allotted period of time.

        :param last_run_time:
        :return:
        """
        # get the time difference
        delta = dt.datetime.now() - last_run_time

        # get it into hours and minutes
        hours = divmod(delta.seconds, 3600)  # 3600

        # if we reach the magic number of hours send a Slack message
        if hours[0] >= self.k8s_config.get("SV_INACTIVITY"):
            # build up the message
            msg = f'The Supervisor application has not seen any new runs in the last {self.k8s_config.get("SV_INACTIVITY")} hours.'

            # log the event
            self.logger.info(msg)

            # reset the clock
            last_run_time = dt.datetime.now()

        # return the last run time
        return last_run_time
