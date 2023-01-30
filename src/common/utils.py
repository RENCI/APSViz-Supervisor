# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    utility methods for the project

    Author: Phil Owen, RENCI.org
"""

import os
import datetime as dt
from json import load
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class Utils:
    """
    Methods that are common to components in the project
    """

    def __init__(self, logger, system):
        """
        Initialization of this class

        """
        # assign the logger
        self.logger = logger

        # assign the system name
        self.system = system

        # init the Slack channels
        self.slack_channels: dict = {'slack_status_channel': os.getenv('SLACK_STATUS_CHANNEL'),
                                     'slack_issues_channel': os.getenv('SLACK_ISSUES_CHANNEL')}

    @staticmethod
    def get_base_config() -> dict:
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

    def send_slack_msg(self, run_id, msg, channel, debug_mode=False, instance_name=None):
        """
        sends a msg to the Slack channel

        :param run_id: the ID of the supervisor run
        :param msg: the msg to be sent
        :param channel: the Slack channel to post the message to
        :param debug_mode: mode to indicate that this is a no-op
        :param instance_name: the name of the ASGS instance
        :return: nothing
        """
        # init the final msg
        final_msg = f"APSViz Supervisor ({self.system}) - "

        # if there was an instance name use it
        final_msg += '' if instance_name is None else f'Instance name: {instance_name}, '

        # add the run id and msg
        final_msg += msg if run_id is None else f'Run ID: {run_id} {msg}'

        # log the message
        self.logger.info(final_msg)

        # send the message to Slack if not in debug mode and not running locally
        if not debug_mode and self.system in ['Dev', 'Prod', 'AWS/EKS']:
            # determine the client based on the channel
            if channel == 'slack_status_channel':
                client = WebClient(token=os.getenv('SLACK_STATUS_TOKEN'))
            else:
                client = WebClient(token=os.getenv('SLACK_ISSUES_TOKEN'))

            try:
                # send the message
                client.chat_postMessage(channel=self.slack_channels[channel], text=final_msg)
            except SlackApiError:
                # log the error
                self.logger.exception('Slack %s messaging failed. msg: %s', self.slack_channels[channel], final_msg)

    @staticmethod
    def get_run_time_delta(run) -> str:
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
        return f'in {minutes[0]} minutes, {minutes[1]} seconds'

    def check_last_run_time(self, last_run_time):
        # get the time difference
        delta = dt.datetime.now() - last_run_time

        # get it into hours and minutes
        hours = divmod(delta.seconds, 3600)

        # if we reach 8 hours send a Slack message
        if hours[0] == 8:
            msg = f'The Supervisor application has not seen any new runs in 8 hours.'

            # send the Slack message to the issues channel
            self.send_slack_msg(None, msg, 'slack_issues_channel', debug_mode=False, instance_name=None)

            # log the event
            self.logger.exception(msg)

            # reset the clock
            last_run_time = dt.datetime.now()

        # return the last run time
        return last_run_time
