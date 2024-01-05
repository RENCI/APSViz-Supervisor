# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class for database functionalities

    Author: Phil Owen, RENCI.org
"""
from src.common.pg_utils_multi import PGUtilsMultiConnect
from src.common.logger import LoggingUtil


class PGImplementation(PGUtilsMultiConnect):
    """
        Class that contains DB calls for the job supervisor.

        Note this class inherits from the PGUtilsMultiConnect class
        which has all the connection and cursor handling.
    """

    def __init__(self, db_names: tuple, _logger=None, _auto_commit=True):
        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("iRODS.Supervisor.Jobs.PGImplementation", level=log_level, line_format='medium',
                                                   log_file_path=log_path)

        # init the base class
        PGUtilsMultiConnect.__init__(self, 'iRODS.Supervisor.Jobs.PGImplementation', db_names, _logger=self.logger, _auto_commit=_auto_commit)

    def __del__(self):
        """
        Calls super base class to clean up DB connections and cursors.

        :return:
        """
        # clean up connections and cursors
        PGUtilsMultiConnect.__del__(self)

    def get_job_defs(self):
        """
        gets the supervisor job definitions

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_job_defs_json()'

        # get the data
        ret_val = self.exec_sql('irods-sv', sql)

        # return the data
        return ret_val

    def get_new_runs(self):
        """
        gets the DB records for new runs

        :return: a json record of newly requested runs
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_request_items_json()'

        # get the data
        ret_val = self.exec_sql('irods-sv', sql)

        # if there were no runs return None
        if ret_val == -1:
            ret_val = None

        # return to the caller
        return ret_val

    def update_job_status(self, run_id, value):
        """
        updates the job status

        :param run_id:
        :param value:
        :return: nothing
        """

        # create the sql. ensure the value does not exceed the column size (1024)
        sql = f"SELECT public.set_request_item({run_id}, '{value[:1024]}')"

        # run the SQL
        ret_val = self.exec_sql('irods-sv', sql)

        # if there were no errors, commit the updates
        if ret_val > -1:
            self.commit('irods-sv')

    def get_first_job(self, workflow_type: str):
        """
        gets the supervisor job order

        :return:
        """
        # create the sql
        sql: str = f"SELECT public.get_supervisor_job_order('{workflow_type}')"

        # get the order of jobs for this workflow type
        jobs_in_order = self.exec_sql('irods-sv', sql)

        # if we got a list get the first one
        if isinstance(jobs_in_order, list):
            ret_val = jobs_in_order[0]['job_name']
        else:
            ret_val = None

        # return the first item in the ordered list
        return ret_val
