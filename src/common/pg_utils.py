# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class for database functionalities

    Author: Phil Owen, RENCI.org
"""

import os
import time
import psycopg2
from src.common.logger import LoggingUtil


class PGUtils:
    """
        methods for database functionalities
    """

    def __init__(self):
        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.Supervisor.pg_utils", line_format='medium')

        # get configuration params from the pods secrets
        username = os.environ.get('ASGS_DB_USERNAME')
        password = os.environ.get('ASGS_DB_PASSWORD')
        host = os.environ.get('ASGS_DB_HOST')
        database = os.environ.get('ASGS_DB_DATABASE')
        port = os.environ.get('ASGS_DB_PORT')

        # create a connection string
        self.conn_str = f"host={host} port={port} dbname={database} user={username} password={password}"

        # init the DB connection objects
        self.conn = None
        self.cursor = None

        # get a db connection and cursor
        self.get_db_connection()

    def __del__(self):
        """
        close up the DB

        :return:
        """
        try:
            # in there is a cursor, delete it
            if self.cursor is not None:
                self.cursor.close()

            # if there is a connection, close it
            if self.conn is not None:
                self.conn.close()
        except Exception:
            self.logger.exception('Error detected closing cursor or connection.')

    def get_db_connection(self):
        """
        Gets a connection to the DB. performs a check to continue trying until
        a connection is made

        :return:
        """
        # init the connection status indicator
        good_conn = False

        # until forever
        while not good_conn:
            # check the DB connection
            good_conn = self.check_db_connection()

            try:
                # do we have a good connection
                if not good_conn:
                    # connect to the DB
                    self.conn = psycopg2.connect(self.conn_str)

                    # insure records are updated immediately
                    self.conn.autocommit = True

                    # create the connection cursor
                    self.cursor = self.conn.cursor()

                    # check the DB connection
                    good_conn = self.check_db_connection()

                    # is the connection ok now?
                    if good_conn:
                        self.logger.info('DB Connection Established.')
                        # ok to continue
                        return
                else:
                    # ok to continue
                    return
            except Exception:
                good_conn = False

            self.logger.error('DB Connection failed. Retrying...')
            time.sleep(5)

    def check_db_connection(self) -> bool:
        """
        checks to see if there is a good connection to the DB

        :return: boolean
        """
        # init the return value
        ret_val = None

        try:
            # is there a connection
            if not self.conn or not self.cursor:
                ret_val = False
            else:
                # get the DB version
                self.cursor.execute("SELECT version()")

                # get the value
                db_version = self.cursor.fetchone()

                # did we get a value
                if db_version:
                    # update the return flag
                    ret_val = True

        except (Exception, psycopg2.DatabaseError):
            # connect failed
            ret_val = False

        # return to the caller
        return ret_val

    def exec_sql(self, sql_stmt):
        """
        executes a sql statement

        :param sql_stmt:
        :return:
        """
        # init the return
        ret_val = None

        # insure we have a valid DB connection
        self.get_db_connection()

        try:
            # execute the sql
            self.cursor.execute(sql_stmt)

            # get the returned value
            ret_val = self.cursor.fetchone()

            # trap the return
            if ret_val is None or ret_val[0] is None:
                # specify a return code on an empty result
                ret_val = -1
            else:
                # get the one and only record of json
                ret_val = ret_val[0]

        except Exception:
            self.logger.exception("Error detected executing SQL: %s.", sql_stmt)

            # set the error code
            ret_val = -1

        # return to the caller
        return ret_val

    def get_job_defs(self):
        """
        gets the supervisor job definitions

        :return:
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_job_defs_json()'

        # get the data
        return self.exec_sql(sql)

    def get_new_runs(self):
        """
        gets the DB records for new runs

        :return: a json record of newly requested runs
        """

        # create the sql
        sql: str = 'SELECT public.get_supervisor_config_items_json()'

        # get the data
        ret_val = self.exec_sql(sql)

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

        # split the run id. run id is in the form <instance id>_<url>
        run = run_id.split('-')

        # create the sql. ensure the value does not exceed the column size (1024)
        sql = f"SELECT public.set_config_item({int(run[0])}, '{run[1]}-{run[2]}', 'supervisor_job_status', '{value[:1024]}')"

        # run the SQL
        self.exec_sql(sql)
