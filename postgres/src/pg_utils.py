import sys
import os
import psycopg2
import logging
from common.logging import LoggingUtil


class PGUtils:
    def __init__(self):
        # get the log level and directory from the environment.
        # level comes from the container dockerfile, path comes from the k8s secrets
        log_level: int = int(os.getenv('LOG_LEVEL', logging.INFO))
        log_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the dir if it does not exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        # create a logger
        self.logger = LoggingUtil.init_logging("APSVIZ.pg_utils", level=log_level, line_format='medium', log_file_path=log_path)

        # get configuration params from the pods secrets
        username = os.environ.get('ASGS_DB_USERNAME')
        password = os.environ.get('ASGS_DB_PASSWORD')
        host = os.environ.get('ASGS_DB_HOST')
        database = os.environ.get('ASGS_DB_DATABASE')
        port = os.environ.get('ASGS_DB_PORT')

        # create a connection string
        conn_str = f"host={host} port={port} dbname={database} user={username} password={password}"

        # connect to the DB
        self.conn = psycopg2.connect(conn_str)

        # insure records are updated immediately
        self.conn.autocommit = True

        # create the connection cursor
        self.cursor = self.conn.cursor()

    def __del__(self):
        """
        close up the DB

        :return:
        """
        try:
            if self.cursor is not None:
                self.cursor.close()

            if self.conn is not None:
                self.conn.close()
        except Exception as e:
            self.logger.error(f'Error detected closing cursor or connection. {e}')
            sys.exc_info()[0]

    def exec_sql(self, sql_stmt):
        """
        executes a sql statement

        :param sql_stmt:
        :return:
        """
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

            # return to the caller
            return ret_val
        except Exception as e:
            self.logger.error(f'Error detected executing SQL: {sql_stmt}. {e}')
            sys.exc_info()[0]
            return

    def get_new_runs(self):
        """
        gets the DB records for new runs

        :return: a json record of newly requested runs
        """
        # create the sql
        sql: str = 'SELECT public.get_supervisor_config_items_json()'

        # get the data
        return self.exec_sql(sql)

    def update_job_status(self, run_id, value):
        """
        updates the job status

        :param instance_id:
        :param value:
        :return: nothing
        """
        # split the run id. run id is in the form <instance id>_<url>
        run = run_id.split('-')

        # create the sql
        sql = f"SELECT public.set_config_item({int(run[0])}, '{run[1]}-{run[2]}', 'supervisor_job_status', '{value}')"

        # run the SQL
        self.exec_sql(sql)
