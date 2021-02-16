import sys
import os
import psycopg2


class PGUtils:
    def __init__(self):
        username = os.getenv('ASGS_DB_USERNAME')
        password = os.environ.get('ASGS_DB_PASSWORD')
        host = os.environ.get('ASGS_DB_HOST')
        database = os.environ.get('ASGS_DB_DATABASE')
        port = os.environ.get('ASGS_DB_PORT')

        conn_str = f"host={host} port={port} dbname={database} user={username} password={password}"

        self.conn = psycopg2.connect(conn_str)

        self.cursor = self.conn.cursor()

    def __del__(self):
        # close up the DB
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            e = sys.exc_info()[0]

    ###########################################
    # executes a sql statement, returns the first row
    ###########################################
    def exec_sql(self, sql_stmt):
        try:
            # execute the sql
            self.cursor.execute(sql_stmt)

            # get the returned value
            ret_val = self.cursor.fetchone()

            if ret_val is None or ret_val[0] is None:
                ret_val = -1
            else:
                ret_val = ret_val[0]

            return ret_val
        except:
            e = sys.exc_info()[0]
            return

