import sys
import os
from json import load
import psycopg2

class PGUtils:
    def __init__(self):
        try:
            config = self.get_config()

            connection = config['connection']

            conn_str = f"host={connection['host']} port={connection['port']} dbname={connection['database']} user={connection['username']} password={connection['password']}"

            self.conn = psycopg2.connect(conn_str)

            self.cursor = self.conn.cursor()
        except:
            e = sys.exc_info()[0]

    def __del__(self):
        # close up the DB
        try:
            self.cursor.close()
            self.conn.close()
        except:
            e = sys.exc_info()[0]

    @staticmethod
    def get_config() -> dict:
        """
        gets the connection configuration

        :return: Dict, configuration params
        """

        # get the config file path/name
        config_name = os.path.join(os.path.dirname(__file__), '..', 'db_config.json')

        # open the config file
        with open(config_name, 'r') as json_file:
            # load the config items into a dict
            data: dict = load(json_file)

        # return the config data
        return data

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

