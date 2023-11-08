from typing import Any, Sequence
import pymysql.cursors

from utils.jdbc import JDBCConnectionContainer

class MySQLCommunicator(JDBCConnectionContainer):

    def __init__(self, jdbc_connection_name: str):
        super().__init__(jdbc_connection_name)

    def query(self, sql: str, params: Sequence, db_name: str) -> tuple[dict[str, Any]]:
        conn = pymysql.connect(
            host=self.jdbc_hostname,
            user=self.jdbc_user,
            password=self.jdbc_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor,
        )
        with conn:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                result = cursor.fetchall()
        return result
