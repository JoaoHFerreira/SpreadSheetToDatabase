import os
from lib.etl.utils import get_postgres_conn


class ClearData:
    CSV_DIR = "/home/joaoh/joaoproject/challenge/csv"

    def __init__(self):
        pass

    def execute(self):
        self._clear_files(self.CSV_DIR)
        self._clear_database()

    @staticmethod
    def _clear_files(csv_dir):
        for value in os.listdir(csv_dir):
            os.remove(f"{csv_dir}/{value}")

    def _clear_database(self):
        with get_postgres_conn() as conn, conn.cursor() as cur:
            tables_to_delete_list = self._get_tables_to_delete(cur)
            for table_to_delete in tables_to_delete_list:
                cur.execute(self._drop_table_sql_template(table_to_delete))
                conn.commit()

    def _get_tables_to_delete(self, cur):
        cur.execute(self._tables_to_delete_sql_template())
        return [value[0] for value in cur.fetchall()]

    @staticmethod
    def _tables_to_delete_sql_template():
        return """
            SELECT 
                tablename 
            FROM pg_catalog.pg_tables
            where schemaname = 'public'
        """

    @staticmethod
    def _drop_table_sql_template(table_name):
        return f"DROP TABLE {table_name};"
