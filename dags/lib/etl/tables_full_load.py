from lib.etl.utils import get_postgres_conn
import os


class TablesFullLoad:
    CSV_DIR = "/home/joaoh/joaoproject/challenge/csv"

    def __init__(self):
        pass

    def execute(self):
        self._load_csv_tables()

    def _load_csv_tables(self):
        file_path_list = self._get_file_path_list()
        with get_postgres_conn() as conn, conn.cursor() as cur:
            for file_path in file_path_list:
                self._process_file(cur, file_path)
                conn.commit()

    def _get_file_path_list(self):
        return [f"{self.CSV_DIR}/{path}" for path in os.listdir(self.CSV_DIR)]

    @staticmethod
    def _process_file(cur, file_path):
        table_name = file_path.split("/")[-1].split(".csv")[0].lower()
        with open(file_path, "r") as file:
            cur.copy_from(file, table_name, sep=",")
