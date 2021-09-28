from lib.etl.utils import get_files_name_list, get_postgres_conn
import os


class TablesGenerator:
    CSV_DIR = "/home/joaoh/joaoproject/challenge/csv"
    EMPTY = '""'

    def __init__(self):
        pass

    def execute(self):
        csv_file_names = get_files_name_list(files_dir=self.CSV_DIR)
        self._generate_tables(csv_file_names)

    def _generate_tables(self, csv_file_names):
        headers_list = self._get_headers(csv_file_names)
        sql_script = str()
        for file, header in zip(csv_file_names, headers_list):
            sql_script += self._create_table_sql_template(file, header)
        self._run_script(sql_script)

    @staticmethod
    def _get_headers(csv_file_names):
        return list(
            map(lambda csv_file: os.popen(f"head -1 {csv_file}").read(), csv_file_names)
        )

    @staticmethod
    def _create_table_sql_template(file, header):
        header_list = [
            '"' + str(value.replace('"', "").replace("\n", "")) + '"'
            for value in header.split(",")
        ]
        return f"""
            create table {file.split("/")[-1].split(".csv")[0].lower()}(
                {" TEXT DEFAULT NULL, ".join(header_list)} TEXT DEFAULT NULL);
            """

    @staticmethod
    def _run_script(sql_script):
        with get_postgres_conn() as conn, conn.cursor() as cur:
            cur.execute(sql_script)
            conn.commit()
