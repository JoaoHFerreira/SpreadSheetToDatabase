import os
import psycopg2


def get_files_name_list(files_dir):
    return list(map(lambda x: f"{files_dir}/{x}", os.listdir(files_dir)))


def get_postgres_conn():
    return psycopg2.connect(
        "dbname=postgres user=postgres password=postgres host=localhost"
    )
