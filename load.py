'''Uploads data to the database'''
import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from dotenv import load_dotenv


def write_cancel_codes(conn: connection):
    pass


def get_cancel_code_csv_data() -> pd.DataFrame:
    pass


def get_connection(host: str, db_name: str, password: str, user: str):
    '''Connects to the database'''
    conn = psycopg2.connect(host=host,
                            dbname=db_name,
                            password=password,
                            user=user,
                            cursor_factory=RealDictCursor)
    return conn


if __name__ == "__main__":
    load_dotenv()
    conn = get_connection(os.environ["DB_HOST"], os.environ["DB_NAME"],
                          os.environ["DB_PASS"], os.environ["DB_USER"])
