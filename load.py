'''Uploads data to the database'''
import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from dotenv import load_dotenv

CODES_CSV = "cancel_codes.csv"


def write_cancel_codes(conn: connection, codes_df: pd.DataFrame):
    '''writes the cancel codes from the dataframe to the database'''
    records = codes_df.to_records(index=False)
    with conn.cursor() as cur:
        sql_query = '''
            INSERT INTO cancel_code (code, reason, abbreviation)
            VALUES (%s, %s, %s)'''
        cur.executemany(sql_query, records)

    conn.commit()


def get_cancel_code_csv_data() -> pd.DataFrame:
    '''extracts the cancel codes from the csv to a dataframe'''
    cancel_codes_df = pd.read_csv(CODES_CSV)
    return cancel_codes_df


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
    cancel_codes_df = get_cancel_code_csv_data()
    write_cancel_codes(conn, cancel_codes_df)
