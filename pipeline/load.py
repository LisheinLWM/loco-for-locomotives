'''Uploads data to the database'''

import os

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection
from dotenv import load_dotenv


CODES_CSV = "cancel_codes.csv"


def get_connection(host: str, db_name: str, password: str, user: str):
    """Connects to the database"""

    try:
        conn = psycopg2.connect(host=host,
                                dbname=db_name,
                                password=password,
                                user=user,
                                cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Error {e} occured!")


def switch_between_schemas(schema_name: str) -> None:
    """Switches to the schema by the schema name provided"""

    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s", (schema_name,))
    conn.commit()


def write_cancel_codes(conn: connection, codes_df: pd.DataFrame):
    """writes the cancel codes from the dataframe to the database"""

    records = codes_df.to_records(index=False)
    with conn.cursor() as cur:
        sql_query = '''
            INSERT INTO cancel_code (code, reason, abbreviation)
            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;'''
        cur.executemany(sql_query, records)

    conn.commit()


def insert_company_data(conn: connection, data: pd.DataFrame) -> None:
    """Inserts company data to the database"""

    company_names = data[['company_name']].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO company (company_name) VALUES
                        (%s) ON CONFLICT DO NOTHING;""", company_names)
    conn.commit()


def insert_station_data(conn: connection, data: pd.DataFrame) -> None:
    """Inserts station data to the database"""

    crs_data = data[['origin_crs', 'planned_final_crs',
                    'destination_reached_crs', 'cancellation_station_crs']].values
    codes = [code for code in crs_data.flatten() if not pd.isna(code)]
    name_data = data[['origin_stn_name', 'planned_final_destination',
                      'destination_reached_name', 'cancellation_station_name']].values
    names = [name for name in name_data.flatten() if not pd.isna(name)]
    data_to_insert = list(zip(codes, names))

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO station (crs, station_name) VALUES
                        (%s, %s) ON CONFLICT DO NOTHING;""", data_to_insert)
    conn.commit()


if __name__ == "__main__":

    load_dotenv()
    conn = get_connection(os.environ["DB_HOST"], os.environ["DB_NAME"],
                          os.environ["DB_PASS"], os.environ["DB_USER"])

    switch_between_schemas("previous_day_data")
    # cancel_codes_df = pd.read_csv(CODES_CSV)
    # write_cancel_codes(conn, cancel_codes_df)

    data = pd.read_csv("transformed_service_data.csv")
    # insert_company_data(conn, data)
    insert_station_data(conn, data)
