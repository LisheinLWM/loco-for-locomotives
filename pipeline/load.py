'''Uploads data to the database'''

import os

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
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
        execute_values(cur, """INSERT INTO cancel_code (code, reason, abbreviation)
            VALUES %s ON CONFLICT DO NOTHING;""", records)
    conn.commit()


def insert_company_data(conn: connection, data: pd.DataFrame) -> None:
    """Inserts company data to the database"""

    company_names = data[['company_name']].values.tolist()

    with conn.cursor() as cur:
        execute_values(cur, """INSERT INTO company (company_name) VALUES
                        %s ON CONFLICT DO NOTHING;""", company_names)
    conn.commit()


def insert_station_data(conn: connection, data: pd.DataFrame) -> None:
    """Inserts station data to the database"""

    col_sets = [['origin_crs', 'origin_stn_name'],
                ['planned_final_crs', 'planned_final_destination'],
                ['destination_reached_crs', 'destination_reached_name'],
                ['cancellation_station_crs', 'cancellation_station_name']]

    dfs = [data[cs].to_numpy() for cs in col_sets]

    stations = set([tuple(x) for x in np.concatenate(
        dfs).tolist() if not pd.isna(x[0])])

    with conn.cursor() as cur:
        execute_values(cur, """INSERT INTO station (crs, station_name) VALUES
                        %s ON CONFLICT DO NOTHING;""", stations)
    conn.commit()


def insert_service_details_data(conn: connection, data: pd.DataFrame) -> None:
    """Inserts each service into the service details table with the corresponding
    foreign key IDs"""

    details = data[["service_uid", "company_name", "service_type", "origin_crs",
                    "planned_final_crs", "origin_run_datetime"]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO service_details (service_uid, company_id, service_type_id,
                       origin_station_id, destination_station_id, run_date) VALUES (%s, (SELECT company_id
                    FROM company WHERE company_name = %s), (SELECT service_type_id FROM service_type
                       WHERE service_type_name = %s), (SELECT station_id FROM station WHERE crs = %s),
                       (SELECT station_id FROM station WHERE crs = %s), %s) ON CONFLICT DO NOTHING;""", details)
    conn.commit()


def insert_delay_details(conn: connection, data: pd.DataFrame) -> None:
    """Inserts all services where the arrival lateness is great that 0 into the delay details table"""

    details = data[["service_uid", "arrival_lateness",
                    "scheduled_arrival_datetime"]]
    delays = details[data["arrival_lateness"] > 0].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO delay_details (service_details_id, arrival_lateness, scheduled_arrival)
                        VALUES ((SELECT service_details_id FROM service_details WHERE service_uid = %s),
                        %s, %s) ON CONFLICT DO NOTHING;""", delays)
    conn.commit()


def insert_cancellations(conn: connection, data: pd.DataFrame) -> None:

    details = data[["service_uid", "cancellation_station_crs",
                   "destination_reached_crs", "cancel_code"]]
    cancellations = details[data["cancel_code"].notna()].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO cancellation (service_details_id, cancelled_station_id, reached_station_id, cancel_code_id)
                        VALUES ((SELECT service_details_id FROM service_details WHERE service_uid = %s), (SELECT station_id FROM station WHERE crs = %s),
                        (SELECT station_id FROM station WHERE crs = %s), (SELECT cancel_code_id FROM cancel_code WHERE code = %s))
                        ON CONFLICT DO NOTHING""", cancellations)
    conn.commit()


if __name__ == "__main__":

    load_dotenv()
    conn = get_connection(os.environ["DB_HOST"], os.environ["DB_NAME"],
                          os.environ["DB_PASS"], os.environ["DB_USER"])

    switch_between_schemas("previous_day_data")

    data = pd.read_csv("data/transformed_service_data.csv")
    insert_company_data(conn, data)
    insert_station_data(conn, data)
    insert_service_details_data(conn, data)
    insert_delay_details(conn, data)
    insert_cancellations(conn, data)

    switch_between_schemas("all_data")
    insert_company_data(conn, data)
    insert_station_data(conn, data)
    insert_service_details_data(conn, data)
    insert_delay_details(conn, data)
    insert_cancellations(conn, data)

    os.remove("data/transformed_service_data.csv")
