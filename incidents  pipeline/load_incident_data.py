"""Load file: loads incident data into the database"""

import os

from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor, execute_values

from datetime import datetime, timedelta

import pandas as pd
from pandas import DataFrame


def get_connection(host: str, db_name: str, password: str, user: str):
    """
    Connects to the database
    """
    try:
        conn = psycopg2.connect(host=host,
                                dbname=db_name,
                                password=password,
                                user=user,
                                cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Error {e} occured!")


def switch_between_schemas(conn, schema_name: str) -> None:
    """
    Switches to the schema by the 
    schema name provided
    """
    with conn.cursor() as cur:
        cur.execute("SET search_path TO %s", (schema_name,))
    conn.commit()


def get_operator_info_df():
    """
    Creates a DataFrame of National Rail
    operators, with columns that include
    passenger satisfaction and operator
    code. Some operators are manually inserted
    into the DataFrame, to account for issues 
    with the Wikipedia table
    """
    df = pd.read_html("https://en.wikipedia.org/wiki/List_of_companies_operating_trains_in_the_United_Kingdom",
                      flavor="bs4", attrs={"class": "wikitable"})[0]
    df["Passenger satisfaction[1]"] = df["Passenger satisfaction[1]"].apply(
        lambda x: str(x).strip("%"))
    df["Passenger satisfaction[1]"] = pd.to_numeric(
        df["Passenger satisfaction[1]"], errors="coerce")
    df = df.rename(
        columns={"Passenger satisfaction[1]": "Passenger satisfaction (%)"})
    df = df.drop([9, 21], axis=0)
    additional_operators_df = pd.DataFrame({
        "Operator": ["Gatwick Express", "Great Northern", "Southern", "Thameslink",
                     "South Western Railway", "Island Line"],
        "Code": ["GX", "GN", "SN", "TL", "SW", "IL"],
        "Passenger satisfaction (%)": [80, 80, 80, 80, 75, 75]
    })
    df = pd.concat([df, additional_operators_df], ignore_index=True)
    print(df[["Code", "Passenger satisfaction (%)"]])
    return df


def seed_operator_table(conn: connection, operator_info: DataFrame):
    """
    Adds data to the 'operator' table
    """
    operator_info_values = operator_info[[
        'Operator', 'Code', 'Passenger satisfaction (%)']].values.tolist()

    with conn.cursor() as cur:

        cur.executemany("""INSERT INTO operator 
                    (operator_name,
                    operator_code,
                    customer_satisfaction)
                    VALUES
                    (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """, operator_info_values)

    conn.commit()


def load_priority(conn: connection, msg_df: DataFrame):
    """
    Load data into the priority table 
    from the incoming message
    """
    priority = msg_df[['incident_priority']].values.tolist()

    print(f"priorit {priority}")
    with conn.cursor() as cur:
        execute_values(cur, """INSERT INTO priority (priority_code) VALUES %s
                       ON CONFLICT DO NOTHING;""", priority)
    conn.commit()


def load_incident(conn: connection, msg_df: DataFrame):
    """
    Loads incident data into the incident table
    """
    data = msg_df[["incident_number", "version",
                   "info_link", "summary", "incident_priority", "planned", "creation_time",
                   "start_time", "end_time"]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO incident (incident_num, incident_version, link, summary,
                       priority_id, is_planned, creation_time, start_time, end_time) VALUES (%s,%s,%s,
                        %s,(SELECT priority_id FROM priority WHERE priority_code = %s), %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""", data)
    conn.commit()


def load_routes(conn: connection, msg_df: DataFrame):
    """
    Loads route data into the routes table
    """
    routes = msg_df[["route_affected"]].values.tolist()

    with conn.cursor() as cur:
        execute_values(
            cur, """INSERT INTO route_affected (route_name) VALUES %s ON CONFLICT DO NOTHING""", routes)
    conn.commit()


def load_route_link(conn: connection, msg_df: DataFrame):
    """
    Creates links between incidents and
    routes and loads them into the
    incident_route_link table
    """
    data = msg_df[["route_affected", "version"]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO incident_route_link (route_id, incident_id) VALUES
                        ((SELECT route_id FROM route_affected WHERE route_name = %s),
                        (SELECT incident_id FROM incident WHERE incident_version = %s));""", data)
    conn.commit()


def load_operator_link(conn: connection, msg_df: DataFrame):
    """
    Creates links between incidents and
    operators and loads them into the
    incident_operator_link table
    """
    data = msg_df[["affected_operator_ref", "version"]].values.tolist()

    with conn.cursor() as cur:
        cur.executemany("""INSERT INTO incident_operator_link (operator_id, incident_id) VALUES
                        ((SELECT operator_id FROM operator WHERE operator_code = %s),
                        (SELECT incident_id FROM incident WHERE incident_version = %s));""", data)
    conn.commit()


def load_all_incidents(msg):
    """
    Calls all of the load functions
    """
    load_dotenv()
    conn = get_connection(os.environ["DB_HOST"], os.environ["DB_NAME"],
                          os.environ["DB_PASS"], os.environ["DB_USER"])
    switch_between_schemas(conn, "incident_data")
    load_priority(conn, msg)
    load_incident(conn, msg)
    load_routes(conn, msg)
    load_route_link(conn, msg)
    load_operator_link(conn, msg)
