from os import environ, _Environ

from dotenv import load_dotenv
from psycopg2 import connect
from psycopg2.extensions import connection

from datetime import datetime, timedelta

import pandas as pd
from pandas import DataFrame


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
                    passenger_satisfaction)
                    VALUES
                    (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """, operator_info_values)

    conn.commit()
