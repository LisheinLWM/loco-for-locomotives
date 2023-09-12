"""
Streamlit dashboard application code.

Module contains code for connecting to postgres database (RDS)
and using that data to create charts for data analysis.
"""
from datetime import datetime, timedelta
import sys
from os import environ
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from psycopg2 import connect, Error
from psycopg2.extensions import connection
from dotenv import load_dotenv

CSV_COLUMNS = [
    "cancel_code_id",
    "cancel_code",
    "cancel_reason",
    "cancel_abbreviation",
    "company_id",
    "company_name",
    "origin_station_id",
    "origin_station_name",
    "destination_station_id",
    "destination_station_name",
    "service_type_id",
    "service_type_name",
    "service_details_id",
    "service_uid",
    "run_date",
    "delay_details_id",
    "arrival_lateness",
    "scheduled_arrival",
    "cancellation_id",
    "cancelled_station_id",
    "reached_station_id"
]


def get_db_connection():
    """
    Establishes a connection with the PostgreSQL database.
    """
    try:
        conn = connect(
            user=environ.get("DATABASE_USERNAME"),
            password=environ.get("DATABASE_PASSWORD"),
            host=environ.get("DATABASE_IP"),
            port=environ.get("DATABASE_PORT"),
            database=environ.get("DATABASE_NAME"),)
        print("Database connection established successfully.")
        return conn
    except Error as err:
        print("Error connecting to database: ", err)
        sys.exit()


def get_live_database(conn: connection) -> pd.DataFrame:
    """
    Retrieve 24hr data from the database and return as a DataFrame.
    """
    yesterday = datetime.datetime.now() - timedelta(days=1)
    yesterday_date = yesterday.strftime("%Y-%m-%d")

    query = """
    SET search_path TO service_data;
    SELECT 
        cc.cancel_code_id,
        cc.code AS cancel_code,
        cc.reason AS cancel_reason,
        cc.abbreviation AS cancel_abbreviation,
        c.company_id,
        c.company_name,
        s.station_id AS origin_station_id,
        s.station_name AS origin_station_name,
        s2.station_id AS destination_station_id,
        s2.station_name AS destination_station_name,
        st.service_type_id,
        st.service_type_name,
        sd.service_details_id,
        sd.service_uid,
        sd.run_date,
        dd.delay_details_id,
        dd.arrival_lateness,
        dd.scheduled_arrival,
        cn.cancellation_id,
        cn.cancelled_station_id,
        cn.reached_station_id
        FROM service_details sd
        LEFT JOIN company c ON sd.company_id = c.company_id
        LEFT JOIN service_type st ON sd.service_type_id = st.service_type_id
        LEFT JOIN station s ON sd.origin_station_id = s.station_id
        LEFT JOIN station s2 ON sd.destination_station_id = s2.station_id
        LEFT JOIN delay_details dd ON sd.service_details_id = dd.service_details_id
        LEFT JOIN cancellation cn ON sd.service_details_id = cn.service_details_id
        LEFT JOIN cancel_code cc ON cn.cancel_code_id = cc.cancel_code_id
        WHERE DATE(sd.run_date) = %s;
        """

    with conn.cursor() as cur:
        cur.execute(query, (yesterday_date,))
        data = cur.fetchall()

    data_df = pd.DataFrame(data, columns=CSV_COLUMNS)
    data_df.to_csv("database_df.csv")

    return data_df


def dashboard_header() -> None:
    """
    Creates a header for the dashboard and title on tab.
    """

    st.title("LOCO_FOR_LOCOMOTIVES")
    st.markdown("An app for analysing your train services")


def plot_average_delays_by_company(data_df: pd.DataFrame) -> None:
    """
    Create a bar chart showing the average delays for each company.
    """
    st.title("Average_delays_by_company")

    average_delays = data_df.groupby('company_name')[
        'arrival_lateness'].mean().reset_index()

    plt.figure(figsize=(15, 6))
    ax = sns.barplot(x='company_name', y='arrival_lateness',
                     data=average_delays)
    plt.xlabel('Company Name')
    plt.ylabel('Average Delay')
    plt.title('Average Delays by Company')
    plt.xticks(rotation=45)
    plt.tight_layout()  # Ensure the labels fit in the plot area

    for p in ax.patches:
        ax.annotate(f"{p.get_height():.2f} min", (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', fontsize=10, color='black', xytext=(0, 5),
                    textcoords='offset points')

    st.pyplot(plt)


def plot_average_delays_by_station(data_df: pd.DataFrame, station) -> None:
    """
    Create a bar chart showing the average delays for each station.
    """
    st.title("Average_delays_by_station (TOP 20)")

    max_stations = 20

    if len(station) > max_stations:
        st.error(f"You have reached the maximum limit of {max_stations} stations in display.\
                  Please remove a station to add more.")
        station = station[:max_stations]  # Truncate the list

    if len(station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(station)]

    # Group the data by origin_station_name and calculate the average delay
    average_delays = data_df.groupby('origin_station_name')[
        'arrival_lateness'].mean().reset_index()

    # Sort the data by average delay in descending order and select the top 20 stations
    average_delays = average_delays.sort_values(
        by='arrival_lateness', ascending=False).head(20)

    plt.figure(figsize=(15, 6))
    ax = sns.barplot(x='origin_station_name',
                     y='arrival_lateness', data=average_delays)
    plt.xlabel('Station Name')
    plt.ylabel('Average Delay')
    plt.title('Average Delays by Station')
    plt.xticks(rotation=45)
    plt.tight_layout()

    for p in ax.patches:
        ax.annotate(f"{p.get_height():.2f} min", (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', fontsize=10, color='black', xytext=(0, 5),
                    textcoords='offset points')
    st.pyplot(plt)


if __name__ == "__main__":
    st.set_page_config(
        page_title="Train Services Monitoring Dashboard", layout="wide")
    load_dotenv()
    connection = get_db_connection()
    database_df = get_live_database(connection)

    dashboard_header()
    selected_station = st.sidebar.multiselect(
        "Station", options=database_df["origin_station_name"].unique())

    plot_average_delays_by_station(database_df, selected_station)
    plot_average_delays_by_company(database_df)
