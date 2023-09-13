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

import altair as alt
from altair.vegalite.v5.api import Chart

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
    """Establishes a connection with the PostgreSQL database."""
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
    """Retrieve 24hr data from the database and return as a DataFrame."""
    yesterday = datetime.now() - timedelta(days=1
                                           )
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
    """Creates a header for the dashboard and title on tab."""

    st.image("logo.png", width=350)

    st.markdown("An app for analysing your train services.")

    st.markdown("---")


def first_row_display(data_df: pd.DataFrame) -> None:
    """Controls how the first row figures are displayed for the overall data."""
    cols = st.columns(3)
    st.markdown(
        """
        <style>
            [data-testid="stMetricValue"] {
            font-size: 25px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with cols[0]:
        st.metric("TOTAL SERVICES:", len(data_df))

    with cols[1]:
        total_delays = (data_df['arrival_lateness'] > 0).sum()
        total_records = len(data_df)  # Total number of records
        percentage_delays = (total_delays / total_records) * 100
        st.metric("TOTAL DELAYS:",
                  f"{total_delays} ({percentage_delays:.2f}%)")

    with cols[2]:
        total_cancellations = data_df['cancellation_id'].count()
        percentage_cancellations = (total_cancellations / total_records) * 100
        st.metric("TOTAL CANCELLATIONS:", f"{total_cancellations}\
                  ({percentage_cancellations:.2f}%)")


def second_row_display(data_df: pd.DataFrame) -> None:
    """Controls how the second row figures are displayed for the overall data."""

    most_cancelled_station = data_df['origin_station_name'].value_counts(
    ).idxmax()

    cols = st.columns(3)
    st.markdown(
        """
        <style>
            [data-testid="stMetricValue"] {
            font-size: 25px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with cols[0]:
        most_cancelled_station = data_df['origin_station_name'].value_counts(
        ).idxmax()
        num_cancellations = data_df.loc[data_df['arrival_lateness'].idxmax(
        )]['origin_station_name']
        st.write("MOST DELAYED STATION:",
                 data_df.loc[data_df['arrival_lateness'].idxmax()]['origin_station_name'])

    with cols[1]:
        most_cancelled_station = data_df['origin_station_name'].value_counts(
        ).idxmax()
        num_cancellations = data_df['origin_station_name'].value_counts().max()
        st.write("MOST CANCELLED STATION:",
                 f"{most_cancelled_station} (Num of cancellations: {num_cancellations})")

    with cols[2]:
        avg_delay_minutes = round(data_df['arrival_lateness'].mean(), 2)
        st.metric("AVG DELAYS TIME FOR ALL SERVICES:",
                  f"{avg_delay_minutes} MINUTES")

    st.markdown("---")


def plot_average_delays_by_company(data_df: pd.DataFrame, selected_company) -> None:
    """Create a horizontal bar chart showing the average delays for each company."""

    max_companies = 5

    if len(selected_company) > max_companies:
        st.error(f"You have reached the maximum limit of {max_companies} companies in display.\
                  Please remove a company to add more.")
        # Truncate the list
        selected_company = selected_company[:max_companies]

    if len(selected_company) != 0:
        data_df = data_df[data_df['company_name'].isin(selected_company)]

    average_delays = data_df.groupby('company_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays = average_delays.sort_values(by='arrival_lateness',
                                                ascending=False).head(max_companies)

    chart = alt.Chart(average_delays).mark_bar(
        color="#ffc05f"
    ).encode(
        y=alt.Y('company_name:N', title='COMPANY NAME',
                sort=alt.EncodingSortField(field='arrival_lateness', order='descending')),
        x=alt.X('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)'),
        tooltip=[alt.Tooltip('company_name:N', title='COMPANY NAME'),
                 alt.Tooltip('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)')]
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelAngle=0,
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    return chart


def plot_average_delays_by_station(data_df: pd.DataFrame, selected_station) -> None:
    """Create a horizontal bar chart showing the average delays for each station."""

    max_stations = 5

    if len(selected_station) > max_stations:
        st.error(f"You have reached the maximum limit of {max_stations} stations in display.\
                  Please remove a station to add more.")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    average_delays = data_df.groupby('origin_station_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays = average_delays.sort_values(by='arrival_lateness',
                                                ascending=False).head(max_stations)

    chart = alt.Chart(average_delays).mark_bar(
        color="#ffc05f"
    ).encode(
        y=alt.Y('origin_station_name:N', title='STATION NAME',
                sort=alt.EncodingSortField(field='arrival_lateness', order='descending')),
        x=alt.X('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)'),
        tooltip=[alt.Tooltip('origin_station_name:N', title='STATION NAME'),
                 alt.Tooltip('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)')]
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelAngle=0,
        labelColor="#1f5475",
        titleColor="#1f5475"
    )
    return chart


def plot_cancellations_per_station(data_df: pd.DataFrame, selected_station) -> None:
    """Create a line graph showing the number of cancellations per station."""

    st.title("Number of Cancellations by Station")

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(f"""You have reached the maximum limit of {max_stations} 
                 stations in display. Please remove a station to add more.""")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    cancellations_per_station = data_df[data_df['cancellation_id'].notnull()].groupby(
        'origin_station_name')['cancellation_id'].count().reset_index()
    cancellations_per_station = cancellations_per_station.rename(
        columns={'cancellation_id': 'cancellation_count'})

    # Sort the stations by cancellation count in descending order and select the top 20 stations
    cancellations_per_station = cancellations_per_station.sort_values(
        by='cancellation_count', ascending=False).head(20)

    chart = alt.Chart(cancellations_per_station).mark_line(
        color="#ffc05f"
    ).encode(
        x=alt.X('origin_station_name:N', title='STATION NAME'),
        y=alt.Y('cancellation_count:Q', title='NUMBER OF CANCELLATIONS'),
        tooltip=[alt.Tooltip('origin_station_name:N', title='STATION NAME'),
                 alt.Tooltip('cancellation_count:Q', title='NUMBER OF CANCELLATIONS')]
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelAngle=45,
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)


def plot_bus_replacements_per_station(data_df: pd.DataFrame, selected_station) -> None:
    """Create an Altair plot showing the number of bus replacements per station."""

    st.title("Number of Bus Replacements per Station")

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(f"""You have reached the maximum limit of {max_stations} 
                 stations in display. Please remove a station to add more.""")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    bus_replacements_per_station = data_df[data_df["service_type_name"] == "bus"].groupby(
        'origin_station_name').size().reset_index(name='bus_replacement_count')
    bus_replacements_per_station = bus_replacements_per_station.rename(
        columns={"service_type_name": 'bus_replacement_count'})

    # Sort the stations by bus replacement count in descending order and select the top 20 stations.
    bus_replacements_per_station = bus_replacements_per_station.sort_values(
        by='bus_replacement_count', ascending=False).head(20)

    chart = alt.Chart(bus_replacements_per_station).mark_bar(
        color="#ffc05f"
    ).encode(
        x=alt.X('origin_station_name:N', title='STATION NAME', sort=alt.EncodingSortField(
            field='bus_replacement_count', order='descending')),
        y=alt.Y('bus_replacement_count:Q', title='NUMBER OF BUS REPLACEMENTS'),
        tooltip=[alt.Tooltip('origin_station_name:N', title='Station Name'), alt.Tooltip(
            'bus_replacement_count:Q', title='NUMBER OF BUS REPLACEMENTS')]
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelAngle=45,
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)


def plot_percentage_of_services_reaching_final_destination(data_df: pd.DataFrame, selected_station) -> None:
    """Create a pie chart showing the percentage of services that reach their planned final destination per station."""

    st.title("Percentage of Services Reaching Final Destination by Station")

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(
            f"You have reached the maximum limit of {max_stations} stations in display. Please remove a station to add more.")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    # Calculate the percentage of services reaching their planned final destination for each station
    services_reaching_final_destination = data_df[data_df['destination_station_id']
                                                  == data_df['reached_station_id']]
    station_summary = services_reaching_final_destination.groupby(
        'origin_station_name').size().reset_index(name='reached_destination_count')
    total_services = data_df.groupby(
        'origin_station_name').size().reset_index(name='total_services')
    station_summary = station_summary.merge(
        total_services, on='origin_station_name', how='outer')
    station_summary['percentage_reached_destination'] = (
        station_summary['reached_destination_count'] / station_summary['total_services']) * 100
    station_summary = station_summary.sort_values(
        by='percentage_reached_destination', ascending=False).head(20)

    chart = alt.Chart(station_summary).mark_arc().encode(
        color=alt.Color('origin_station_name:N',
                        scale=alt.Scale(scheme='category20c')),
        tooltip=[
            alt.Tooltip('origin_station_name:N', title='STATION NAME'),
            alt.Tooltip('percentage_reached_destination:Q',
                        title='PERCENTAGE', format='.2f')
        ],
        theta=alt.Theta('percentage_reached_destination:Q', title=None),
        text='origin_station_name:N'  # Display station names as labels
    ).properties(
        width=600,
        height=400
    ).configure_text(
        color="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)


def sidebar_header(text, color='white') -> None:
    """Add text to the dashboard side bar with a colored header"""
    with st.sidebar:
        st.markdown(
            f"""
            <h2 style='color: {color}; padding: 10px 0; margin: 0;'>{text}</h2>
            <hr style='margin: 0;'>
            """, unsafe_allow_html=True
        )


if __name__ == "__main__":
    st.set_page_config(
        page_title="Train Services Monitoring Dashboard", layout="wide")

    load_dotenv()
    connection = get_db_connection()
    database_df = get_live_database(connection)

    dashboard_header()
    sidebar_header("Filter Options")

    # Set colored headers for Station and Company sections
    sidebar_header("Station:")
    select_station = st.sidebar.multiselect(
        ".", options=database_df["origin_station_name"].unique())

    sidebar_header("Company:")
    select_companies = st.sidebar.multiselect(
        ":", options=database_df["company_name"].unique())

    first_row_display(database_df)
    second_row_display(database_df)
    col1, col2 = st.columns([5, 6])
    chart_company = plot_average_delays_by_company(
        database_df, select_companies)
    col1.altair_chart(chart_company, use_container_width=True)
    chart_station = plot_average_delays_by_station(database_df, select_station)
    col2.altair_chart(chart_station, use_container_width=True)
    plot_cancellations_per_station(database_df, select_station)
    plot_bus_replacements_per_station(database_df, select_station)
    plot_percentage_of_services_reaching_final_destination(
        database_df, select_station)