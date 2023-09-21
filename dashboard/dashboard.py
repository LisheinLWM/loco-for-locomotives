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
from psycopg2 import connect, Error
from psycopg2.extensions import connection
from dotenv import load_dotenv

import altair as alt

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
    """Retrieve previous day data from the database and return as a DataFrame."""
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


def dashboard_header(page: str) -> None:
    """Creates a header for the dashboard and title on tab."""

    col1, col2, col3 = st.columns([6, 1, 1])

    with col1:
        st.title(f'{page} VISUALISATION PAGE')

    with col2:
        st.title('')

    with col3:
        st.title('')

    st.write('An app for analysing your train services.')

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

    cols = st.columns(3)

    st.markdown(
        """
        <style>
            .st.metric {
                font-size: 25px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with cols[0]:
        most_delayed_station = data_df.groupby('origin_station_name')[
            'arrival_lateness'].mean().idxmax()
        avg_delay_minutes = round(data_df.groupby('origin_station_name')[
                                  'arrival_lateness'].mean().max(), 2)
        st.write("MOST DELAYED STATION:",
                 most_delayed_station)

    with cols[1]:
        company_cancel_counts = data_df.groupby('origin_station_name')[
            'cancellation_id'].count()
        most_cancelled_station = company_cancel_counts.idxmax()
        most_cancelled_station_count = company_cancel_counts.max()

        st.write("MOST CANCELLED STATION:",
                 f"{most_cancelled_station} (Num of cancellations: {most_cancelled_station_count})")

    with cols[2]:
        avg_delay_minutes = round(data_df['arrival_lateness'].mean(), 2)
        st.metric("AVG DELAYS TIME FOR ALL SERVICES:",
                  f"{avg_delay_minutes} MINUTES")

    st.markdown("---")


def plot_average_delays_by_station(data_df: pd.DataFrame, selected_station) -> None:
    """Create a horizontal bar chart showing the average delays per station."""
    st.write('<h2 style="font-size: 24px;">Average delays per station</h2>',
             unsafe_allow_html=True)

    max_stations = 20

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

    chart = alt.Chart(average_delays).mark_bar().encode(
        y=alt.Y('origin_station_name:N', title='STATION NAME',
                sort=alt.EncodingSortField(field='arrival_lateness', order='descending')),
        x=alt.X('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)'),
        tooltip=[alt.Tooltip('origin_station_name:N', title='STATION NAME'),
                 alt.Tooltip('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)')]
    ).properties(
        width=600,
        height=500
    ).configure_axis(
        labelAngle=0,
        labelColor="#1f5475",
        titleColor="#1f5475"
    )
    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def plot_cancellations_per_station(data_df: pd.DataFrame, selected_station) -> None:
    """Create a bar chart showing the number of cancellations per station."""

    st.write('<h2 style="font-size: 24px;">Number of Cancellations per Station</h2>',
             unsafe_allow_html=True)

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(
            f"You have reached the maximum limit of {max_stations} stations in display. Please remove a station to add more.")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    cancellations_per_station = data_df[data_df['cancellation_id'].notnull()].groupby(
        'origin_station_name')['cancellation_id'].count().reset_index()
    cancellations_per_station = cancellations_per_station.rename(
        columns={'cancellation_id': 'cancellation_count'})

    cancellations_per_station = cancellations_per_station.sort_values(
        by='cancellation_count', ascending=False).head(20)

    chart = alt.Chart(cancellations_per_station).mark_bar().encode(
        x=alt.X('origin_station_name:N', title='STATION NAME',
                sort='-y'),  # Sort by count in descending order
        y=alt.Y('cancellation_count:Q', title='NUMBER OF CANCELLATIONS'),
        tooltip=[alt.Tooltip('origin_station_name:N', title='STATION NAME'),
                 alt.Tooltip('cancellation_count:Q', title='NUMBER OF CANCELLATIONS')]
    ).properties(
        width=800,
        height=400
    ).configure_axis(
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def plot_bus_replacements_per_station(data_df: pd.DataFrame, selected_station) -> None:
    """Create a donut Altair chart showing the number of bus replacements per station."""

    st.write('<h2 style="font-size: 24px;">Number of bus replacements per station</h2>',
             unsafe_allow_html=True)

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

    bus_replacements_per_station = bus_replacements_per_station.sort_values(
        by='bus_replacement_count', ascending=False).head(20)

    total_bus_replacements = bus_replacements_per_station['bus_replacement_count'].sum(
    )

    chart = alt.Chart(bus_replacements_per_station).mark_arc(innerRadius=60).encode(
        theta='bus_replacement_count:Q',
        color=alt.Color('origin_station_name:N', legend=None),
        tooltip=[alt.Tooltip('origin_station_name:N', title='Station Name'), alt.Tooltip(
            'bus_replacement_count:Q', title='NUMBER OF BUS REPLACEMENTS')]
    ).transform_aggregate(
        bus_replacement_count='sum(bus_replacement_count)',
        groupby=['origin_station_name']
    ).properties(
        width=600,
        height=400
    )

    # Display the total number of bus replacements in the central hole
    central_text = alt.Chart(pd.DataFrame({'total_bus_replacements': [total_bus_replacements]})).mark_text(
        size=24, color="#1f5475", align='center', baseline='middle'
    ).encode(
        text='total_bus_replacements:Q'
    ).properties(
        width=600,
        height=400
    )

    # Layer the donut chart and the central text with configurations
    donut_chart = alt.layer(chart, central_text).configure_view(
        strokeWidth=0
    ).configure_axis(
        labelAngle=90,
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(donut_chart, use_container_width=True)
    st.markdown("---")


def plot_percentage_of_services_reaching_final_destination(data_df: pd.DataFrame, selected_station) -> None:
    """Create a stacked bar chart showing the percentage breakdown of services that reached their destination and those that didn't per station."""

    st.write('<h2 style="font-size: 24px;">Percentage of services reached vs not reached destination per Station</h2>',
             unsafe_allow_html=True)

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(
            f"You have reached the maximum limit of {max_stations} stations in display. Please remove a station to add more.")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    data_df['destination_status'] = data_df['reached_station_id'].apply(
        lambda x: 'Reached' if pd.isna(x) or x == '' else 'Not Reached')

    station_summary = data_df.groupby(
        ['origin_station_name', 'destination_status']).size().reset_index(name='count')

    # Calculate the total count per station
    station_total_count = station_summary.groupby('origin_station_name')[
        'count'].sum().reset_index()

    # Merge the counts with the total counts
    station_summary = station_summary.merge(
        station_total_count, on='origin_station_name', suffixes=('', '_total'))

    # Calculate the percentage
    station_summary['percentage'] = (
        station_summary['count'] / station_summary['count_total']) * 100

    station_summary = station_summary.sort_values(
        by='percentage', ascending=False).head(20)
    chart = alt.Chart(station_summary).mark_bar().encode(
        x=alt.X('origin_station_name:N', title='Station Name'),
        y=alt.Y('percentage:Q', title='Percentage'),
        color=alt.Color('destination_status:N', scale=alt.Scale(
            scheme='set1'), title='Destination Status'),
        tooltip=[
            alt.Tooltip('origin_station_name:N', title='Station Name'),
            alt.Tooltip('destination_status:N', title='Destination Status'),
            alt.Tooltip('percentage:Q', title='Percentage')
        ]
    ).properties(
        width=600,
        height=400
    ).configure_title(
        fontSize=16,
        color="#333"
    )

    st.altair_chart(chart, use_container_width=True)


def plot_percentage_of_services_reaching_final_destination_by_company(data_df: pd.DataFrame, selected_company: list) -> None:
    """Create a stacked bar chart showing the percentage breakdown of services that reached their destination and those that didn't per company."""

    st.write('<h2 style="font-size: 24px;">Percentage of services reached vs not reached destination per Company</h2>',
             unsafe_allow_html=True)

    max_companies = 20

    if len(selected_company) > max_companies:
        st.error(
            f"You have reached the maximum limit of {max_companies} companies in display. Please remove a company to add more.")
        # Truncate the list
        selected_company = selected_company[:max_companies]

    if len(selected_company) != 0:
        data_df = data_df[data_df['company_name'].isin(selected_company)]

    data_df['destination_status'] = data_df['reached_station_id'].apply(
        lambda x: 'Reached' if pd.isna(x) or x == '' else 'Not Reached')

    company_summary = data_df.groupby(
        ['company_name', 'destination_status']).size().reset_index(name='count')

    # Calculate the total count per company
    company_total_count = company_summary.groupby(
        'company_name')['count'].sum().reset_index()

    # Merge the counts with the total counts
    company_summary = company_summary.merge(
        company_total_count, on='company_name', suffixes=('', '_total'))

    # Calculate the percentage
    company_summary['percentage'] = (
        company_summary['count'] / company_summary['count_total']) * 100

    company_summary = company_summary.sort_values(
        by='percentage', ascending=False).head(20)

    chart = alt.Chart(company_summary).mark_bar().encode(
        x=alt.X('company_name:N', title='Company Name'),
        y=alt.Y('percentage:Q', title='Percentage'),
        color=alt.Color('destination_status:N', scale=alt.Scale(
            scheme='set1'), title='Destination Status'),
        tooltip=[
            alt.Tooltip('company_name:N', title='Company Name'),
            alt.Tooltip('destination_status:N', title='Destination Status'),
            alt.Tooltip('percentage:Q', title='Percentage')
        ]
    ).properties(
        width=600,
        height=400
    ).configure_title(
        fontSize=16,
        color="#333"
    )

    st.altair_chart(chart, use_container_width=True)


def create_average_delays_line_charts(data_df: pd.DataFrame, selected_stations: list) -> None:
    """Create line charts of average delays by hour for selected stations."""

    st.write('<h2 style="font-size: 24px;">Average Delays by Hour</h2>',
             unsafe_allow_html=True)

    for selected_station in selected_stations:
        st.subheader(f'For Station: {selected_station}')

        station_data = data_df[data_df['origin_station_name']
                               == selected_station]

        # Group data by hour and calculate the average delay for each hour
        hourly_avg_delays = station_data.groupby(station_data['scheduled_arrival'].dt.hour)[
            'arrival_lateness'].mean().reset_index()

        # Rename columns for clarity
        hourly_avg_delays.columns = ['Hour', 'Average Delay (minutes)']

        # Create the line chart for the current station
        chart = alt.Chart(hourly_avg_delays).mark_line().encode(
            x=alt.X('Hour:O', title='Hour of Day'),
            y=alt.Y('Average Delay (minutes):Q',
                    title='Average Delay (minutes)'),
            tooltip=['Hour:O', 'Average Delay (minutes):Q']
        ).properties(
            width=800,
            height=400
        ).configure_axis(
            labelColor="#1f5475",
            titleColor="#1f5475"
        )

        st.altair_chart(chart, use_container_width=True)
        st.markdown("---")


def create_average_cancellations_line_charts(data_df: pd.DataFrame, selected_stations: list) -> None:
    """Create line charts of average cancellations by hour for selected stations."""

    st.write('<h2 style="font-size: 24px;">Average Cancellations by Hour</h2>',
             unsafe_allow_html=True)

    for selected_station in selected_stations:
        st.subheader(f'For Station: {selected_station}')

        station_data = data_df[data_df['origin_station_name']
                               == selected_station]

        # Group data by hour and calculate the average cancellations for each hour
        station_data['Hour'] = station_data['scheduled_arrival'].dt.hour

        # Group data by hour and calculate the average cancellations for each hour
        hourly_avg_cancellations = station_data[station_data['cancellation_id'].notnull()].groupby(
            'Hour')['cancel_code_id'].count().reset_index()

        # Rename columns for clarity
        hourly_avg_cancellations.columns = ['Hour', 'Average Cancellations']

        # Create the line chart for the current station
        chart = alt.Chart(hourly_avg_cancellations).mark_line().encode(
            x=alt.X('Hour:O', title='Hour of Day'),
            y=alt.Y('Average Cancellations:Q', title='Average Cancellations'),
            tooltip=['Hour:O', 'Average Cancellations:Q']
        ).properties(
            width=800,
            height=400
        ).configure_axis(
            labelColor="#1f5475",
            titleColor="#1f5475"
        )

        st.altair_chart(chart, use_container_width=True)
        st.markdown("---")


def create_line_plot_arrival_lateness_vs_scheduled(data_df: pd.DataFrame, selected_station: str) -> None:
    """Create a line graph of Arrival Lateness vs. Scheduled Arrival."""

    st.write('<h2 style="font-size: 24px;">Line graph of arrival lateness vs scheduled arrival</h2>',
             unsafe_allow_html=True)

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    # Format the scheduled_arrival column as a string with date and time
    data_df['scheduled_arrival_formatted'] = data_df['scheduled_arrival'].dt.strftime(
        '%Y-%m-%d %H:%M:%S')

    # Create a tooltip field that combines origin and destination station names
    data_df['station_tooltip'] = data_df['origin_station_name'] + \
        ' to ' + data_df['destination_station_name']

    chart = alt.Chart(data_df).mark_line().encode(
        x=alt.X('scheduled_arrival:T', title='Scheduled Arrival'),
        y=alt.Y('arrival_lateness:Q', title='Arrival Lateness'),
        color=alt.Color('arrival_lateness:Q', title='Arrival Lateness',
                        scale=alt.Scale(scheme='viridis')),
        tooltip=[
            alt.Tooltip('station_tooltip:N', title='Station'),
            alt.Tooltip('scheduled_arrival_formatted:N',
                        title='Scheduled Arrival'),
            alt.Tooltip('arrival_lateness:Q', title='Arrival Lateness')
        ]
    ).properties(
        width=800,
        height=400
    ).configure_axis(
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def plot_cancellations_by_station_and_reason(data_df: pd.DataFrame, selected_station: list):
    """Create a stacked bar chart to visualize cancellations by station and reason."""

    st.write('<h2 style="font-size: 24px;">Cancellations and reasons per station</h2>',
             unsafe_allow_html=True)

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(f"You have reached the maximum limit of {max_stations} stations in display.\
                  Please remove a station to add more.")
        # Truncate the list
        selected_station = selected_station[:max_stations]

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(
            selected_station)]

    # Filter rows with cancellations
    cancellations = data_df[data_df['cancel_code'].notna()]

    # Group data by station and cancel_reason and count the occurrences
    station_reason_counts = cancellations.groupby(
        ['origin_station_name', 'cancel_code']).size().reset_index(name='frequency')

    cancel_reasons_df = data_df[['cancel_code',
                                 'cancel_reason']].drop_duplicates()
    station_reason_counts = station_reason_counts.merge(
        cancel_reasons_df, on='cancel_code', how='left')

    chart = alt.Chart(station_reason_counts).mark_bar().encode(
        x=alt.X('origin_station_name:N', title='Station Name',
                axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('frequency:Q', title='Frequency'),
        color=alt.Color('cancel_reason:N', title='Cancellation Reason',
                        scale=alt.Scale(scheme='category20')),
        tooltip=['origin_station_name:N', 'cancel_reason:N', 'frequency:Q']
    ).properties(
        width=2000,
        height=400
    ).configure_axis(
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def sidebar_header(text, color='white') -> None:
    """Add text to the dashboard side bar with a colored header"""
    with st.sidebar:
        st.markdown(
            f"""
            <h2 style='color: {color}; padding: 10px 0; margin: 0;'>{text}</h2>
            <hr style='margin: 0;'>
            """, unsafe_allow_html=True
        )


def first_row_display_company(data_df: pd.DataFrame) -> None:
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
        total_records = len(data_df)
        percentage_delays = (total_delays / total_records) * 100
        st.metric("TOTAL DELAYS:",
                  f"{total_delays} ({percentage_delays:.2f}%)")

    with cols[2]:
        total_cancellations = data_df['cancellation_id'].count()
        percentage_cancellations = (total_cancellations / total_records) * 100
        st.metric("TOTAL CANCELLATIONS:", f"{total_cancellations}\
                  ({percentage_cancellations:.2f}%)")


def second_row_display_company(data_df: pd.DataFrame) -> None:
    """Controls how the second row figures are displayed for the overall data."""

    cols = st.columns(3)

    st.markdown(
        """
        <style>
            .st.metric {
                font-size: 25px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with cols[0]:
        most_delayed_company = data_df.groupby(
            'company_name')['arrival_lateness'].mean().idxmax()
        avg_delay_minutes = round(data_df.groupby('company_name')[
                                  'arrival_lateness'].mean().max(), 2)
        st.write("MOST DELAYED COMPANY:",
                 most_delayed_company)
    with cols[1]:
        company_cancel_counts = data_df.groupby(
            'company_name')['cancellation_id'].count()
        most_cancelled_company = company_cancel_counts.idxmax()
        most_cancelled_company_count = company_cancel_counts.max()

        st.write("MOST CANCELLED COMPANY:",
                 f"{most_cancelled_company} (Num of cancellations: {most_cancelled_company_count})")

    with cols[2]:
        avg_delay_minutes = round(data_df['arrival_lateness'].mean(), 2)
        st.metric("AVG DELAYS TIME FOR ALL SERVICES:",
                  f"{avg_delay_minutes} MINUTES")

    st.markdown("---")


def plot_most_average_delays_by_company(data_df: pd.DataFrame, selected_company: str) -> None:
    """Create a horizontal bar chart showing the average delays for each company."""

    st.write('<h2 style="font-size: 24px;">Average delays for each company.</h2>',
             unsafe_allow_html=True)

    max_companies = 20

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

    chart = alt.Chart(average_delays).mark_bar().encode(
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

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def plot_cancellations_by_company(data_df: pd.DataFrame, selected_company: str):
    """Create a bar chart to compare the frequency of cancellations per company."""

    st.write('<h2 style="font-size: 24px;">Frequency of cancellations per company</h2>',
             unsafe_allow_html=True)

    max_companies = 20

    if len(selected_company) > max_companies:
        st.error(f"You have reached the maximum limit of {max_companies} companies in display.\
                  Please remove a company to add more.")
        # Truncate the list
        selected_company = selected_company[:max_companies]

    if len(selected_company) != 0:
        data_df = data_df[data_df['company_name'].isin(selected_company)]

    cancellation_counts = data_df.groupby(
        'company_name')['cancel_code'].count().reset_index()
    cancellation_counts.columns = ['company_name', 'cancellation_count']

    cancellation_counts = cancellation_counts.sort_values(
        by='cancellation_count', ascending=False)

    cancellation_counts = cancellation_counts.sort_values(by='cancellation_count',
                                                          ascending=False).head(max_companies)

    chart = alt.Chart(cancellation_counts).mark_bar().encode(
        x=alt.X('cancellation_count:Q', title='Cancellation Count'),
        y=alt.Y('company_name:N', title='Company Name',
                sort='-x'),  # Sort in descending order
        tooltip=['company_name:N', 'cancellation_count:Q']
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def plot_cancellations_by_company_and_reason(data_df: pd.DataFrame, selected_company: str):
    """Create a stacked bar chart to visualize cancellations by company and reason."""
    st.write('<h2 style="font-size: 24px;">Cancellations and reasons per company</h2>',
             unsafe_allow_html=True)

    max_companies = 20

    if len(selected_company) > max_companies:
        st.error(f"You have reached the maximum limit of {max_companies} companies in display.\
                  Please remove a company to add more.")
        # Truncate the list
        selected_company = selected_company[:max_companies]

    if len(selected_company) != 0:
        data_df = data_df[data_df['company_name'].isin(selected_company)]

    # Filter rows with cancellations
    cancellations = data_df[data_df['cancel_code'].notna()]

    # Group data by company and cancel_reason and count the occurrences
    company_reason_counts = cancellations.groupby(
        ['company_name', 'cancel_code']).size().reset_index(name='frequency')

    cancel_reasons_df = data_df[['cancel_code',
                                 'cancel_reason']].drop_duplicates()
    company_reason_counts = company_reason_counts.merge(
        cancel_reasons_df, on='cancel_code', how='left')

    chart = alt.Chart(company_reason_counts).mark_bar().encode(
        x=alt.X('company_name:N', title='Company Name',
                axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('frequency:Q', title='Frequency'),
        color=alt.Color('cancel_reason:N', title='Cancellation Reason',
                        scale=alt.Scale(scheme='category20')),
        tooltip=['company_name:N', 'cancel_reason:N', 'frequency:Q']
    ).properties(
        width=2000,
        height=400
    ).configure_axis(
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def create_multiselect(filter):
    st.markdown("""
        <style>
        li {
        background-color: lightblue !important;
        }
        </style>
        """, unsafe_allow_html=True)

    select_station = st.sidebar.multiselect(
        ".", options=database_df[f"{filter}"].unique())

    return select_station


if __name__ == "__main__":

    load_dotenv()
    connection = get_db_connection()
    database_df = get_live_database(connection)

    st.set_page_config(
        page_title="Train Services Monitoring Dashboard", layout="wide")

    st.sidebar.image("logo.png", use_column_width=True)

    sidebar_header("FILTER OPTIONS:")
    sidebar_header("SELECT A PAGE")

    page = st.sidebar.selectbox("", ["COMPANY PAGE", "STATION PAGE"])

    if page == "STATION PAGE":
        dashboard_header("STATION")
        sidebar_header("SELECT STATION")

        select_station = create_multiselect("origin_station_name")

        first_row_display(database_df)
        second_row_display(database_df)

        plot_average_delays_by_station(database_df, select_station)
        create_average_delays_line_charts(database_df, select_station)
        plot_cancellations_per_station(database_df, select_station)
        plot_cancellations_by_station_and_reason(database_df, select_station)
        plot_bus_replacements_per_station(database_df, select_station)
        plot_percentage_of_services_reaching_final_destination(
            database_df, select_station)

    elif page == "COMPANY PAGE":
        dashboard_header("COMPANY")
        sidebar_header("SELECT COMPANY")

        select_companies = create_multiselect("company_name")

        first_row_display_company(database_df)
        second_row_display_company(database_df)

        plot_most_average_delays_by_company(database_df, select_companies)
        plot_cancellations_by_company(database_df, select_companies)
        plot_cancellations_by_company_and_reason(database_df, select_companies)
        plot_percentage_of_services_reaching_final_destination_by_company(
            database_df, select_companies)
