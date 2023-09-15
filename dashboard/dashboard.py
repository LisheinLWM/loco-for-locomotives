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


def dashboard_header(page:str) -> None:
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

    most_cancelled_station = data_df['origin_station_name'].value_counts().idxmax()

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
        most_cancelled_station = data_df['origin_station_name'].value_counts().idxmax()
        num_cancellations = data_df.loc[data_df['arrival_lateness'].idxmax()]['origin_station_name']
        st.write("MOST DELAYED STATION:",
                 data_df.loc[data_df['arrival_lateness'].idxmax()]['origin_station_name'])

    with cols[1]:
        most_cancelled_station = data_df['origin_station_name'].value_counts().idxmax()
        num_cancellations = data_df['origin_station_name'].value_counts().max()
        st.write("MOST CANCELLED STATION:",
                 f"{most_cancelled_station} (Num of cancellations: {num_cancellations})")

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
    """Create a line graph showing the number of cancellations per station."""

    st.write('<h2 style="font-size: 24px;"> Number of cancellations per station</h2>',
             unsafe_allow_html=True)

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

    cancellations_per_station = cancellations_per_station.sort_values(
        by='cancellation_count', ascending=False).head(20)

    chart = alt.Chart(cancellations_per_station).mark_line().encode(
        x=alt.X('origin_station_name:N', title='STATION NAME'),
        y=alt.Y('cancellation_count:Q', title='NUMBER OF CANCELLATIONS'),
        tooltip=[alt.Tooltip('origin_station_name:N', title='STATION NAME'),
                 alt.Tooltip('cancellation_count:Q', title='NUMBER OF CANCELLATIONS')]
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelAngle=90,
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


def plot_percentage_of_services_reaching_final_destination(data_df: pd.DataFrame, selected_station) -> None:
    """Create a stacked bar chart showing the breakdown of services that reached their destination and those that didn't per station."""
    
    st.write('<h2 style="font-size: 24px;">Breakdown of Services by Final Destination per Station</h2>',
             unsafe_allow_html=True)

    max_stations = 20

    if len(selected_station) > max_stations:
        st.error(
            f"You have reached the maximum limit of {max_stations} stations in display. Please remove a station to add more.")
        selected_station = selected_station[:max_stations]  # Truncate the list

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(selected_station)]


    data_df['destination_status'] = data_df['reached_station_id'].apply(lambda x: 'Reached' if pd.isna(x) or x == '' else 'Not Reached')

    station_summary = data_df.groupby(['origin_station_name', 'destination_status']).size().reset_index(name='count')

    chart = alt.Chart(station_summary).mark_bar().encode(
        x=alt.X('origin_station_name:N', title='Station Name'),
        y=alt.Y('count:Q', title='Count'),
        color=alt.Color('destination_status:N', scale=alt.Scale(scheme='set1'), title='Destination Status'),
        tooltip=[
            alt.Tooltip('origin_station_name:N', title='Station Name'),
            alt.Tooltip('destination_status:N', title='Destination Status'),
            alt.Tooltip('count:Q', title='Count')
        ]
    ).properties(
        width=600,
        height=400
    ).configure_title(
        fontSize=16,
        color="#333"
    )

    st.altair_chart(chart, use_container_width=True)


def create_scatter_plot_arrival_lateness_vs_scheduled(data_df: pd.DataFrame, selected_station) -> None:
    """Create a scatter plot of Arrival Lateness vs. Scheduled Arrival."""

    st.write('<h2 style="font-size: 24px;">Scatter plot of arrival lateness vs scheduled arrival</h2>', unsafe_allow_html=True)

    if len(selected_station) != 0:
        data_df = data_df[data_df['origin_station_name'].isin(selected_station)]

    # Format the scheduled_arrival column as a string with date and time
    data_df['scheduled_arrival_formatted'] = data_df['scheduled_arrival'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Create a tooltip field that combines origin and destination station names
    data_df['station_tooltip'] = data_df['origin_station_name'] + ' to ' + data_df['destination_station_name']

    chart = alt.Chart(data_df).mark_circle().encode(
        x=alt.X('scheduled_arrival:T', title='Scheduled Arrival'),
        y=alt.Y('arrival_lateness:Q', title='Arrival Lateness'),
        color=alt.Color('arrival_lateness:Q', title='Arrival Lateness', scale=alt.Scale(scheme='viridis')),
        tooltip=[
            alt.Tooltip('station_tooltip:N', title='Station'),
            alt.Tooltip('scheduled_arrival_formatted:N', title='Scheduled Arrival'),
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


def plot_cancel_codes_frequency_with_reasons(data_df):
    """Create a bar chart to visualize the frequency of different cancellation codes."""

    st.write("""<h2 style="font-size: 24px;"> Frequency of cancellation codes with reasons</h2>""",
             unsafe_allow_html=True)

    data_df['cancel_reason'].fillna('None filled out', inplace=True)

    # Count the frequency of each cancellation code
    cancel_code_counts = data_df['cancel_code'].value_counts().reset_index()
    cancel_code_counts.columns = ['cancel_code', 'frequency']

    cancel_reason_df = data_df[['cancel_code',
                                'cancel_reason']].drop_duplicates()

    merged_df = pd.merge(cancel_code_counts, cancel_reason_df,
                         on='cancel_code', how='left')

    merged_df = merged_df.sort_values(by='frequency', ascending=False).head(20)

    chart = alt.Chart(merged_df).mark_bar().encode(
        x=alt.X('cancel_code:N', title='Cancellation Code', sort=alt.EncodingSortField(
            field='bus_replacement_count', order='descending')),
        y=alt.Y('frequency:Q', title='Frequency'),
        tooltip=['cancel_code:N', 'frequency:Q', 'cancel_reason:N']
    ).properties(
        width=800,
        height=400
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


def plot_most_average_delays_by_company(data_df: pd.DataFrame, selected_company:str) -> None:
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

    cancellation_counts = data_df.groupby('company_name')['cancel_code'].count().reset_index()
    cancellation_counts.columns = ['company_name', 'cancellation_count']

    cancellation_counts = cancellation_counts.sort_values(by='cancellation_count', ascending=False)

    cancellation_counts = cancellation_counts.sort_values(by='cancellation_count',
                                                ascending=False).head(max_companies)

    chart = alt.Chart(cancellation_counts).mark_bar().encode(
        x=alt.X('cancellation_count:Q', title='Cancellation Count'),
        y=alt.Y('company_name:N', title='Company Name', sort='-x'),  # Sort in descending order
        tooltip=['company_name:N', 'cancellation_count:Q']  # Include tooltips
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelColor="#1f5475",
        titleColor="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---")


def plot_percentage_of_services_reaching_final_destination_by_company(data_df: pd.DataFrame):
    """Create a pie chart to visualize the percentage of services reaching their planned final destination by company."""

    st.write('<h2 style="font-size: 24px;">Final destination reached services per company</h2>',
             unsafe_allow_html=True)

    services_reached_destination = data_df[data_df['destination_station_id'] == data_df['reached_station_id']]

    # Group data by company and calculate the percentage of services that reached their final destination
    company_summary = services_reached_destination.groupby('company_name').size().reset_index(name='reached_destination_count')
    total_services = data_df.groupby('company_name').size().reset_index(name='total_services')
    company_summary = company_summary.merge(total_services, on='company_name', how='outer')
    company_summary['percentage_reached_destination'] = (company_summary['reached_destination_count'] / company_summary['total_services']) * 100

    company_summary = company_summary.sort_values(by='percentage_reached_destination', ascending=False)

    chart = alt.Chart(company_summary).mark_arc().encode(
        color=alt.Color('company_name:N', title='Company Name'),
        tooltip=[
            alt.Tooltip('company_name:N', title='Company Name'),
            alt.Tooltip('percentage_reached_destination:Q', title='Percentage Reached Destination', format='.2f')
        ],
        theta=alt.Theta('percentage_reached_destination:Q', title='Percentage Reached Destination'),
        text='company_name:N'  # Display company names as labels
    ).properties(
        width=600,
        height=400
    ).configure_text(
        color="#1f5475"
    )

    st.altair_chart(chart, use_container_width=True)



def plot_cancellations_by_company_and_reason(data_df: pd.DataFrame):
    """Create a stacked bar chart to visualize cancellations by company and reason."""
    st.markdown("---")
    st.write('<h2 style="font-size: 24px;">Cancellations and reasons per company</h2>',
             unsafe_allow_html=True)

    # Filter rows with cancellations
    cancellations = data_df[data_df['cancel_code'].notna()]

    # Group data by company and cancel_reason and count the occurrences
    company_reason_counts = cancellations.groupby(['company_name', 'cancel_code']).size().reset_index(name='frequency')

    cancel_reasons_df = data_df[['cancel_code', 'cancel_reason']].drop_duplicates()
    company_reason_counts = company_reason_counts.merge(cancel_reasons_df, on='cancel_code', how='left')

    chart = alt.Chart(company_reason_counts).mark_bar().encode(
        x=alt.X('company_name:N', title='Company Name', axis=alt.Axis(labelAngle=-45)),
        y=alt.Y('frequency:Q', title='Frequency'),
        color=alt.Color('cancel_reason:N', title='Cancellation Reason', scale=alt.Scale(scheme='category20')),
        tooltip=['company_name:N', 'cancel_reason:N', 'frequency:Q']
    ).properties(
        width=2000,
        height=400
    ).configure_axis(
        labelAngle=180,
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

    # Sidebar navigation
    page = st.sidebar.selectbox("", ["STATION PAGE", "COMPANY PAGE"])

    if page == "STATION PAGE":
        dashboard_header("STATION")
        sidebar_header("SELECT STATION")

        select_station = create_multiselect("origin_station_name")

        first_row_display(database_df)
        second_row_display(database_df)

        plot_average_delays_by_station(database_df, select_station)
        plot_cancellations_per_station(database_df, select_station)
        create_scatter_plot_arrival_lateness_vs_scheduled(database_df, select_station)

        plot_bus_replacements_per_station(database_df, select_station)
        
        plot_percentage_of_services_reaching_final_destination(database_df, select_station)
            
        plot_cancel_codes_frequency_with_reasons(database_df)

    elif page == "COMPANY PAGE":
        dashboard_header("COMPANY")
        sidebar_header("SELECT COMPANY")

        select_companies = create_multiselect("company_name")

        plot_most_average_delays_by_company(database_df, select_companies)
        
        plot_cancellations_by_company_and_reason(database_df)

        plot_cancellations_by_company(database_df, select_companies)
        plot_percentage_of_services_reaching_final_destination_by_company(database_df)