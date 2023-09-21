"""Streamlit dashboard file: used to build visualisations based on the incidents schema of the database"""

from os import environ
from os import _Environ
from datetime import datetime

import altair as alt
from boto3 import client
from boto3.resources.base import ServiceResource
from dotenv import load_dotenv
import streamlit as st
from pandas import DataFrame
from psycopg2 import connect
from psycopg2.extensions import connection
import pandas as pd


def subscribe_to_topic(sns: ServiceResource, phone_number: str,
                        operator_code: str) -> None:
    """
    Subscribes a provided phone number to the
    chosen rail SNS topic
    """
    print(f"Subscribing {phone_number}.")
    try:
        sns.subscribe(TopicArn=f"arn:aws:sns:eu-west-2:129033205317:rail-incidents-{operator_code}",
                    Protocol='sms',
                    Endpoint=phone_number,
                    ReturnSubscriptionArn=True)
    except:
        print("Invalid selection.")


def connect_to_db(environ: _Environ) -> connection:
    """
    Returns a connection to the database
    """
    try:
        return connect(
            database=environ["DB_NAME"],
            user=environ["DB_USER"],
            password=environ["DB_PASS"],
            port=environ["DB_PORT"],
            host=environ["DB_HOST"]
        )
    except:
        print("Error connecting to database.")


def generate_sns_client(environ: _Environ) -> ServiceResource:
    """
    Returns an SNS client
    """
    try:
        return client('sns', region_name=environ["AWS_REGION"],
                        aws_access_key_id=environ["ACCESS_KEY_ID"],
                        aws_secret_access_key=environ["SECRET_ACCESS_KEY"])
    except:
        print("Error generating SNS client.")


def display_headline_figures(incident_df: DataFrame):
    """
    Displays a number of headline figures, generated
    from the given DataFrame, in columns
    """
    col1, col2, col3, col4 = st.columns(4)
    idx = incident_df.groupby('incident_num')['incident_version'].idxmax()
    incident_df = incident_df.loc[idx]
    with col1:
        st.metric(f"TOTAL INCIDENTS", incident_df.shape[0])
    with col2:
        current_time = datetime.now()
        incident_df["start_time"] = pd.to_datetime(incident_df['start_time'])
        incident_df['end_time'] = pd.to_datetime(incident_df['end_time'])
        current_incidents = incident_df[(incident_df['start_time'] <= current_time) & 
                                        ((current_time <= incident_df['end_time']) |
                                         incident_df['end_time'].isna())]
        st.metric("ACTIVE INCIDENTS ", len(current_incidents))
    with col3:
        operator_counts = incident_df['operator_code'].value_counts()
        operator_with_highest_incidents = operator_counts.idxmax()
        highest_incident_count = operator_counts.max()
        st.metric("OPERATOR W/ MOST INCIDENTS ", f"{operator_with_highest_incidents} ({highest_incident_count})")
    with col4:
        incident_priority_counts = incident_df['priority_code'].value_counts()
        priority_with_highest_count = incident_priority_counts.idxmax()
        highest_incident_priority_count = incident_priority_counts.max()
        st.metric("MOST COMMON INCIDENT PRIORITY", f"{priority_with_highest_count} ({highest_incident_priority_count})")


def display_active_incidents(data: DataFrame):
    """
    Uses Streamlit to display a table of the most
    recent incidents, pulled from the database with
    an SQL query.
    """
    idx = data.groupby('incident_num')['incident_version'].idxmax()
    data = data.loc[idx]

    data = data[['incident_num', 'operator_name', 'summary', 'route_name',
                 'link', 'priority_code', 'start_time', 'end_time', 'is_planned']]
    data.columns = ['Incident Number', 'Operator Name', 'Summary', 'Affected Routes',
                    'Link', 'Priority Code', 'Start Time', 'End Time', 'Is Planned']

    current_time = datetime.now()
    data = data[(data['Start Time'] <= current_time) & 
                ((current_time <= data['End Time']) |
                data['End Time'].isna())]

    st.subheader("ACTIVE INCIDENTS:")
    st.markdown("""<style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>""", unsafe_allow_html=True)
    st.dataframe(data, hide_index=True)


def get_subscription_count(sns: ServiceResource, operator_code: str) -> int:
    """
    Returns the subscription count for the
    specified SNS topic
    """
    results = sns.list_subscriptions_by_topic(
    TopicArn=f"arn:aws:sns:eu-west-2:129033205317:rail-incidents-{operator_code}")
    print(results)
    return len(results["Subscriptions"])


def calculate_total_subscriptions(sns_client: ServiceResource, operator_list: list[str]):
    """
    Returns the total number of subscriptions
    to rail operator topics
    """
    count = 0
    for operator in operator_list:
        count += get_subscription_count(sns_client, operator)
    return count


def show_metrics_for_given_operator(sns_client: ServiceResource, operators_dict: dict,
                                    incident_df: DataFrame):
    """
    Displays specific statistics for the
    specified operator
    """
    idx = incident_df.groupby('incident_num')['incident_version'].idxmax()
    incident_df = incident_df.loc[idx]
    operator_name = st.selectbox('SELECT OPERATOR TO VIEW METRICS FOR', options=operators_dict.keys())
    code = operators_dict.get(operator_name)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("TOTAL SUBSCRIPTIONS, ALL OPERATORS",
                  calculate_total_subscriptions(sns_client, operators_dict.values()))
    with col2:
        st.metric(f"{code} SUBSCRIBER COUNT", get_subscription_count(sns_client, code))
    with col3:
        st.metric(f"{code} TOTAL INCIDENTS", len(incident_df[incident_df["operator_code"] == code]))
    with col4:
        current_time = datetime.now()
        incident_df = incident_df[incident_df['operator_code'].isin([code])]
        current_incidents = incident_df[(incident_df['start_time'] <= current_time) & 
                                        ((current_time <= incident_df['end_time']) |
                                         incident_df['end_time'].isna())]
        st.metric(f"{code} ACTIVE INCIDENTS", len(current_incidents))


def create_incident_subscription_form(operator_dict: dict):
    """
    Creates a form which allows users to input
    information and subscribe their number to
    the relevant SNS topic    
    """
    with st.form(clear_on_submit=True, key="subscribe_form"):
        st.subheader("SUBSCRIBE TO GET INCIDENT NOTIFICATIONS")
        phone_number = st.text_input("PHONE NUMBER (INCL. AREA CODE)")
        operator_name = st.selectbox("OPERATOR", options=operator_dict.keys())
        code = operator_dict.get(operator_name)
        submit_button = st.form_submit_button("SUBSCRIBE")
        if submit_button:
            print(code, phone_number)
            subscribe_to_topic(sns_client, phone_number, code)    


def retrieve_incident_data_as_dataframe(conn: connection) -> DataFrame:
    """
    Connects to the RDS database, selects relevant
    incident data, and returns it as a DataFrame
    """
    with conn.cursor() as cur:

        query = """SELECT
            i.incident_id,
            i.incident_num,
            i.incident_version,
            i.link,
            i.summary,
            p.priority_code,
            i.is_planned,
            i.creation_time,
            i.start_time,
            i.end_time,
            o.operator_code,
            o.operator_name,
            o.customer_satisfaction,
            r.route_name
        FROM
            incident i
        LEFT JOIN
            priority p
        ON
            i.priority_id = p.priority_id
        LEFT JOIN
            incident_operator_link iol
        ON
            i.incident_id = iol.incident_id
        LEFT JOIN
            operator o
        ON
            iol.operator_id = o.operator_id
        LEFT JOIN
            incident_route_link irl
        ON
            i.incident_id = irl.incident_id
        LEFT JOIN
            route_affected r
        ON
            irl.route_id = r.route_id;
        """

        df = pd.read_sql_query(query, conn)
    
    conn.commit()

    return df


def set_search_path(conn: connection) -> None:
    """
    Sets the search path of a Database connection
    to 'incident_data'
    """
    with conn.cursor() as cur:

        cur.execute("SET SEARCH_PATH TO incident_data;")

    conn.commit()


def bar_graph_avg_incidents_per_day_per_operator(df: DataFrame) -> None:
    """
    Displays a graph which visualises the
    average number of incidents per day
    for each operator
    """
    idx = df.groupby('incident_num')['incident_version'].idxmax()
    df = df.loc[idx]
    
    df['start_time'] = pd.to_datetime(df['start_time'])

    operator_avg_incidents = df.groupby(['operator_code', df['start_time'].dt.date])['incident_id'].count().reset_index()
    operator_avg_incidents = operator_avg_incidents.rename(columns={'incident_id': 'avg_incidents'})

    st.subheader("AVERAGE INCIDENTS PER DAY BY OPERATOR\n")

    bar_chart = alt.Chart(operator_avg_incidents).mark_bar().encode(
        x=alt.X('operator_code:N', title='Operator'),
        y=alt.Y('mean(avg_incidents):Q', title='Average Incidents per Day'),
        color='operator_code:N'
    ).properties(
        width=600
    )

    st.altair_chart(bar_chart, use_container_width=True)


def bar_graph_avg_incidents_per_day_per_route(df: DataFrame) -> None:
    """
    Displays a graph which visualises the
    average number of incidents per day
    for each route
    """
    idx = df.groupby('incident_num')['incident_version'].idxmax()
    df = df.loc[idx]

    df['start_time'] = pd.to_datetime(df['start_time'])

    route_avg_incidents = df.groupby(['route_name', df['start_time'].dt.date])['incident_id'].count().reset_index()
    route_avg_incidents = route_avg_incidents.rename(columns={'incident_id': 'avg_incidents'})

    st.subheader("AVERAGE INCIDENTS PER DAY BY BY ROUTE\n")

    bar_chart = alt.Chart(route_avg_incidents).mark_bar().encode(
        x=alt.X('route_name:N', title='Route'),
        y=alt.Y('mean(avg_incidents):Q', title='Average Incidents per Day')
    ).properties(
        width=600
    )

    bar_chart


def bar_graph_to_show_incidents_per_day(incident_df: DataFrame):

    current_time = datetime.now()
    one_month_ago = current_time - pd.DateOffset(months=1)

    idx = incident_df.groupby('incident_num')['incident_version'].idxmax()
    incident_df = incident_df.loc[idx]
    filtered_incident_df = incident_df[(incident_df['start_time'] >= one_month_ago) & (incident_df['start_time'] <= current_time)]

    daily_incident_counts = filtered_incident_df.groupby(filtered_incident_df['start_time'].dt.date)['incident_id'].count().reset_index()
    daily_incident_counts = daily_incident_counts.rename(columns={'start_time': 'Date', 'incident_id': 'Incident Count'})

    st.subheader("INCIDENTS PER DAY (PAST MONTH)\n")

    bar_chart = alt.Chart(daily_incident_counts).mark_bar().encode(
        x=alt.X('Date:T', title='Date'),
        y=alt.Y('Incident Count:Q', title='Incident Count')
    ).properties(
        width=800
    )

    st.altair_chart(bar_chart, use_container_width=True)


def scatter_plot_to_show_incident_freq_vs_customer_satisfaction(incident_df: DataFrame):

    idx = incident_df.groupby('incident_num')['incident_version'].idxmax()
    incident_df = incident_df.loc[idx]

    operator_stats = incident_df.groupby('operator_name').agg({
        'incident_id': 'count',
        'customer_satisfaction': 'mean'
    }).reset_index()

    operator_stats.columns = ['Operator', 'Average Daily Incidents', 'Customer Satisfaction']

    scatter_plot = alt.Chart(operator_stats).mark_circle().encode(
        x='Average Daily Incidents:Q',
        y='Customer Satisfaction:Q',
        color='Operator:N',
        tooltip=['Operator', 'Average Daily Incidents', 'Customer Satisfaction']
    ).properties(
        width=600,
        height=400,
        title='Operator Average Daily Incidents vs. Customer Satisfaction (Scatter Plot)'
    )

    st.altair_chart(scatter_plot, use_container_width=True)


if __name__ == "__main__":

    load_dotenv()

    conn = connect_to_db(environ)
    set_search_path(conn)
    incident_df = retrieve_incident_data_as_dataframe(conn)

    sns_client = generate_sns_client(environ)

    operators_dict = {
        "London Overground": "LO",
        "Avanti West Coast": "VT",
        "c2c": "CC",
        "Caledonian Sleeper": "CS",
        "Chiltern Railways": "CH",
        "CrossCountry": "XC",
        "East Midlands Railway": "EM",
        "Elizabeth line": "XR",
        "Eurostar": "ES",
        "Gatwick Express": "GX",
        "Great Northern": "GN",
        "Southern": "SN",
        "Thameslink": "TL",
        "Grand Central": "GC",
        "Greater Anglia (also operating Stansted Express)": "LE",
        "Great Western Railway": "GW",
        "Heathrow Express": "HX",
        "Hull Trains": "HT",
        "London North Eastern Railway": "GR",
        "Lumo": "LD",
        "Merseyrail": "ME",
        "Northern Trains (Trading as Northern)": "NT",
        "ScotRail": "SR",
        "Southeastern": "SE",
        "South Western Railway": "SW",
        "Island Line": "IL",
        "TransPennine Trains (Trading as TransPennine Express)": "TP",
        "Transport for Wales Rail": "AW",
        "West Midlands Trains": "LM"
    }

    st.title("DISRUPTION DETECT: INCIDENTS")
    st.divider()
    display_headline_figures(incident_df)
    st.divider()
    display_active_incidents(incident_df)
    st.divider()
    show_metrics_for_given_operator(sns_client, operators_dict, incident_df)
    st.divider()
    create_incident_subscription_form(operators_dict)
    st.divider()