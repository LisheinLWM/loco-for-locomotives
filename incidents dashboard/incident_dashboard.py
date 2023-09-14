from os import environ
from os import _Environ

import streamlit as st
import boto3
from datetime import datetime
from boto3 import client
from boto3.resources.base import ServiceResource
from dotenv import load_dotenv
import altair as alt
from altair.vegalite.v5.api import Chart
import pandas as pd
from pandas import DataFrame
from psycopg2 import connect
from psycopg2.extensions import connection

def subscribe_to_topic(sns: ServiceResource, phone_number: str,
                        operator_code: str) -> None:
    print(f"Subscribing {phone_number}.")
    sns.subscribe(TopicArn=f"arn:aws:sns:eu-west-2:129033205317:rail-incidents-{operator_code}",
                  Protocol='sms',
                  Endpoint=phone_number,
                  ReturnSubscriptionArn=True)


def connect_to_db(environ: _Environ) -> connection:

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

    try:
        return client('sns', region_name=environ["AWS_REGION"],
                        aws_access_key_id=environ["ACCESS_KEY_ID"],
                        aws_secret_access_key=environ["SECRET_ACCESS_KEY"])
    
    except:
        print("Error generating SNS client.")


def display_headline_figures(incident_df: DataFrame):

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


def display_most_recent_incident(conn):

    with conn.cursor() as cur:

        query = """
            SELECT
            i.incident_num as "Incident Number",
            i.summary as "Incident Summary",
            p.priority_code as "Incident Priority",
            i.incident_version,
            i.link,
            i.start_time,
            i.end_time,
            string_agg(DISTINCT r.route_name, ', ') as "Affected Routes",
            string_agg(DISTINCT o.operator_name, ', ') as "Affected Operators"
        FROM
            incident i
        LEFT JOIN
            priority p
        ON
            i.priority_id = p.priority_id
        LEFT JOIN
            incident_route_link irl
        ON
            i.incident_id = irl.incident_id
        LEFT JOIN
            route_affected r
        ON
            irl.route_id = r.route_id
        LEFT JOIN
            incident_operator_link iol
        ON
            i.incident_id = iol.incident_id
        LEFT JOIN
            operator o
        ON
            iol.operator_id = o.operator_id
        GROUP BY
            i.incident_num,
            i.incident_version,
            i.link,
            i.summary,
            p.priority_code,
            i.start_time,
            i.end_time
        LIMIT 100;
        """

        data = pd.read_sql_query(query, conn)
    
    conn.commit()
    
    idx = data.groupby('Incident Number')['incident_version'].idxmax()
    data = data.loc[idx]

    st.subheader("MOST RECENT INCIDENTS:")
    st.markdown("""<style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>""", unsafe_allow_html=True)
    st.dataframe(data, hide_index=True)


def get_subscription_count(sns: ServiceResource, operator_code: str) -> int:

    results = sns.list_subscriptions_by_topic(
    TopicArn=f"arn:aws:sns:eu-west-2:129033205317:rail-incidents-{operator_code}")
    print(results)
    return len(results["Subscriptions"])


def calculate_total_subscriptions(sns_client: ServiceResource, operator_list: list[str]):

    count = 0
    for operator in operator_list:
        count += get_subscription_count(sns_client, operator)
    return count


def show_metrics_for_given_operator(sns_client: ServiceResource, operator_list: list[str],
                                    incident_df: DataFrame):

    code = st.selectbox('SELECT OPERATOR TO VIEW METRICS FOR', options=operator_list)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("TOTAL SUBSCRIPTIONS, ALL OPERATORS",
                  calculate_total_subscriptions(sns_client, operator_list))
    with col2:
        st.metric(f"{code} SUBSCRIBER COUNT", get_subscription_count(sns_client, code))
    idx = incident_df.groupby('incident_num')['incident_version'].idxmax()
    incident_df = incident_df.loc[idx]
    with col3:
        st.metric(f"{code} TOTAL INCIDENTS", len(incident_df[incident_df["operator_code"] == code]))
    with col4:
        current_time = datetime.now()
        incident_df = incident_df[incident_df['operator_code'].isin([code])]
        current_incidents = incident_df[(incident_df['start_time'] <= current_time) & 
                                        ((current_time <= incident_df['end_time']) |
                                         incident_df['end_time'].isna())]
        st.metric(f"{code} ACTIVE INCIDENTS", len(current_incidents))

def create_incident_subscription_form(operator_list: list[str]):

    with st.form(clear_on_submit=True, key="subscribe_form"):
        st.subheader("SUBSCRIBE TO GET INCIDENT NOTIFICATIONS")
        phone_number = st.text_input("PHONE NUMBER (INCL. AREA CODE)")
        operator = st.selectbox("OPERATOR", options=operator_list)
        submit_button = st.form_submit_button("SUBSCRIBE")
        if submit_button:
            print(operator, phone_number)
            subscribe_to_topic(sns_client, phone_number, operator)    


def retrieve_incident_data_as_dataframe(conn: connection) -> DataFrame:

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

    with conn.cursor() as cur:

        cur.execute("SET SEARCH_PATH TO incident_data;")

    conn.commit()


def bar_graph_avg_incidents_per_day_per_operator(df: DataFrame) -> None:

    idx = df.groupby('incident_num')['incident_version'].idxmax()
    df = df.loc[idx]
    
    df['start_time'] = pd.to_datetime(df['start_time'])

    operator_avg_incidents = df.groupby(['operator_code', df['start_time'].dt.date])['incident_id'].count().reset_index()
    operator_avg_incidents = operator_avg_incidents.rename(columns={'incident_id': 'avg_incidents'})

    st.subheader("AVERAGE INCIDENTS PER DAY BY OPERATOR\n")

    chart = alt.Chart(operator_avg_incidents).mark_bar().encode(
        x='operator_code:N',
        y='mean(avg_incidents):Q',
        color='operator_code:N'
    ).properties(
        width=600
    )

    st.altair_chart(chart, use_container_width=True)


def bar_graph_avg_incidents_per_day_per_route(df: DataFrame) -> None:

    df['start_time'] = pd.to_datetime(df['start_time'])

    route_avg_incidents = df.groupby(['route_name', df['start_time'].dt.date])['incident_id'].count().reset_index()
    route_avg_incidents = route_avg_incidents.rename(columns={'incident_id': 'avg_incidents'})

    st.subheader("AVERAGE INCIDENTS PER DAY BY PER ROUTE\n")

    bar_chart = alt.Chart(route_avg_incidents).mark_bar().encode(
        x=alt.X('route_name:N', title='Route'),
        y=alt.Y('mean(avg_incidents):Q', title='Average Incidents per Day')
    ).properties(
        width=600
    )

    bar_chart


if __name__ == "__main__":

    load_dotenv()

    conn = connect_to_db(environ)

    set_search_path(conn)

    incident_df = retrieve_incident_data_as_dataframe(conn)

    print(list(incident_df.columns.values))
    print(incident_df.info)

    sns_client = generate_sns_client(environ)
    
    operator_list = ['LO', 'VT', 'CC', 'CS', 'CH', 'XC',
                    'EM', 'XR', 'ES', 'GC', 'LE', 'GW',
                    'HX', 'HT', 'GR', 'LD', 'ME', 'NT',
                    'SR', 'SE', 'TP', 'AW', 'LM', 'GX',
                    'GN', 'SN', 'TL', 'SW', 'IL']

    st.title("DISRUPTION DETECT: INCIDENTS")

    st.divider()

    display_headline_figures(incident_df)

    st.divider()

    show_metrics_for_given_operator(sns_client, operator_list, incident_df)

    st.divider()

    create_incident_subscription_form(operator_list)

    st.divider()

    display_most_recent_incident(conn)

    st.divider()

    selected_operators = st.sidebar.multiselect("Operator", set(incident_df["operator_name"].unique().tolist()))
    operator_filtered_df = incident_df[incident_df['operator_name'].isin(selected_operators)]
    bar_graph_avg_incidents_per_day_per_operator(operator_filtered_df)

    st.divider()

    selected_routes = st.sidebar.multiselect("Route", set(incident_df["route_name"].unique().tolist()))
    route_filtered_df = incident_df[incident_df['route_name'].isin(selected_routes)]
    bar_graph_avg_incidents_per_day_per_route(route_filtered_df)