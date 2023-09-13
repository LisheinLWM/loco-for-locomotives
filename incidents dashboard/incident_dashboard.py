from os import environ
from os import _Environ

import streamlit as st
import boto3
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


def display_headline_figures(incident_df):

    pass


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


    st.write("MOST RECENT INCIDENTS:")
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


def show_metrics_for_given_operator(sns_client: ServiceResource, operator_list: list[str]):

    code = st.selectbox('SELECT OPERATOR TO VIEW METRICS FOR', options=operator_list)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("TOTAL SUBSCRIPTIONS, ALL OPERATORS",
                  calculate_total_subscriptions(sns_client, operator_list))
    with col2:
        st.metric(f"{code} SUBSCRIBER COUNT", get_subscription_count(sns_client, code))
        

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


if __name__ == "__main__":

    load_dotenv()

    conn = connect_to_db(environ)

    set_search_path(conn)

    incident_df = retrieve_incident_data_as_dataframe(conn)

    print(list(incident_df.columns.values))

    sns_client = generate_sns_client(environ)
    
    operator_list = ['LO', 'VT', 'CC', 'CS', 'CH', 'XC',
                    'EM', 'XR', 'ES', 'GC', 'LE', 'GW',
                    'HX', 'HT', 'GR', 'LD', 'ME', 'NT',
                    'SR', 'SE', 'TP', 'AW', 'LM', 'GX',
                    'GN', 'SN', 'TL', 'SW', 'IL']

    st.title("DISRUPTION DETECT")

    st.divider()

    show_metrics_for_given_operator(sns_client, operator_list)

    st.divider()

    create_incident_subscription_form(operator_list)

    st.divider()

    display_most_recent_incident(conn)

