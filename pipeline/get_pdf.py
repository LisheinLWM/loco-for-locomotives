from os import environ
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection
from datetime import datetime, timedelta
from xhtml2pdf import pisa

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


def get_db_connection() -> connection:
    """Establish a database connection."""
    try:
        conn = psycopg2.connect(
            user=environ["DB_USER"],
            password=environ["DB_PASS"],
            host=environ["DB_HOST"],
            database=environ["DB_NAME"],
        )
        print("Database connection established successfully.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None


def get_data_from_database(conn: connection):
    """ Retrieve the tables for database and return as a data frame."""

    yesterday = datetime.now() - timedelta(days=1)
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
    return data_df


def export_to_html(data, average_delays, total_services):
    """ Create the HTML string and export to html file."""
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Report Data</title>
    </head>
    <body>
        <h1>
        Total services
        </h1>
        <p> {total_services.to_html(index=False)} </p>
        
        <h1>Report Data</h1>
        <p> {data.to_html(index=False)}</p>
        
        
        <h2>Average Delays per Company</h2>
        {average_delays.to_html(index=False)}
    </body>
    </html>
    """
    return html_content


def convert_html_to_pdf(source_html, output_filename):

    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
        source_html,
        dest=result_file
    )

    result_file.close()

    return pisa_status.err


def create_report(data):
    average = get_average_delays(data)
    total_services = data.groupby(
        'origin_station_name').size().reset_index(name='total_services')
    html_data = export_to_html(data, average, total_services)

    convert_html_to_pdf(html_data, "test.pdf")


def get_average_delays(data_df):

    average_delays = data_df.groupby('company_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays = average_delays.sort_values(
        by='arrival_lateness', ascending=False).head(20)

    return average_delays


def lambda_handler(event=None, context=None) -> dict:
    load_dotenv()
    connection = get_db_connection()
    data_df = get_data_from_database(connection)
    create_report(data_df)

    return {
        "statusCode": 200,
        "body": "Hello"
    }


if __name__ == "__main__":
    lambda_handler()
