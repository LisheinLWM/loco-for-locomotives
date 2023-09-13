from os import environ
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection
from datetime import datetime, timedelta
from xhtml2pdf import pisa
from boto3 import client

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
    """Retrieve the tables for database and return as a data frame."""

    # MUST CHANGE BACK TO 1! THIS IS IMPORTANT
    yesterday = datetime.now() - timedelta(days=2)
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
    """Create the HTML string and export to html file."""

    yesterday = datetime.now() - timedelta(days=2)
    yesterday_date = yesterday.strftime("%d-%m-%Y")

    total_services_html = total_services.to_html(
        index=False, classes="center", justify="center")
    total_services_html = total_services_html.replace(
        '<td>', '<td align="center">')

    average_delays_html = average_delays.to_html(index=False, classes="center")
    average_delays_html = average_delays_html.replace(
        '<td>', '<td align="center">')

    company = data.groupby(
        'company_name')['arrival_lateness'].sum().reset_index()
    company_html = company.to_html(
        index=False, classes="center", justify="center")
    company_html = company_html.replace(
        '<td>', '<td align="center">')

    cancellations = data.groupby('company_name')[
        'cancel_code'].count().reset_index()
    cancellations = cancellations.to_html(
        index=False, classes="center", justify="center")
    cancellations = cancellations.replace(
        '<td>', '<td align="center">')

    delays_station = data.groupby(
        'origin_station_name')['arrival_lateness'].mean().reset_index()
    delays_station = delays_station.to_html(
        index=False, classes="center", justify="center")
    delays_station = delays_station.replace(
        '<td>', '<td align="center">')

    cancellations_station = data.groupby(
        'origin_station_name')['cancel_code'].count().reset_index()
    cancellations_station = cancellations_station.to_html(
        index=False, classes="center", justify="center")
    cancellations_station = cancellations_station.replace(
        '<td>', '<td align="center">')

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Report Data</title>
    </head>
    <body>
        <h1 align="right">
        <center>
        Report to summary yesterday's data
        </center>
        Data for {yesterday_date}
        </h1>
        <align="center">
        The total number of services on {yesterday_date} was {data["service_uid"].count()}.

        <h2><center>Total services per station</center></h2>
        <p> {total_services_html} </p>
        <h2><center>Average Delays per Company</center></h2>
        <p>{average_delays_html}</p>
        <h2><center>Total Delays per Company</center></h2>
        <p>{company_html}</p>
        <h2><center>Cancellations per Company</center></h2>
        <p>{cancellations}</p>
        <h2><center>Delays per Station</center></h2>
        <p>{delays_station}</p>
        <h2><center>Cancellations per Station</center></h2>
        <p>{cancellations_station}</p>

    </body>
    </html>
    """
    return html_content


def convert_html_to_pdf(source_html, output_filename):
    """Using the html provided, outputs a pdf as requested to be stored in s3"""
    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
        source_html,
        dest=result_file
    )

    result_file.close()

    return pisa_status.err


def create_report(data):
    """
    Creates the pdf report in one function and 
    calls all functions required to do so
    """
    average = get_average_delays(data)
    total_services = data.groupby(
        'origin_station_name').size().reset_index(name='total_services')
    html_data = export_to_html(data, average, total_services)

    convert_html_to_pdf(html_data, "test.pdf")


def get_average_delays(data_df):
    """Gets the average delays by company"""
    average_delays = data_df.groupby('company_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays = average_delays.sort_values(
        by='arrival_lateness', ascending=False).head(20)

    return average_delays


# def upload_to_s3_bucket(file_name):
#     """Uploads the pdf to our s3 bucket as required."""
#     amazon_s3 = client("s3", region_name="eu-west-2",
#                        aws_access_key_id=os.environ["ACCESS_KEY_ID"],
#                        aws_secret_access_key=os.environ["SECRET_ACCESS_KEY_ID"])

#     amazon_s3.upload_file(
#         f'/tmp/{file_name}',
#         bucket_name,
#         f'{file_name}')


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
