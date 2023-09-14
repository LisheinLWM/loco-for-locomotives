from os import environ
import base64
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection
from xhtml2pdf import pisa
from boto3 import client
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


def get_data_from_database(conn: connection) -> pd.DataFrame:
    """Retrieve the tables for database and return as a data frame."""

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


def clean_html_dataframes(data_frame: pd.DataFrame) -> str:
    data_frame_html = data_frame.to_html(
        index=False, classes="center", justify="center")
    data_frame_html = data_frame_html.replace(
        '<td>', '<td align="center">')
    return data_frame_html


def export_to_html(data: pd.DataFrame, average_delays: pd.DataFrame, total_services: pd.DataFrame) -> str:
    """Create the HTML string and export to html file."""

    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday.strftime("%d-%m-%Y")

    total_services_html = clean_html_dataframes(total_services)

    average_delays_html = clean_html_dataframes(average_delays)

    company = data.groupby(
        'company_name')['arrival_lateness'].sum().reset_index()
    company_html = clean_html_dataframes(company)

    cancellations = data.groupby('company_name')[
        'cancel_code'].count().reset_index()
    cancellations = clean_html_dataframes(cancellations)

    delays_station = data.groupby(
        'origin_station_name')['arrival_lateness'].mean().reset_index()
    delays_station = clean_html_dataframes(delays_station)

    cancellations_station = data.groupby(
        'origin_station_name')['cancel_code'].count().reset_index()
    cancellations_station = clean_html_dataframes(cancellations_station)

    cancellations_per_station = data[data['cancellation_id'].notnull()].groupby(
        'origin_station_name')['cancellation_id'].count().reset_index()
    cancellations_per_station = cancellations_per_station.rename(
        columns={'cancellation_id': 'cancellation_count'})

    cancellations_per_company = data[data['cancellation_id'].notnull()].groupby(
        'company_name')['cancellation_id'].count().reset_index()
    cancellations_per_company = cancellations_per_company.rename(
        columns={'cancellation_id': 'cancellation_count'})

    cancellations_per_station = cancellations_per_station.sort_values(
        by='cancellation_count', ascending=False).head(20)

    alt.Chart(cancellations_per_station).mark_line(
        color="#B1D4E0"
    ).encode(
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
    ).save("testing.png")

    with open("testing.png", "rb") as f:
        img_bytes = f.read()
        img = base64.b64encode(img_bytes).decode("utf-8")

    alt.Chart(cancellations_per_company).mark_line(
        color="#B1D4E0"
    ).encode(
        x=alt.X('company_name:N', title='COMPANY NAME'),
        y=alt.Y('cancellation_count:Q', title='NUMBER OF CANCELLATIONS'),
        tooltip=[alt.Tooltip('company_name:N', title='COMPANY NAME'),
                 alt.Tooltip('cancellation_count:Q', title='NUMBER OF CANCELLATIONS')]
    ).properties(
        width=600,
        height=400
    ).configure_axis(
        labelAngle=90,
        labelColor="#1f5475",
        titleColor="#1f5475"
    ).save("testing2.png")

    with open("testing2.png", "rb") as f:
        img2_bytes = f.read()
        img2 = base64.b64encode(img2_bytes).decode("utf-8")

    average_delays2 = data.groupby('origin_station_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays2 = average_delays2.sort_values(by='arrival_lateness',
                                                  ascending=False).head(20)

    alt.Chart(average_delays2).mark_bar(
        color="#B1D4E0"
    ).encode(
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
    ).save('testing3.png')

    with open("testing3.png", "rb") as f:
        img3_bytes = f.read()
        img3 = base64.b64encode(img3_bytes).decode("utf-8")

    average_delays3 = data.groupby('company_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays3 = average_delays3.sort_values(by='arrival_lateness',
                                                  ascending=False).head(20)

    alt.Chart(average_delays3).mark_bar(
        color="#B1D4E0"
    ).encode(
        y=alt.Y('company_name:N', title='COMPANY NAME',
                sort=alt.EncodingSortField(field='arrival_lateness', order='descending')),
        x=alt.X('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)'),
        tooltip=[alt.Tooltip('company_name:N', title='COMPANY NAME'),
                 alt.Tooltip('arrival_lateness:Q', title='AVERAGE DELAY (MINUTES)')]
    ).properties(
        width=600,
        height=500
    ).configure_axis(
        labelAngle=0,
        labelColor="#1f5475",
        titleColor="#1f5475"
    ).save('testing4.png')

    with open("testing4.png", "rb") as f:
        img4_bytes = f.read()
        img4 = base64.b64encode(img4_bytes).decode("utf-8")

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Report Data</title>
        <style>
        .title-container {{
            display: flex;
            flex-direction: row;
        }}
        table {{
            width: 100%;
        }}
        td {{
            text-align: center;
            vertical-align: bottom;
            padding: 1px;
        }}
</style>
    </head>
    <body>
            <div class="title-container">
            <table border="0" style="background-color: #B1D4E0">
            <tr>
            <td><img style="width: 50px; height: 50px; background: #B1D4E0";" src="logo.png" alt="Logo" /></td>
            <td><h1 style="background-color: #B1D4E0; color: #345E7D"> Daily report</h1></td>
            <td><h3 align="right">{yesterday_date}</h3></td>
            </tr>
            </table>
            <br />
            </div>
            <div>
            <table border="0">
            <tr>
            <td>
            <h2>Cancellations per Station</h2>
            <img style="width: 250; height: 200" src="data:image/png;base64,{img}">
            </td>
            <td>
            <h2>Cancellations per Company</h2>
            <img style="width: 250; height: 200" src="data:image/png;base64,{img2}"></td>
            </tr>
            </table>
            </div>
            <div>
            <h2 align="center">Average Delays per Station</h2>
            <img style="width: 500; height: 200" src="data:image/png;base64,{img3}">
            </div><div>
            <h2 align="center">Average Delays per Company</h2>
            <img style="width: 500; height: 200" src="data:image/png;base64,{img4}">
            </div>
            <br /><br /><br /><br />

        <center>
        <h3>Key Statistics</h3>
        <table border="0.1">
        <tr><td>Total number of services</td><td>{data["service_uid"].count()}</td></tr>
        <tr><td>Total number of delays</td><td>{data["arrival_lateness"].count()}</td></tr>
        <tr><td>Total number of cancellations </td><td>{data["cancellation_id"].count()}</td></tr>
        <tr><td>Average delay (minutes)</td><td>{data["arrival_lateness"].mean()}</td></tr>
        </table>
        <h3><center>Total services per station</center></h3>
        <p> {total_services_html} </p>
        <h3><center>Average Delays per Company</center></h3>
        <p>{average_delays_html}</p>
        <h3><center>Total Delays per Company</center></h3>
        <p>{company_html}</p>
        <h3><center>Cancellations per Company</center></h3>
        <p>{cancellations}</p>
        <h3><center>Delays per Station</center></h3>
        <p>{delays_station}</p>
        <h3><center>Cancellations per Station</center></h3>
        <p>{cancellations_station}</p>
        </center>
    </body>
    </html>
    """
    with open("testing.html", "w") as f:
        f.write(html_content)
    return html_content


def convert_html_to_pdf(source_html: str, output_filename: str) -> bool:
    """Using the html provided, outputs a pdf as requested to be stored in s3"""
    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
        source_html,
        dest=result_file
    )

    result_file.close()

    return pisa_status.err


def create_report(data: pd.DataFrame):
    """
    Creates the pdf report in one function and 
    calls all functions required to do so
    """
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday.strftime("%d-%m-%Y")
    average = get_average_delays(data)
    total_services = data.groupby(
        'origin_station_name').size().reset_index(name='total_services')
    html_data = export_to_html(data, average, total_services)

    convert_html_to_pdf(html_data, f"daily_report_{yesterday_date}.pdf")


def get_average_delays(data_df: pd.DataFrame) -> pd.DataFrame:
    """Gets the average delays by company"""
    average_delays = data_df.groupby('company_name')[
        'arrival_lateness'].mean().reset_index()

    average_delays = average_delays.sort_values(
        by='arrival_lateness', ascending=False).head(20)

    return average_delays


def upload_to_s3_bucket(file_name):
    """Uploads the pdf to our s3 bucket as required."""
    amazon_s3 = client("s3", region_name="eu-west-2",
                       aws_access_key_id=environ["ACCESS_KEY_ID"],
                       aws_secret_access_key=environ["SECRET_ACCESS_KEY_ID"])

    amazon_s3.upload_file(
        f'/tmp/{file_name}',
        'disruption-detect-daily-reports',
        f'{file_name}')


def lambda_handler(event=None, context=None) -> dict:
    load_dotenv()
    connection = get_db_connection()
    data_df = get_data_from_database(connection)
    create_report(data_df)
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday.strftime("%d-%m-%Y")
    upload_to_s3_bucket(f"daily_report_{yesterday_date}.pdf")

    return {
        "statusCode": 200,
        "body": "Hello"
    }


if __name__ == "__main__":
    lambda_handler()
