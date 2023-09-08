import os
from os import environ
from dotenv import load_dotenv

from extract import get_authentication, run_extract
from transform import run_transform
from load import get_connection, run_load


if __name__ == "__main__":

    load_dotenv()

    username_realtime = environ.get("RTA_USERNAME")
    password_realtime = environ.get("RTA_PASSWORD")
    authentication_realtime = get_authentication(
        username_realtime, password_realtime)

    run_extract(authentication_realtime)

    input_csv_path = "data/service_data.csv"
    run_transform(input_csv_path)

    conn = get_connection(os.environ["DB_HOST"], os.environ["DB_NAME"],
                          os.environ["DB_PASS"], os.environ["DB_USER"])
    run_load(conn)
