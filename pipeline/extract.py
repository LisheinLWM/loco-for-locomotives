"""Extracts data from the Realtime Trains API and creates a CSV with the relevant data."""

import base64
from datetime import date, datetime, timedelta
import os
from os import environ
import time


import requests
from dotenv import load_dotenv
import pandas as pd


def get_authentication(username: str, password: str) -> str:
    """Returns the Base64 encoding of the credentials in the form username:password."""

    credentials = f"{username}:{password}"
    credentials_bytes = credentials.encode("UTF-8")
    authentication_bytes = base64.b64encode(credentials_bytes)
    authentication_string = authentication_bytes.decode("UTF-8")

    return authentication_string


def get_service_data_by_station(station_crs: str, service_date: date, authentication) -> dict:
    """Connects to the Realtime Trains API and returns a dictionary
    consisting of required data."""

    url = f"https://api.rtt.io/api/v1/json/search/{station_crs}/{service_date}"
    data = {
        "Authorization": f"Basic {authentication}"
    }

    try:
        response = requests.get(url, headers=data, timeout=10)
    except requests.exceptions.Timeout:
        return {
            "error": "Timeout: The request could not be completed.", "Station": station_crs}
        return response

    return response.json()


def get_service_data_by_service(service_uid: str, service_date: date, authentication) -> dict:
    """Connects to the Realtime Trains API and returns a dictionary
    consisting of required data."""

    url = f"https://api.rtt.io/api/v1/json/service/{service_uid}/{service_date}"
    data = {
        "Authorization": f"Basic {authentication}"
    }

    try:
        response = requests.get(url, headers=data, timeout=10)
    except requests.exceptions.Timeout:
        response = {
            "error": "Timeout: The request could not be completed.", "Service": service_uid}
        return response

    return response.json()


def relevant_fields(journey: dict, service: dict) -> dict:
    """Returns a dictionary containing the required information for each service."""

    arrival_lateness = None
    for location in reversed(service["locations"]):
        if location["displayAs"] == "TERMINATES" or location["displayAs"] == "DESTINATION":
            arrival_lateness = location.get(
                "realtimeGbttArrivalLateness", None)
            if arrival_lateness is None:
                arrival_lateness = 0
            reached_crs = location['crs']
        if location["displayAs"] == "CANCELLED_CALL":
            service_cancelled = location

    try:
        cancel_crs = service_cancelled["crs"]
        cancel_station = service_cancelled["description"]
        cancel_code = service_cancelled["cancelReasonCode"]
    except:
        cancel_crs = None
        cancel_station = None
        cancel_code = None

    if arrival_lateness is None:
        destination_reached_crs = service["locations"][0]["crs"]
        destination_reached_name = journey["locationDetail"]["origin"][0]["description"]
    else:
        destination_reached_crs = reached_crs
        destination_reached_name = journey["locationDetail"]["destination"][0]["description"]

    relevant_data = {
        "service_uid": service["serviceUid"],
        "company_name": service["atocName"],
        "service_type": service["serviceType"],
        "origin_crs": service["locations"][0]["crs"],
        "origin_stn_name": journey["locationDetail"]["origin"][0]["description"],
        "origin_run_time": journey["locationDetail"]["origin"][0]["workingTime"],
        "origin_run_date": journey["runDate"],
        "planned_final_destination": service["locations"][-1]["description"],
        "planned_final_crs": service["locations"][-1]["crs"],
        "destination_reached_crs": destination_reached_crs,
        "destination_reached_name": destination_reached_name,
        "scheduled_arrival_time": journey["locationDetail"]["destination"][0]["workingTime"],
        "scheduled_arrival_date": journey["runDate"],
        "arrival_lateness": arrival_lateness,
        "cancellation_station_crs": cancel_crs,
        "cancellation_station_name": cancel_station,
        "cancel_code": cancel_code
    }

    return relevant_data


def obtain_relevant_data_by_service(station_crs: str, service_date: date, authentication: str) -> list:
    """Returns a list of all the services for a single station on a given date."""

    list_of_services = []

    station_data = get_service_data_by_station(
        station_crs, service_date, authentication)

    for journey in station_data["services"]:

        try:
            service_uid = journey["serviceUid"]
            service = get_service_data_by_service(
                service_uid, service_date, authentication)
            data = relevant_fields(journey, service)
            list_of_services.append(data)
        except:
            print(service_uid, station_crs)

    return list_of_services


def convert_to_csv(list_of_services: list) -> None:
    """Takes in a list of services and creates a csv file with a row for each service."""

    dataframe = pd.DataFrame(list_of_services)
    csv_filename = "data/service_data.csv"
    dataframe.to_csv(csv_filename, index=False)


def create_download_folders() -> None:
    """Creates a folder with the name "data" if it doesn't already exist"""

    folder_exists = os.path.exists("data")
    if not folder_exists:
        os.makedirs("data")


def run_extract(authentication_realtime):

    stations = {
        "BRI": "Bristol Temple Meads",
        "WAT": "London Waterloo",
        "BHM": "Birmingham New Street",
        "NCL": "Newcastle",
        "YRK": "York",
        "MAN": "Manchester Piccadilly",
        "LIV": "Liverpool Lime Street",
        "LDS": "Leeds",
        "PAD": "London Paddington",
        "SHF": "Sheffield"
    }

    yesterday = datetime.now()-timedelta(days=1)
    yesterday_date = yesterday.strftime("%Y/%m/%d")

    start_time = time.time()
    print("Extracting...")

    create_download_folders()

    list_of_services = []
    for station_crs in stations.keys():
        services = obtain_relevant_data_by_service(
            station_crs, yesterday_date, authentication_realtime)
        list_of_services.extend(services)

    convert_to_csv(list_of_services)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total extraction time: {elapsed_time:.2f} seconds.")


if __name__ == "__main__":  # pragma: no cover

    load_dotenv()

    username_realtime = environ.get("RTA_USERNAME")
    password_realtime = environ.get("RTA_PASSWORD")
    authentication_realtime = get_authentication(
        username_realtime, password_realtime)

    run_extract(authentication_realtime)
