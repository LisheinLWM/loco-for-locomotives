"""Extracts data from the Realtime Trains API and creates a CSV with the relevant data."""

import base64
from os import environ
from datetime import date

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
        response = {
            "error": "Timeout: The request could not be completed.", "Station": station_crs}

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

    if arrival_lateness == "CANCELLED AT ORIGIN":
        destination_reached_crs = journey["locationDetail"]["origin"][0]["tiploc"][:3]
        destination_reached_name = journey["locationDetail"]["origin"][0]["description"]
    else:
        destination_reached_crs = journey["locationDetail"]["destination"][0]["tiploc"][:3]
        destination_reached_name = journey["locationDetail"]["destination"][0]["description"]

    relevant_data = {
        "service_uid": service["serviceUid"],
        "company_name": service["atocName"],
        "service_type": service["serviceType"],
        "origin_crs": journey["locationDetail"]["origin"][0]["tiploc"][:3],
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

        service_uid = journey["serviceUid"]
        service = get_service_data_by_service(
            service_uid, service_date, authentication)
        data = relevant_fields(journey, service)
        list_of_services.append(data)

    return list_of_services


def convert_to_csv(list_of_services: list) -> None:
    """Takes in a list of services and creates a csv file with a row for each service."""

    dataframe = pd.DataFrame(list_of_services)
    csv_filename = "service_data.csv"
    dataframe.to_csv(csv_filename, index=False)


if __name__ == "__main__":

    load_dotenv()

    username_realtime = environ.get("RTA_USERNAME")
    password_realtime = environ.get("RTA_PASSWORD")
    authentication_realtime = get_authentication(
        username_realtime, password_realtime)

    CRS = "MAN"
    DATE_OF_SERVICE = "2023/09/03"

    services = obtain_relevant_data_by_service(
        CRS, DATE_OF_SERVICE, authentication_realtime)
    convert_to_csv(services)
