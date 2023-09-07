from extract import (get_authentication, relevant_fields,
                     get_service_data_by_service, get_service_data_by_station)
from unittest.mock import patch, MagicMock
import datetime
import requests

darton_service_info = {
    "serviceUid": "P44650",
    "runDate": "2023-09-06",
    "serviceType": "train",
    "isPassenger": True,
    "trainIdentity": "2L69",
    "powerType": "DMU",
    "trainClass": "S",
    "atocCode": "NT",
    "atocName": "Northern",
    "performanceMonitored": True,
    "origin": [
        {
            "tiploc": "LEEDS",
            "description": "Leeds",
            "workingTime": "143200",
            "publicTime": "1432"
        }
    ],
    "destination": [
        {
            "tiploc": "SHEFFLD",
            "description": "Sheffield",
            "workingTime": "155100",
            "publicTime": "1551"
        }
    ],
    "locations": [
        {
            "realtimeActivated": True,
            "tiploc": "LEEDS",
            "crs": "LDS",
            "description": "Leeds",
            "gbttBookedDeparture": "1432",
            "origin": [
                {
                    "tiploc": "LEEDS",
                    "description": "Leeds",
                    "workingTime": "143200",
                    "publicTime": "1432"
                }
            ],
            "destination": [
                {
                    "tiploc": "SHEFFLD",
                    "description": "Sheffield",
                    "workingTime": "155100",
                    "publicTime": "1551"
                }
            ],
            "isCall": True,
            "isPublicCall": True,
            "realtimeDeparture": "1432",
            "realtimeDepartureActual": True,
            "platform": "17",
            "platformConfirmed": True,
            "platformChanged": False,
            "line": "F",
            "lineConfirmed": True,
            "displayAs": "ORIGIN",
            "associations": [
                {
                    "type": "next",
                    "associatedUid": "P44602",
                    "associatedRunDate": "2023-09-06"
                }
            ]
        }
    ]
}

darton_service = {
    "locationDetail": {
        "realtimeActivated": True,
        "tiploc": "DRTN",
        "crs": "DRT",
        "description": "Darton",
        "gbttBookedArrival": "1514",
        "gbttBookedDeparture": "1514",
        "origin": [
            {
                "tiploc": "LEEDS",
                "description": "Leeds",
                "workingTime": "143200",
                "publicTime": "1432"
            }
        ],
        "destination": [
            {
                "tiploc": "SHEFFLD",
                "description": "Sheffield",
                "workingTime": "155100",
                "publicTime": "1551"
            }
        ],
        "isCall": True,
        "isPublicCall": True,
        "realtimeArrival": "1514",
        "realtimeArrivalActual": False,
        "realtimeDeparture": "1515",
        "realtimeDepartureActual": False,
        "displayAs": "CALL"
    },
    "serviceUid": "P44650",
    "runDate": "2023-09-06",
    "trainIdentity": "2L69",
    "runningIdentity": "2L69",
    "atocCode": "NT",
    "atocName": "Northern",
    "serviceType": "train",
    "isPassenger": True
}


def test_get_authentication_returns_str():
    assert isinstance(get_authentication('yes', 'simple'), str)


@patch('extract.requests.get')
def test_get_service_data_by_station(mock_get):
    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = {
        "crs": "BRI", "station": "Bristol Temple Meads"}

    mock_get.return_value = fake_connection

    json_data = get_service_data_by_station(
        'BRI', datetime.date.today(), 'yes')

    assert json_data == {"crs": "BRI", "station": "Bristol Temple Meads"}


@patch('extract.requests.get')
def test_get_service_data_by_station_timed_out(mock_get):
    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = {
        "error": "Timeout: The request could not be completed.", "Station": "BRI"}
    expected_result = {
        "error": "Timeout: The request could not be completed.", "Station": "BRI"}

    mock_get.return_value = fake_connection
    mock_get.side_effect = requests.exceptions.Timeout

    json_data = get_service_data_by_station(
        'BRI', datetime.date.today(), 'yes')

    assert json_data == expected_result


@patch('extract.requests.get')
def test_get_service_data_by_service(mock_get):
    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = {
        "service_uid": "H38443", "company_name": "Great Northern"}

    mock_get.return_value = fake_connection

    json_data = get_service_data_by_service(
        'H38443', datetime.date.today(), 'yes')

    assert json_data == {"service_uid": "H38443",
                         "company_name": "Great Northern"}


@patch('extract.requests.get')
def test_relevant_fields_returns_correct_type(mock_get_service_data):

    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = darton_service_info
    mock_get_service_data.return_value = fake_connection
    journey = darton_service
    service_uid = "P44650"
    service = get_service_data_by_service(service_uid, "2023-09-06", 'yes')
    print(service)
    print("\n")
    print(service["locations"])
    assert isinstance(relevant_fields(journey, service), dict)
