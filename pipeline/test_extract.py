import os
from unittest.mock import patch, MagicMock
import datetime
import requests
from extract import (get_authentication, relevant_fields,
                     get_service_data_by_service, get_service_data_by_station,
                     create_download_folders, convert_to_csv, run_extract)


def test_get_authentication_returns_str():
    """Tests that get authentication returns required string"""
    assert isinstance(get_authentication('yes', 'simple'), str)


@patch('extract.requests.get')
def test_get_service_data_by_station(mock_get, darton_service_info):
    """Checks that by the station, it gets the service data required."""
    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = darton_service_info

    mock_get.return_value = fake_connection

    json_data = get_service_data_by_station(
        'DRT', datetime.date.today(), 'yes')

    assert json_data == darton_service_info


@patch('extract.requests.get')
def test_get_service_data_by_station_timed_out(mock_get):
    """Tests that the service data by station times out as expected"""
    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = {
        "error": "Timeout: The request could not be completed.", "Station": "DRT"}
    expected_result = {
        "error": "Timeout: The request could not be completed.", "Station": "DRT"}

    mock_get.return_value = fake_connection
    mock_get.side_effect = requests.exceptions.Timeout

    json_data = get_service_data_by_station(
        'DRT', datetime.date.today(), 'yes')

    assert json_data == expected_result


@patch('extract.requests.get')
def test_get_service_data_by_service(mock_get):
    """Tests that we can get service data by service"""
    fake_connection = MagicMock()
    fake_connection.status_code = 200
    fake_connection.json.return_value = {
        "service_uid": "H38443", "company_name": "Great Northern"}

    mock_get.return_value = fake_connection

    json_data = get_service_data_by_service(
        'P44650', datetime.date.today(), 'yes')

    assert json_data == {"service_uid": "H38443",
                         "company_name": "Great Northern"}


@patch('extract.requests.get')
def test_relevant_fields_returns_correct_type(mock_get_service_data, darton_service, darton_service_info):

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


"""
def test_run_extract()

def test_obtain_relevant_data_by_service()
"""


def test_create_download_folders():
    "Tests that the download folders get created."
    assert not os.path.exists("z_folder")
    with patch('os.path.exists') as mock_exists:

        mock_exists.return_value = False
        create_download_folders("z_folder")
    assert os.path.exists("z_folder")

    os.rmdir("z_folder")
    assert True == True


def test_convert_to_csv():
    """Tests that the list gets converted to a csv file"""
    assert not os.path.exists("unseen.csv")
    convert_to_csv(['NT', 'GWR'], 'unseen.csv')
    assert os.path.exists("unseen.csv")

    os.remove("unseen.csv")
    assert True == True
