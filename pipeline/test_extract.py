from extract import (get_authentication, relevant_fields,
                     get_service_data_by_service, get_service_data_by_station)
from unittest.mock import patch, MagicMock
import datetime


def test_get_authentication_returns_str():
    assert isinstance(get_authentication('harold', 'potato'), str)


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


def test_relevant_fields():
    pass
