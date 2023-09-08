"""Test Script: Testing functions from transform.py"""

import pandas as pd
from pandas import DataFrame
import pytest

from transform import (
    load_data,
    create_timestamp_from_date_and_time,
    replace_non_integers_with_none,
    generate_list_of_valid_cancel_codes,
    determine_if_cancel_code_is_valid,
    check_values_in_column_have_three_characters
)


def test_data_is_loaded(generated_df):
    """Tests that the data is loaded correctly"""
    generated_df.to_csv('test_data.csv', index=False)
    result_1 = load_data('test_data.csv')
    assert isinstance(result_1, DataFrame)
    result_2 = load_data('non-existent-file.csv')
    assert result_2 is None


def test_create_timestamp_creates_new_column(generated_df):
    """
    Tests that create_timestamp_from_date_and_time()
    successfully adds the specified column to the 
    DataFrame
    """
    new_column_name = "datetime"
    df = create_timestamp_from_date_and_time(generated_df, new_column_name,
                                             "date_column", "time_column")
    assert new_column_name in df
    print(df.dtypes[new_column_name])


def test_create_timestamp_new_column_has_datetime_values(generated_df):
    """
    Tests that each value in the column
    created by create_timestamp_from_date_and_time()
    is a datetime value
    """
    df = create_timestamp_from_date_and_time(generated_df, "datetime",
                                             "date_column", "time_column")
    assert df.dtypes["datetime"] == "datetime64[ns]"


def test_create_timestamp_raises_error_if_invalid_values_given():
    """
    Tests that create_timestamp_from_date_and_time()
    handles cases where invalid values exist
    in the provided columns
    """
    # covers cases where an invalid value is in the date column
    data_1 = {"date_column": ["not-a-date", "2023-09-06"],
              "time_column": ["123045", "081530"]}
    df_1 = pd.DataFrame(data_1)
    df_1 = create_timestamp_from_date_and_time(df_1, "datetime",
                                               "date_column", "time_column")
    assert df_1 is None

    # covers cases where an invalid value is in the time column
    data_2 = {"date_column": ["2023-09-05", "2023-09-06"],
              "time_column": ["not-a-time", "081530"]}
    df_2 = pd.DataFrame(data_2)
    df_2 = create_timestamp_from_date_and_time(df_2, "datetime",
                                               "date_column", "time_column")

    assert df_2 is None


def test_create_timestamp_generates_correct_values(generated_df):
    """
    Tests that create_timestamp_from_date_and_time()
    is creating datetime objects in the new column,
    with the required format (YYYY-MM-DD HH:MM:SS)
    """
    df = create_timestamp_from_date_and_time(generated_df, "datetime",
                                             "date_column", "time_column")

    datetime_value = df.loc[0, "datetime"]
    assert datetime_value == pd.Timestamp("2023-09-05 12:30:45")
    assert isinstance(datetime_value, pd.Timestamp)


def test_cancel_code_list_generation(cancel_codes_df):
    """
    Tests whether generate_list_of_valid_cancel_codes()
    correctly processes html data and returns a list
    of codes
    """
    cancel_codes_df.to_html('mock_cancel_codes.html',
                            classes='wikitable', index=False, border=3, justify='center')
    valid_code_list = generate_list_of_valid_cancel_codes(
        'mock_cancel_codes.html')
    assert isinstance(valid_code_list, list)
    assert valid_code_list == ['AA', 'AC', 'AD', 'ZZ']


def test_valid_and_invalid_cancel_codes_are_processed_correctly(cancel_codes_df, generated_df):
    """Tests that the validity of codes is processed correctly"""
    cancel_codes_df.to_html('mock_cancel_codes.html',
                            classes='wikitable', index=False, border=3, justify='center')
    valid_code_list = generate_list_of_valid_cancel_codes(
        'mock_cancel_codes.html')
    service_df = generated_df
    service_df = determine_if_cancel_code_is_valid(service_df, valid_code_list)
    assert service_df['cancel_code'].tolist() == ["AA", None, "ZZ"]


def test_numbers_are_processed_correctly(generated_df):
    """Tests that numbers are processed correctly"""
    df = replace_non_integers_with_none(generated_df, "numbers")
    assert df["numbers"].tolist() == [12.0, -5.0, None]


def test_CRS_values_are_confirmed_to_be_three_characters(generated_df):
    """Tests that CRS values are 3 characters exactly"""
    df_1 = generated_df
    df_1 = check_values_in_column_have_three_characters(
        df_1, "crs", drop_row=True)
    assert df_1["crs"].tolist() == ["ABC"]
    df_2 = generated_df
    df_2 = check_values_in_column_have_three_characters(
        df_2, "crs", drop_row=False)
    assert df_2["crs"].tolist() == ["ABC", None, None]
