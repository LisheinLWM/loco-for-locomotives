"""Test Script: Testing functions from transform.py"""

import pandas as pd
import pytest

from transform import (
    create_timestamp_from_date_and_time,
    replace_non_integers_with_none
)


def generate_test_dataframe_with_date_and_time_columns():
    """
    Generates a DataFrame with a column of
    date strings and a column of time strings
    """
    data = {"date_column": ["2023-09-05", "2023-09-06"],
            "time_column": ["123045", "081530"]}
    return pd.DataFrame(data)


def test_create_timestamp_creates_new_column():
    """
    Tests that create_timestamp_from_date_and_time()
    successfully adds the specified column to the 
    DataFrame
    """
    df = generate_test_dataframe_with_date_and_time_columns()
    new_column_name = "datetime"
    df = create_timestamp_from_date_and_time(df, new_column_name,
                                             "date_column", "time_column")
    assert new_column_name in df
    print(df.dtypes[new_column_name])


def test_create_timestamp_new_column_has_datetime_values():
    """
    Tests that each value in the column
    created by create_timestamp_from_date_and_time()
    is a datetime value
    """
    df = generate_test_dataframe_with_date_and_time_columns()
    df = create_timestamp_from_date_and_time(df, "datetime",
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


def test_create_timestamp_generates_correct_values():
    """
    Tests that create_timestamp_from_date_and_time()
    is creating datetime objects in the new column,
    with the required format (YYYY-MM-DD HH:MM:SS)
    """
    df = generate_test_dataframe_with_date_and_time_columns()
    df = create_timestamp_from_date_and_time(df, "datetime",
                                             "date_column", "time_column")

    datetime_value = df.loc[0, "datetime"]
    assert datetime_value == pd.Timestamp("2023-09-05 12:30:45")
    assert isinstance(datetime_value, pd.Timestamp)


def test_