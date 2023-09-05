"""Test Script: Testing functions from transform.py"""

import pandas as pd
from transform import (
    create_timestamp_from_date_and_time
)

def test_create_timestamp_creates_new_column():
    data = {"date_column": ["2023-09-05", "2023-09-06"],
            "time_column": ["123045", "081530"]}
    df = pd.DataFrame(data) 
    new_column_name = "datetime"
    df = create_timestamp_from_date_and_time(df, new_column_name,
                                             "date_column", "time_column")
    assert new_column_name in df
    print(df.dtypes[new_column_name])


def test_create_timestamp_new_column_has_datetime_values():
    data = {"date_column": ["2023-09-05", "2023-09-06"],
            "time_column": ["123045", "081530"]}
    df = pd.DataFrame(data) 
    new_column_name = "datetime"
    df = create_timestamp_from_date_and_time(df, new_column_name,
                                             "date_column", "time_column")
    assert df.dtypes[new_column_name] == "datetime64[ns]"


test_create_timestamp_creates_new_column()