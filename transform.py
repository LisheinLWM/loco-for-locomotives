"""Pipeline Script: Transforming pipeline data"""

from datetime import datetime
import pandas as pd
from pandas import DataFrame


def load_data(csv_path: str) -> DataFrame:
    """
    Load data from a .csv file and
    return the data as a DataFrame
    """
    try:
        data = pd.read_csv(csv_path)
        return data

    except FileNotFoundError:
        print("Error loading .csv data: FileNotFound")
        return None


def hhmmss_to_timestamp(time_string: str):
    """
    Takes a 'time' string in the form
    'HHMMSS' and returns a valid timestamp
    """
    try:
        hh_value = int(time_string[:2])
        mm_value = int(time_string[2:4])
        ss_value = int(time_string[4:])
        datetime_value = datetime.now().replace(hour=hh_value, minute=mm_value,
                                                second=ss_value, microsecond=0)
        timestamp = datetime_value.timestamp()
        return timestamp

    except ValueError:
        print("Valid 'HHMMSS' time string must be provided")
        return None


def create_timestamp_from_date_and_time(df: DataFrame, new_column_name: str,
                                         date_column_name: str, time_column_name: str) -> DataFrame:
    """
    Takes a DataFrame, the name of a column
    containing dates, and the name of a column
    containing times, and creates a new column
    with datetime timestamps
    """
    try:
        df[date_column_name] = pd.to_datetime(df[date_column_name], format='%Y-%m-%d')

    except ValueError:
        print("Error: invalid values in date column")
        return None

    try:
        df[time_column_name] = pd.to_datetime(df[time_column_name], format='%H%M%S')

    except ValueError:
        print("Error: invalid values in time column")
        return None

    df[new_column_name] = df[date_column_name] + pd.to_timedelta(
        df[time_column_name].dt.strftime('%H:%M:%S'))
    return df


if __name__ == "__main__":

    input_csv_path = "service_data.csv"

    service_df = load_data(input_csv_path)

    print(service_df)

    service_df = create_timestamp_from_date_and_time(service_df,
                                                     "scheduled_arrival_datetime",
                                                     "scheduled_arrival_date",
                                                     "scheduled_arrival_time")

    service_df = create_timestamp_from_date_and_time(service_df,
                                                     "origin_run_datetime",
                                                     "origin_run_date",
                                                     "origin_run_time")

    output_csv_path = "transformed_service_data.csv"

    service_df.to_csv(output_csv_path)
