from pytest import fixture
import pandas as pd


@fixture
def darton_service():
    """A fixture to return data on the darton journey data"""
    return {
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


@fixture
def darton_service_info():
    """
    A fixture to show the service info for Darton
    as an example of a service
    """
    return {
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


@fixture
def cancel_codes_df():
    """
    A fixture that returns a dataframe providing
    a fake cancel codes dataframe
    """
    return pd.DataFrame({'Code': ['AA', 'AC', 'AD', 'ZZ'],
                         'Cause': ['text1', 'text2', 'text3', 'text4'],
                         'Abbreviation': ['ACCEPTANCE', 'TRAIN PREP', 'WTG STAFF', 'SYS LIR']})


@fixture
def generated_df():
    """
    Generates a DataFrame with a column of
    date strings and a column of time strings
    """
    data = {"date_column": ["2023-09-05", "2023-09-06", "2023-09-08"],
            "time_column": ["123045", "081530", "153024"],
            "cancel_code": ["AA", "tabbycat", "ZZ"],
            "numbers": [12, -5, "cancelled at origin"],
            "crs": ["ABC", 0, "FUDGE"]}
    return pd.DataFrame(data)
