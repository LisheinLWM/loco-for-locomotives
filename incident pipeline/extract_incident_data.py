import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
from datetime import datetime
import pandas as pd
from pandas import DataFrame


namespaces = {
    'ns2': 'http://nationalrail.co.uk/xml/common',
    'ns3': 'http://nationalrail.co.uk/xml/incident'
}


def convert_timestamp(timestamp_str: str) -> datetime:
    """
    Takes a timezone-inclusive timestamp
    and formats it as YYYY-MM-DD HH:MM:SS
    """
    timestamp = datetime.fromisoformat(timestamp_str)
    formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_timestamp


def parse_xml_string(xml_data: str) -> Element:
    """
    Takes a string and returns the data
    as an XML element
    """
    root = ET.fromstring(xml_data)
    return root


def extract_operators_from_element(root: Element, namespaces: dict) -> list:
    """
    Takes an XML element and returns a list
    with details for each 'operator' found
    """
    operators = root.findall(".//ns3:Operators", namespaces)
    operators_info = []

    for operator in operators:
        operator_info = {
            "affected_operator_ref": root.find('.//ns3:OperatorRef', namespaces).text,
            "affected_operator_name": root.find('.//ns3:OperatorName', namespaces).text
        }
        operators_info.append(operator_info)

    return operators_info


def extract_incident_details(root: Element, namespaces: dict) -> dict:
    """
    Takes an XML element and returns a
    dictionary containing incident details
    """
    creation_time = root.find('.//ns3:CreationTime', namespaces)
    incident_num = root.find('.//ns3:IncidentNumber', namespaces)
    version = root.find('.//ns3:Version', namespaces)
    planned = root.find('.//ns3:Planned', namespaces)
    start_time = root.find('.//ns2:StartTime', namespaces)
    end_time = root.find('.//ns2:EndTime', namespaces)
    info_link = root.find('.//ns3:Uri', namespaces)
    summary = root.find('.//ns3:Summary', namespaces)
    incident_priority = root.find('.//ns3:IncidentPriority', namespaces)
    routes = root.find('.//ns3:RoutesAffected', namespaces)

    incident_data = {
        "creation_time": creation_time.text if creation_time is not None else None,
        "incident_number": incident_num.text if incident_num is not None else None,
        "version": version.text if version is not None else None,
        "planned": planned.text if planned is not None else None,
        "start_time": start_time.text if start_time is not None else None,
        "end_time": end_time.text if end_time is not None else None,
        "info_link": info_link.text if info_link is not None else None,
        "summary": summary.text if summary is not None else None,
        "incident_priority": incident_priority.text if incident_priority is not None else None,
        "operators_affected": extract_operators_from_element(root, namespaces),
        "routes_affected": routes.text if routes is not None else None
    }
    print(incident_data)
    return incident_data


def transform_incident_data(incident_data: dict) -> dict:
    """
    Processes the incident data dictionary,
    ensuring correct formats and data types
    """
    for timestamp_key in ["creation_time", "start_time", "end_time"]:
        try:
            incident_data[timestamp_key] = convert_timestamp(
                incident_data[timestamp_key])
        except:
            incident_data[timestamp_key] = None

    try:
        incident_data["planned"] = bool(incident_data["planned"])
    except ValueError:
        incident_data["planned"] = None

    try:
        incident_data["incident_priority"] = int(
            incident_data["incident_priority"])
    except ValueError:
        incident_data["incident_priority"] = None

    try:
        routes_affected = incident_data["routes_affected"]
        if isinstance(routes_affected, str):
            routes_affected = routes_affected.replace(
                "<p>", "").replace("</p>", "")
            incident_data["routes_affected"] = routes_affected.split(" / ")
    except (ValueError, AttributeError):
        incident_data["routes_affected"] = None

    return incident_data


def extract_and_transform_incident_data(input_string: str, namespaces: dict):
    """
    Calls the functions which parse, extract,
    and transform an incoming incident message,
    returning the transformed message
    """
    root = parse_xml_string(input_string)
    incident_data = extract_incident_details(root, namespaces)
    transformed_incident_data = transform_incident_data(incident_data)

    return transformed_incident_data


def flatten_incident_data(incident_data: dict) -> DataFrame:
    """
    Flattens the incident data dictionary into
    a list of dictionaries, each one containing
    only one operator and one affected route
    """
    flattened_data = []

    for operator in incident_data['operators_affected']:
        for route in incident_data['routes_affected']:
            flattened_dict = {
                "creation_time": incident_data['creation_time'],
                "incident_number": incident_data['incident_number'],
                "version": incident_data['version'],
                "planned": incident_data['planned'],
                "start_time": incident_data['start_time'],
                "end_time": incident_data['end_time'],
                "info_link": incident_data['info_link'],
                "summary": incident_data['summary'],
                "incident_priority": incident_data['incident_priority'],
                "affected_operator_ref": operator['affected_operator_ref'],
                "affected_operator_name": operator['affected_operator_name'],
                "route_affected": route
            }
            flattened_data.append(flattened_dict)

    return flattened_data
