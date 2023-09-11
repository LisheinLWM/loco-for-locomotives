import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
from datetime import datetime
import pandas as pd
from pandas import DataFrame

input_string = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><uk.co.nationalrail.xml.incident.PtIncidentStructure xmlns:ns2="http://nationalrail.co.uk/xml/common" xmlns:ns3="http://nationalrail.co.uk/xml/incident"><ns3:CreationTime>2023-07-05T09:22:41.548Z</ns3:CreationTime><ns3:ChangeHistory><ns2:ChangedBy>NRE CMS Editor</ns2:ChangedBy><ns2:LastChangedDate>2023-09-07T09:55:48.591Z</ns2:LastChangedDate></ns3:ChangeHistory><ns3:IncidentNumber>A0E301660F024ACB84C19FB5BBFF6152</ns3:IncidentNumber><ns3:Version>20230907095548</ns3:Version><ns3:ValidityPeriod><ns2:StartTime>2023-09-10T00:00:00.000+01:00</ns2:StartTime><ns2:EndTime>2023-09-10T23:59:00.000+01:00</ns2:EndTime></ns3:ValidityPeriod><ns3:Planned>true</ns3:Planned><ns3:Summary>Buses replace morning trains between Broxbourne and Stansted Airport / Audley End on Sunday 10 September</ns3:Summary><ns3:Description>&lt;p&gt;&lt;a href="https://www.nationalrail.co.uk/travel-information/engineering-works-explained/" title=""&gt;Engineering work&lt;/a&gt; is taking place between Broxbourne and Audley End, closing all lines before 11:10 on Sunday morning.&lt;/p&gt;&lt;p&gt;Before 11:10, buses will replace trains between:&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Broxbourne and Stansted Airport (direct for Stansted Express customers).&lt;/li&gt;&lt;li&gt;Broxbourne and Audley End via Stansted Airport.&lt;/li&gt;&lt;/ul&gt;&lt;p&gt;An amended train service will run between London Liverpool Street and Broxbourne, and between Audley End and Cambridge.&lt;/p&gt;&lt;p&gt;&lt;strong&gt;Check before you travel: &lt;/strong&gt;&lt;/p&gt;&lt;p&gt;You can plan your journey using the National Rail Enquiries &lt;a href="http://ojp.nationalrail.co.uk/" title=""&gt;Journey Planner&lt;/a&gt;&lt;/p&gt;&lt;p&gt;&lt;strong&gt;Replacement Bus Travel Advice: &lt;/strong&gt;&lt;/p&gt;&lt;p&gt;For helpful advice if you need to travel on a rail replacement service, including accessibility and bicycle information, please use &lt;a href="https://www.nationalrail.co.uk/travel-information/rail-replacement-services/" title=""&gt;this page&lt;/a&gt;. &lt;/p&gt;&lt;p&gt;You can find the location of your bus replacement by checking station signs or by searching for your station on our station &lt;a href="http://www.nationalrail.co.uk/stations_destinations/default.aspx" title=""&gt;information pages&lt;/a&gt;. &lt;/p&gt;&lt;p&gt;Please be advised that, on occasion, replacement vehicles may be busier than usual, and you should allow extra time for your journey.&lt;/p&gt;</ns3:Description><ns3:InfoLinks><ns3:InfoLink><ns3:Uri>https://www.nationalrail.co.uk/engineering-works/broxbourne-stansted-airport-audley-end-20230910/</ns3:Uri><ns3:Label>Incident detail page</ns3:Label></ns3:InfoLink></ns3:InfoLinks><ns3:Affects><ns3:Operators><ns3:AffectedOperator><ns3:OperatorRef>LE</ns3:OperatorRef><ns3:OperatorName>Greater Anglia</ns3:OperatorName></ns3:AffectedOperator><ns3:AffectedOperator><ns3:OperatorRef>SX</ns3:OperatorRef><ns3:OperatorName>Stansted Express</ns3:OperatorName></ns3:AffectedOperator></ns3:Operators><ns3:RoutesAffected>&lt;p&gt;Greater Anglia / Stansted Express between London Liverpool Street and Stansted Airport / Cambridge / Cambridge North&lt;/p&gt;</ns3:RoutesAffected></ns3:Affects><ns3:IncidentPriority>2</ns3:IncidentPriority></uk.co.nationalrail.xml.incident.PtIncidentStructure>"""
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
    incident_data = {
        "creation_time": root.find('.//ns3:CreationTime', namespaces).text,
        "incident_number": root.find('.//ns3:IncidentNumber', namespaces).text,
        "version": root.find('.//ns3:Version', namespaces).text,
        "planned": root.find('.//ns3:Planned', namespaces).text,
        "start_time": root.find('.//ns2:StartTime', namespaces).text,
        "end_time": root.find('.//ns2:EndTime', namespaces).text,
        "info_link": root.find('.//ns3:Uri', namespaces).text,
        "summary": root.find('.//ns3:Summary', namespaces).text,
        "incident_priority": root.find('.//ns3:IncidentPriority', namespaces).text,
        "operators_affected": extract_operators_from_element(root, namespaces),
        "routes_affected": root.find('.//ns3:RoutesAffected', namespaces).text
    }

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
        except ValueError:
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


transformed_incident_data = extract_and_transform_incident_data(
    input_string, namespaces)
flattened_data = flatten_incident_data(transformed_incident_data)
print(transformed_incident_data)
df = pd.DataFrame(flattened_data)
print(df)
