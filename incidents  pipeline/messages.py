"""Sends messages to SNs"""

from boto3 import client
from boto3.resources.base import ServiceResource


def send_incident_notification(message_data: dict, sns: ServiceResource):
    """
    Accepts a dictionary containing incident data
    and sends it to the SNS topic(s) corresponding
    to the operator code(s), resulting in SMS
    notifications
    """
    for operator in message_data["operators_affected"]:
        operator_code = operator["affected_operator_ref"]
        operator_name = operator["affected_operator_name"]
        priority = message_data["incident_priority"]
        summary = message_data["summary"]
        start_time = message_data["start_time"]
        end_time = message_data["end_time"]
        routes_affected = message_data["routes_affected"]
        if operator_code in ['LO', 'VT', 'CC', 'CS', 'CH', 'XC',
                             'EM', 'XR', 'ES', 'GC', 'LE', 'GW',
                             'HX', 'HT', 'GR', 'LD', 'ME', 'NT',
                             'SR', 'SE', 'TP', 'AW', 'LM', 'GX',
                             'GN', 'SN', 'TL', 'SW', 'IL']:
            text_msg = f"\n\n\U0001F684 {operator_name} incident \U0001F684:"
            text_msg += f"\n\nSummary: {summary}"
            text_msg += f"\n\nPriority: {priority}"
            text_msg += f"\n\nRoutes affected: {routes_affected}"
            if start_time and end_time:
                text_msg += f"\n\nDuration: {start_time} to {end_time}"
            elif start_time:
                text_msg += f"\n\nStart time: {start_time} End time: Unknown"
            sns.publish(
                TopicArn=f"arn:aws:sns:eu-west-2:129033205317:rail-incidents-{operator_code}",
                Message=f"{text_msg}"
            )
