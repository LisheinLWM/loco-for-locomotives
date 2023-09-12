"""insert docstring here"""

#
# National Rail Open Data client demonstrator
# Copyright (C)2019-2022 OpenTrainTimes Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import os
import time
import socket
import logging
import xml.etree.ElementTree as ET

import boto3
from boto3 import client
import pandas as pd
import stomp
from dotenv import load_dotenv

from extract_incident_data import (
    extract_and_transform_incident_data,
    flatten_incident_data
)
from load_incident_data import load_all_incidents
from messages import send_incident_notification


def connect_and_subscribe(connection, USERNAME, PASSWORD, CLIENT_ID, TOPIC):
    """insert docstring here"""
    if stomp.__version__[0] < 5:
        connection.start()

    connect_header = {'client-id': USERNAME + '-' + CLIENT_ID}
    subscribe_header = {'activemq.subscriptionName': CLIENT_ID}

    connection.connect(username=USERNAME,
                       passcode=PASSWORD,
                       wait=True,
                       headers=connect_header)

    connection.subscribe(destination=TOPIC,
                         id='1',
                         ack='auto',
                         headers=subscribe_header)


class StompClient(stomp.ConnectionListener):

    def on_heartbeat(self):
        logging.info('Received a heartbeat')

    def on_heartbeat_timeout(self):
        logging.error('Heartbeat timeout')

    def on_error(self, headers, message):
        logging.error(message)

    def on_disconnected(self):
        logging.warning(
            'Disconnected - waiting %s seconds before exiting' % RECONNECT_DELAY_SECS)
        time.sleep(RECONNECT_DELAY_SECS)
        connect_and_subscribe(self.conn)

    def on_connecting(self, host_and_port):
        logging.info('Connecting to ' + host_and_port[0])

    def on_message(self, frame):
        try:
            message_data = extract_and_transform_incident_data(
                frame.body.decode(), namespaces)
            send_incident_notification(message_data, sns)
            flattened_msg = flatten_incident_data(message_data)
            msg_df = pd.DataFrame(flattened_msg)
            load_all_incidents(msg_df)
            logging.info("Incident recorded.")
        except Exception as e:
            logging.error(str(e))


if __name__ == "__main__":

    logging.basicConfig(
        format='%(asctime)s %(levelname)s\t%(message)s', level=logging.INFO)

    load_dotenv()

    ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
    SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]

    s3 = client("s3", aws_access_key_id=os.environ["ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"])
    boto3.setup_default_session(region_name='eu-west-2')
    sns = client("sns", aws_access_key_id=os.environ["ACCESS_KEY_ID"],
                 aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"])

    USERNAME = os.environ["USERNAME"]
    PASSWORD = os.environ["PASSWORD"]
    HOSTNAME = os.environ["HOSTNAME"]
    HOSTPORT = os.environ["HOSTPORT"]

    TOPIC = '/topic/kb.incidents'

    CLIENT_ID = socket.getfqdn()
    HEARTBEAT_INTERVAL_MS = 30000
    HEARTBEAT_RESPONSE_TIMEOUT = 25000
    RECONNECT_DELAY_SECS = 15

    namespaces = {
        'ns2': 'http://nationalrail.co.uk/xml/common',
        'ns3': 'http://nationalrail.co.uk/xml/incident'
    }

    if USERNAME == '':
        logging.error(
            "Username not set - please configure your username\
                and password in opendata-nationalrail-client.py!")

    conn = stomp.Connection12([(HOSTNAME, HOSTPORT)],
                              auto_decode=False,
                              )

    client = StompClient()
    client.conn = conn
    conn.set_listener('', client)
    connect_and_subscribe(conn, USERNAME, PASSWORD, CLIENT_ID, TOPIC)

    while True:
        time.sleep(1)

    conn.disconnect()
