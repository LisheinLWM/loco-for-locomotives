from os import environ
from boto3 import client
from boto3.resources.base import ServiceResource
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY_ID = environ["ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = environ["SECRET_ACCESS_KEY"]
AWS_REGION = environ["AWS_REGION"]

sns_client = client('sns', region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY_ID,
                    aws_secret_access_key=SECRET_ACCESS_KEY)

phone_number = input("""Please input your phone number, starting
                     with '+' and your area code: """)

operator_code = input("""Please input an operator code: """)

try:
    sns_client.subscribe(
        TopicArn=f"arn:aws:sns:eu-west-2:129033205317:rail-incidents-{operator_code}",
        Protocol="sms",
        Endpoint=phone_number,
        ReturnSubscriptionArn=True
    )
except Exception as e:
    print(f"An error occurred: {str(e)}")