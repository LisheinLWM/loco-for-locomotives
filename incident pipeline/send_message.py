from boto3 import client
from dotenv import dotenv_values, load_dotenv
from os import environ

load_dotenv()

config = {}

config["ACCESS_KEY_ID"] = environ.get("ACCESS_KEY_ID")
config["SECRET_ACCESS_KEY"] = environ.get("SECRET_ACCESS_KEY")

s3 = client("s3", aws_access_key_id=config["ACCESS_KEY_ID"],
                aws_secret_access_key=config["SECRET_ACCESS_KEY"])
    
sns = client("sns")

sns.publish(
        TopicArn='arn:aws:sns:eu-west-2:129033205317:rail-incidents-AW',
        Message='!!!! incident alert!!!! !!!! :(',
    )