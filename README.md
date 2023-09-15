## DISRUPTION DETECT

# SETUP

**Credentials**

You will need credentials for 2 data sources:

- RealTime Trains API
- National Rail Incidents

**Database**

A RDS database is required containing 2 schemas for the two pipelines, these can be created by running the SQL schemas files that are found in the corresponding directories for the Incident and Service Pipeline.

**Environment Variables:**

Please ensure that a environment file is available containing the required information as below:

_RealTime Trains API_
RTA_USERNAME=######
RTA_PASSWORD=######

_DATABASE CREDENTIALS_
DB_HOST=######
DB_PASS=######
DB_NAME=######
DB_USER=######

_National Rail Incident Credentials_
USERNAME=######
PASSWORD=######
HOSTNAME=######
HOSTPORT=######
ACCESS_KEY_ID=######
SECRET_ACCESS_KEY=######

**Prerequisites**

Please install all required libraries found in the "requirements.txt" file by using the command below:

pip install -r requirements.txt

# Services Pipeline

The services pipeline extracts, transforms and loads data that is extracted using the RealTime Trains API.

The data is loaded to an RDS database, and visualised with the use of streamlit (last 24hr data) and Tableau (historic data).

**Extract**

The extract script obtains data of services originating at or passing through any station of a given choice for a given date.
Firstly the endpoint for the station is reached using the link below:

-"https://api.rtt.io/api/v1/json/search/{station_crs}/{service_date}"

This endpoint returns data regarding the various services that originates at or passes through this stations which included the service unique ID.

The unique ID can then be used to reach the second endpoint which provides vital information about each service such as the origin and destination of the service, the service type, all the calling points of the service, as well as the booked arrival/departure time and the actual arrival/departure times.
Information regarding wether a service was cancelled at a station is also included.

-"https://api.rtt.io/api/v1/json/service/{service_uid}/{service_date}"

Data is collected on the following fields:

service_uid,company_name,service_type,origin_crs,origin_stn_name,origin_run_time,origin_run_date,planned_final_destination,planned_final_crs,destination_reached_crs,destination_reached_name,scheduled_arrival_time, arrival_lateness,cancellation_station_crs,cancellation_station_name,cancel_code

This is then output as a CSV file containing a row for each service.

**Transform**

**Load**

The load script uses the cleaned csv file output from the transform script as well as a csv file of all cancel codes and reasons to populate the database.

The database ERD can be seen below:

**Streamlit**

**AWS Services Pipeline**

The service pipeline makes use of various AWS resources to ensure smooth and consistent deployment on the cloud. A task is scheduled to run daily at 01:30:00 which triggers the pipeline to extract, transform and load all service data of the prior day.

Please refer to the Terraform section for more information on the various resources created and used.

# Incidents Pipeline

The incidents pipeline listens to a realtime stream of incident information release by National Rail. On receiving this data, the pipeline extracts, transforms and loads this data into the incident_data schema in the RDS database. A text message is also sent out to subscribers using the AWS SNS. All captured incident data is visualised through Streamlit.

**Streamlit**
