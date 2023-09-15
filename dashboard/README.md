# Dashboard

This folder contains all code and resources required to query from the 
services schema and load it into the dashboard for visualisations.

## Configure environment

```sh
python3 -m venv venv
source ./venv/bin/activate
pip3 install -r requirements.txt
```

## Configure environment variables

The following environment variables must be supplied in a `.env` file.

`ACCESS_KEY_ID`
`SECRET_ACCESS_KEY`
`DATABASE NAME`
`DATABASE USERNAME`
`DATABASE PASSWORD`
`DATABASE_IP`
`DATABASE_PORT`

## Configure custom theme
- In config.toml
- primaryColor=""
- backgroundColor=""
- secondaryBackgroundColor=""
- textColor=
- font=""


## Run the code

To run the streamlit_app.py
- Run `streamlit config show` which sets the custom theme for streamlit.
- Run `streamlit run dashboard.py`


# Docker image

Build the docker image

```sh
docker build -t services-dashboard . --platform "linux/amd64"
```

Run the docker image locally

```sh
docker run -it --env-file .env -p 8501:8501 services-dashboard 
```

## Files

### streamlit_app.py

- Contains all code and resources required for the dashboard.
- Use the dropdown to switch between the two pages: station page and company page.
- Station page contains the visualisations for the station and they can be filtered by station.
- Company page contains the visualisations for the companies and they can be filtered by company.



