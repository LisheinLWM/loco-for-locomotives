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

### config.toml
- Contains the theme for customising streamlit color scheme by changing the hex codes.

### Screenshot of the dahboard

Station Page:

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/9da2bd53-958c-428a-84a6-933ad2940347">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/47a81f6b-6f95-4b0a-9dd1-895f318318cc">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/b6fa51f2-f3d8-424d-9475-e8575854b611">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/da7ff682-a91f-49c6-9918-982d64ae313a">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/a859019a-cc33-4cc6-bcbc-3bf01b4f2db0">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/d074c41f-b0f8-42d1-89f6-65c66909d1e1">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/e358a916-cac5-45c9-832c-8c32aa328874">

Company Page:

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/68f1f5c4-de9d-4396-9e1d-8917d9d4a270">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/2160937b-9039-4eae-9181-e19b962941c5">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/309628b5-22dc-4c13-9616-f9356140fb6f">

<img width="1440" alt="image" src="https://github.com/LisheinLWM/loco-for-locomotives/assets/112435983/7c444165-6767-4b60-9e5b-52b118ba943c">
















