FROM python

WORKDIR app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY extract_incident_data.py .

COPY load_incident_data.py .

COPY messages.py .

COPY opendata-nationalrail-client.py .

CMD ["python", "opendata-nationalrail-client.py"]