FROM python

WORKDIR app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY .streamlit ./.streamlit

COPY subscribe_to_topic.py .

COPY incident_dashboard.py .

CMD ["streamlit", "run", "incident_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]