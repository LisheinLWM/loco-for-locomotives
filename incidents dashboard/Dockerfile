FROM python

WORKDIR dashboard

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY .streamlit ./.streamlit

COPY pages ./pages

COPY key_metrics.py .

CMD ["streamlit", "run", "key_metrics.py", "--server.port=8501", "--server.address=0.0.0.0"]