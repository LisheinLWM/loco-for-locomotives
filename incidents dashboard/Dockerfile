FROM python

WORKDIR dashboard

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY .streamlit ./.streamlit

COPY pages ./pages

COPY Key_Metrics.py .

CMD ["streamlit", "run", "Key_Metrics.py", "--server.port=8501", "--server.address=0.0.0.0"]