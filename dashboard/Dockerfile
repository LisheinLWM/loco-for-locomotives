FROM python

WORKDIR dashboard

COPY requirements.txt . 

RUN pip install -r requirements.txt

COPY .streamlit ./.streamlit

COPY logo.png .

COPY dashboard.py .

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]