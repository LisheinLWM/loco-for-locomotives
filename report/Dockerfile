FROM python

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY logo.png .

COPY get_pdf.py .

CMD ["python3","get_pdf.py"]