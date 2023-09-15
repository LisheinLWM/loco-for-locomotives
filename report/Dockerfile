FROM public.ecr.aws/lambda/python

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY logo.png .

COPY report.py .

CMD ["report.lambda_handler"]