FROM python:3.8-slim-buster

WORKDIR /app

# Cache dependencies
COPY clean_all.py /app/
COPY requirements.txt /app/

RUN pip install "pip==20.2.4"
RUN pip install -r requirements.txt
