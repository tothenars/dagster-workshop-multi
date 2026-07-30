FROM python:3.11-slim

WORKDIR /opt/dagster/dagster_home

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=300 --retries=5 -r requirements.txt
COPY dagster.yaml workspace.yaml 

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

EXPOSE 3000
