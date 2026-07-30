FROM python:3.11-slim

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=300 --retries=5 -r requirements.txt

COPY . .

EXPOSE 4002

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4002", "-f", "main.py"]
