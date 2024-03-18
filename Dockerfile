FROM apache/airflow:2.5.1-python3.9
#USER root
#RUN apt-get update && apt-get install -y gcc libpq-dev && apt-get clean
#USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt