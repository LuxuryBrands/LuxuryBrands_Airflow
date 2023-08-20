FROM apache/airflow:2.6.3-python3.10
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim default-jdk\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==2.6.3" -r /requirements.txt
