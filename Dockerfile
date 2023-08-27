FROM apache/airflow:2.6.3-python3.10

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim default-jdk\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY ../config ${AIRFLOW_HOME}/
COPY ../dags ${AIRFLOW_HOME}/dags
COPY ../plugins ${AIRFLOW_HOME}/plugins
COPY ../requirements.txt /
COPY ../entrypoint.sh /

# Add directory in which pip installs to PATH
ENV PATH=${PATH}:/home/airflow/.local/bin
#ENV PYTHONPATH=/opt/airflow
#ENV AIRFLOW_HOME=/opt/airflow

# Make sure Airflow is owned by airflow user
RUN chown -R "airflow" "${AIRFLOW_HOME}"
RUN chmod +x /entrypoint.sh

# Specifies the ports on which the container is listening inside
EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
RUN pip install --no-cache-dir "apache-airflow==2.6.3" -r /requirements.txt

# Set the entrypoint script to run when the container starts
ENTRYPOINT ["/entrypoint.sh"]