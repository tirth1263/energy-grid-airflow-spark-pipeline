FROM apache/airflow:2.10.5-python3.11

# Spark needs a JVM.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /requirements.txt
# Airflow's constraints file pins every dependency to versions tested with this Airflow release.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
