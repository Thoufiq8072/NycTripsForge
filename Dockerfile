FROM apache/airflow:3.1.6

USER root

RUN apt-get update && \
    apt-get install -y default-jdk-headless && \
    apt-get clean

USER airflow