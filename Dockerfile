FROM apache/airflow:latest-python3.11

COPY requirements_clean.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements_clean.txt

RUN mkdir src/

COPY setup.py /src/setup.py
COPY dst_airlines/ /src/dst_airlines/

USER root
RUN pip3 install --no-cache-dir /src
USER airflow