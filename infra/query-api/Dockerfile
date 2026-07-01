FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/binder_data_hub

COPY requirements/spark.txt requirements/query_api.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/query_api.txt

ENV PYTHONPATH=/opt/binder_data_hub
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY src ./src

EXPOSE 8000

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
