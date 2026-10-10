# NepAirWatch application image: Python 3.11 + Java 17 + PySpark (with the Kafka connector pre-fetched)
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN grep -v -E "^(playwright|matplotlib)" requirements.txt > /tmp/req.txt \
 && pip install --no-cache-dir -r /tmp/req.txt

# Download the spark-sql-kafka connector at build time so containers start offline-fast
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', \
'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate().stop()"

COPY common ./common
COPY producer ./producer
COPY spark_jobs ./spark_jobs
COPY dashboard ./dashboard
COPY scripts/wait_for_lake.py ./scripts/wait_for_lake.py
COPY docker ./docker
COPY .streamlit ./.streamlit
COPY docs/architecture.png ./docs/architecture.png
RUN chmod +x docker/*.sh

ENV KAFKA_BOOTSTRAP=kafka:29092 \
    MONGO_URI=mongodb://mongodb:27017 \
    LAKE_PATH=/app/data/lake \
    CHECKPOINT_PATH=/app/data/checkpoints
