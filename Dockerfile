# NepAirWatch application image: Python 3.11 + Java 17 + PySpark (Kafka + Cassandra connectors pre-fetched)
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 && JAVA_BIN="$(readlink -f "$(command -v java)")" \
 && JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")" \
 && ln -sfn "$JAVA_HOME" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN grep -v -E "^(playwright|matplotlib)" requirements.txt > /tmp/req.txt \
 && pip install --no-cache-dir -r /tmp/req.txt

# Download the Spark connectors (Kafka source/sink, Cassandra) at build time so containers start fast
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', \
'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1')\
.getOrCreate().stop()"

COPY common ./common
COPY producer ./producer
COPY spark_jobs ./spark_jobs
COPY dashboard ./dashboard
COPY scripts/wait_for_lake.py scripts/hive_refresh.py scripts/demo_cleanup.py ./scripts/
COPY demo_ui ./demo_ui
COPY hive/queries.hql ./hive/queries.hql
COPY docker ./docker
COPY .streamlit ./.streamlit
COPY docs/architecture.png ./docs/architecture.png
RUN chmod +x docker/*.sh

ENV KAFKA_BOOTSTRAP=kafka:29092 \
    MONGO_URI=mongodb://mongodb:27017 \
    CASSANDRA_HOSTS=cassandra \
    LAKE_PATH=/app/data/lake \
    CHECKPOINT_PATH=/app/data/checkpoints
