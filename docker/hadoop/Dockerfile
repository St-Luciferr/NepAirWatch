# Apache Hadoop 3.4 (HDFS + YARN + MapReduce) and Apache Flume 1.11 images for NepAirWatch.
#   target "hadoop": namenode / datanode / resourcemanager / nodemanager (role = first argument)
#   target "flume" : Flume agent with the Hadoop client on its classpath (for the HDFS sink)
FROM eclipse-temurin:11-jre-jammy AS hadoop
ARG HADOOP_VERSION=3.4.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 curl procps netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*
# Full tarball; the AWS SDK bundle and docs are removed in the same layer to keep the image small.
RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
      | tar -xz -C /opt \
 && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm -rf /opt/hadoop/share/doc /opt/hadoop/share/hadoop/tools/lib/bundle-*.jar
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH
COPY conf/*.xml /opt/hadoop/etc/hadoop/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

FROM hadoop AS flume
ARG FLUME_VERSION=1.11.0
# Flume's bundled Guava clashes with Hadoop 3's (NoSuchMethodError in the HDFS sink): use Hadoop's.
RUN curl -fsSL "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" \
      | tar -xz -C /opt \
 && mv /opt/apache-flume-${FLUME_VERSION}-bin /opt/flume \
 && rm -f /opt/flume/lib/guava-*.jar
ENV FLUME_HOME=/opt/flume PATH=/opt/flume/bin:$PATH
ENTRYPOINT []
CMD ["flume-ng", "agent", "-n", "agent", "-c", "/opt/flume/conf", "-f", "/flume/flume.conf", \
     "-Dflume.root.logger=INFO,console", "-Dflume.monitoring.type=http", "-Dflume.monitoring.port=34545"]
