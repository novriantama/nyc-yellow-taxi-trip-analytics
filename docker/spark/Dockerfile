FROM bitnami/spark:3.5.1

USER root
# Install python and pip to run pyspark scripts natively inside the container if needed
RUN apt-get update \
  && apt-get install -y python3 python3-pip \
  && rm -rf /var/lib/apt/lists/*

# Note: You can download Hadoop COS jars here when ready to integrate with Tencent Cloud.
# Example:
# RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-cos/3.3.4/hadoop-cos-3.3.4.jar -o /opt/bitnami/spark/jars/hadoop-cos-3.3.4.jar

USER 1001
