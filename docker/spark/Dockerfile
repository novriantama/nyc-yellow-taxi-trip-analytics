FROM apache/spark:3.5.1

USER root
# Install python, pip, and curl
RUN apt-get update \
  && apt-get install -y python3 python3-pip curl \
  && rm -rf /var/lib/apt/lists/*

# Download hadoop-aws and aws-java-sdk-bundle for MinIO/S3 connection
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
 && curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
