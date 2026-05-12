FROM apache/spark:3.5.1

USER root
# Install build dependencies for Python
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         zlib1g-dev \
         libncurses5-dev \
         libgdbm-dev \
         libnss3-dev \
         libssl-dev \
         libreadline-dev \
         libffi-dev \
         libsqlite3-dev \
         wget \
         curl \
         ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Download and compile Python 3.12 (takes a few minutes but is 100% reliable)
RUN wget https://www.python.org/ftp/python/3.12.3/Python-3.12.3.tgz \
  && tar -xf Python-3.12.3.tgz \
  && cd Python-3.12.3 \
  && ./configure --enable-optimizations \
  && make -j$(nproc) \
  && make altinstall \
  && cd .. \
  && rm -rf Python-3.12.3 Python-3.12.3.tgz

# Install pip for Python 3.12
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12

# Install pyspark on Python 3.12 so the worker has the library
RUN python3.12 -m pip install pyspark==3.5.1

# Update alternatives to make python3 point to python3.12
RUN update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.12 1
RUN update-alternatives --set python3 /usr/local/bin/python3.12

# Set environment variables for PySpark to use the correct Python
ENV PYSPARK_PYTHON=/usr/local/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

# Download hadoop-aws and aws-java-sdk-bundle for MinIO/S3 connection
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
 && curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
 && curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/spark/jars/postgresql-42.7.3.jar

USER spark
