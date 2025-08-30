# Base image with Python and Java support
FROM ubuntu:22.04

# Prevent user prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3.10 python3.10-venv python3.10-dev \
    python3-pip openjdk-11-jdk \
    wget curl git build-essential \
    libssl-dev libffi-dev ca-certificates \
    && update-ca-certificates \
    && ln -s /usr/bin/python3.10 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Java and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Install Python packages
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Set working directory
WORKDIR /app

# Copy your entire project code
COPY . /app

# Default command to run all steps (modify as needed)
CMD ["bash", "run_pipeline.sh"]
