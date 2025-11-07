FROM apache/airflow:3.1.2

# Install Java (required for PySpark)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Install PySpark and other Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0
