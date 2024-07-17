FROM python:3.8-slim-buster

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless ca-certificates-java procps wget && \
    apt-get clean && \
    update-ca-certificates -f && \
    rm -rf /var/lib/apt/lists/*

# Verify the Java installation
RUN java -version

# Install Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt
ENV SPARK_HOME=/opt/spark-3.5.1-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install postgres class
RUN mkdir -p /opt/spark/jars/
RUN wget -O /opt/spark/jars/postgresql-42.2.20.jar https://jdbc.postgresql.org/download/postgresql-42.2.20.jar

# Find the Java installation path using update-alternatives and set JAVA_HOME
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo $JAVA_HOME && \
    ln -s $JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64 && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /etc/profile && \
    echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /etc/profile

# Set environment variables for Java in Dockerfile scope
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Verify the JAVA_HOME path
RUN echo $JAVA_HOME && ls -l $JAVA_HOME/bin/java

# Copy requirements.txt and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy resources
WORKDIR /
COPY wait-for-it.sh wait-for-it.sh

ADD python-consumer.py .

CMD ["/bin/bash", "-c", "/wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- /wait-for-it.sh -s -t 30 $KAFKA_SERVER -- ${SPARK_HOME}/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.2.20 python-consumer.py"]