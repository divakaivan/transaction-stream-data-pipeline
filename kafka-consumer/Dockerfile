FROM python:3.8-slim-buster

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /
COPY wait-for-it.sh wait-for-it.sh

ADD python-consumer.py .

CMD ./wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- python -u python-consumer.py
