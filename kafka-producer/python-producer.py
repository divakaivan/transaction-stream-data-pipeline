import time
import schedule
from json import dumps
import stripe
from kafka import KafkaProducer
import os

kafka_nodes ='kafka:9092'
my_topic = 'transactions'
stripe.api_key = os.environ['STRIPE_API_KEY']

def gen_data():

    prod = KafkaProducer(bootstrap_servers=kafka_nodes, api_version=(2, 0, 2),
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    charges = stripe.Charge.list(limit=10)
    my_data = {'transactions': charges}
    print(my_data)
    prod.send(my_topic, value=my_data)
    prod.flush()

if __name__ == '__main__':
    schedule.every(3).seconds.do(gen_data)
    while True:
        schedule.run_pending()
        time.sleep(0.5)
