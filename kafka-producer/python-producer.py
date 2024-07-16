import os
import time
import random
import schedule
from json import dumps

from kafka import KafkaProducer
from pyspark.sql import SparkSession

import stripe

# kafka config
kafka_nodes = 'kafka:9092'
my_topic = 'transactions'

def create_test_charge():
    try:
        amount = random.randint(100, 1000000)
        stripe.api_key = os.getenv('STRIPE_API_KEY') 
        charge = stripe.PaymentIntent.create(
            amount=amount,
            currency='gbp'
        )
        return charge
    except stripe.error.CardError as e:
        print(f"Card declined: {e.error.message}")
        return None
    except stripe.error.StripeError as e:
        print(f"Stripe error: {e}")
        return None

def send_to_kafka(charges):
    try:
        prod = KafkaProducer(bootstrap_servers=kafka_nodes, api_version=(2, 0, 2),
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

        my_data = {'transactions': charges}

        prod.send(my_topic, value=my_data)
        prod.flush()

        print(f"Sent {len(charges)} transactions to Kafka")

    except Exception as e:
        print(f"Error sending to Kafka: {e}")
    
def gen_data():
    num_charges = 25
    charges_rdd = spark.sparkContext.parallelize(range(num_charges))
    charges = charges_rdd.map(lambda _: create_test_charge()).filter(lambda x: x is not None).collect()

    send_to_kafka(charges)

if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName("StripeChargesToKafka") \
            .getOrCreate()
    schedule.every(3).seconds.do(gen_data)
    try:
        while True:
            schedule.run_pending()
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        spark.stop()
