import os
import time
import random
import schedule
from json import dumps

from kafka import KafkaProducer

import stripe

kafka_nodes = os.getenv('KAFKA_SERVER')
my_topic = os.getenv('KAFKA_TOPIC')

def create_test_charge():
    try:
        amount = random.randint(100, 1000000) # 1p to £10,000
        stripe.api_key = os.getenv('STRIPE_API_KEY') 
        charge = stripe.Charge.create(
            amount=amount,
            currency='gbp',
            source='tok_visa',
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
    num_charges = 25 # stripe create limit
    charges = [create_test_charge() for _ in range(num_charges)]
    send_to_kafka(charges)

if __name__ == '__main__':
    schedule.every(3).seconds.do(gen_data)
    try:
        while True:
            schedule.run_pending()
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping...")
