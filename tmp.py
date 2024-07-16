import stripe
from kafka import KafkaProducer
import json
import time
from json import dumps

# Configure Stripe API
stripe.api_key = 'pk_test_51HCmE9BiZYeN1Gv3wDGhkyMUs9rsFUtJD5ExlUfToLWvFaKJ6e30P4sERAf3gZ1M8SfVqlF5yeb87QQfdhRzz0en007B66Zaq3'

# Adjust the bootstrap_servers parameter with your actual Kafka brokers' addresses
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
# Function to fetch recent charges from Stripe
def fetch_stripe_charges():
    last_seen_charge_id = None
    while True:
        # Fetch the most recent 10 charges
        if last_seen_charge_id:
            charges = stripe.Charge.list(limit=10, starting_after=last_seen_charge_id)
        else:
            charges = stripe.Charge.list(limit=10)

        # Check if there are any charges
        if charges.data:
            for charge in charges.auto_paging_iter():
                producer.send('stripe_transactions', charge.to_dict())
                last_seen_charge_id = charge.id

        # Sleep for a while before fetching new charges
        time.sleep(60)  # Adjust the interval as needed

if __name__ == "__main__":
    fetch_stripe_charges()































import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from kafka import KafkaProducer
import stripe
import json

# Kafka settings
kafka_nodes = 'localhost:9092'  # Replace with your Kafka broker(s)
myTopic = 'stripe_data'

# Stripe API key
stripe.api_key = 'sk_test_51HCmE9BiZYeN1Gv3o9rL3K5q3PB6bab5ta51dN1w66Y2g60m0GsyU0O4gp223DT6nB6C2uaxwz6sfo8HMnxDFtXH00syIGKm4j'

# Schema for the streaming data
schema = StructType() \
    .add("id", StringType()) \
    .add("amount", IntegerType()) \
    .add("customer_email", StringType())  # Add more fields as needed

def process_event(event_json):
    event_data = json.loads(event_json)
    # Extract relevant fields
    cleaned_data = {
        'id': event_data['id'],
        'amount': event_data['amount'] / 100,  # Convert amount to dollars
        'customer_email': event_data['customer_email']
        # Add more fields as needed
    }
    return cleaned_data

if __name__ == '__main__':
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_nodes,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("StripeDataPipeline") \
        .getOrCreate()

    # Define the streaming DataFrame that reads from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_nodes) \
        .option("subscribe", myTopic) \
        .load()

    # Convert the value column from Kafka (which is binary) to a string
    df = df.selectExpr("CAST(value AS STRING)")

    # Parse the JSON string into structured data
    df = df.withColumn("data", from_json(col("value"), schema))

    # Apply processing to clean and transform the data
    cleaned_df = df \
        .select(expr("data.id AS id"),
                expr("data.amount AS amount"),
                expr("data.customer_email AS customer_email"))

    # Define a query to write the cleaned data to Kafka as a new topic
    query = cleaned_df \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_nodes) \
        .option("topic", "cleaned_stripe_data") \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    # Start the streaming query
    query.awaitTermination()

    # Stop the Spark session
    # spark.stop()
