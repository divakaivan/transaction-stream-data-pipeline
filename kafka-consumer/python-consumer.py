
"""
{
        'id': 'ch_1HCoaZBiZYeN1Gv3dEyqMhBZ',
        'object': 'charge',
        'amount': 1854681,
        'amount_captured': 1854681,
        'amount_refunded': 0,
        'application': None,
        'application_fee': None,
        'application_fee_amount': None,
        'balance_transaction': 'txn_1HCoaaBiZYeN1Gv3qmNGsH1n',
        'billing_details': {
          'address': {
            'city': None,
            'country': None,
            'line1': None,
            'line2': None,
            'postal_code': None,
            'state': None
          },
          'email': None,
          'name': 'dog@dog.com',
          'phone': None
        },
        'calculated_statement_descriptor': 'Stripe',
        'captured': True,
        'created': 1596641495,
        'currency': 'usd',
        'customer': None,
        'description': None,
        'destination': None,
        'dispute': None,
        'disputed': False,
        'failure_balance_transaction': None,
        'failure_code': None,
        'failure_message': None,
        'fraud_details': {
          
        },
        'invoice': None,
        'livemode': False,
        'metadata': {
          
        },
        'on_behalf_of': None,
        'order': None,
        'outcome': {
          'network_status': 'approved_by_network',
          'reason': None,
          'risk_level': 'normal',
          'risk_score': 20,
          'seller_message': 'Payment complete.',
          'type': 'authorized'
        },
        'paid': True,
        'payment_intent': None,
        'payment_method': 'card_1HCoaWBiZYeN1Gv3pUKPmme0',
        'payment_method_details': {
          'card': {
            'amount_authorized': None,
            'brand': 'visa',
            'checks': {
              'address_line1_check': None,
              'address_postal_code_check': None,
              'cvc_check': 'pass'
            },
            'country': 'US',
            'exp_month': 2,
            'exp_year': 2022,
            'extended_authorization': {
              'status': 'disabled'
            },
            'fingerprint': 'EcjpXEmZnyBe8iTI',
            'funding': 'credit',
            'incremental_authorization': {
              'status': 'unavailable'
            },
            'installments': None,
            'last4': '4242',
            'mandate': None,
            'multicapture': {
              'status': 'unavailable'
            },
            'network': 'visa',
            'network_token': {
              'used': False
            },
            'overcapture': {
              'maximum_amount_capturable': 1854681,
              'status': 'unavailable'
            },
            'three_d_secure': None,
            'wallet': None
          },
          'type': 'card'
        },
        'receipt_email': None,
        'receipt_number': '1392-9253',
        'receipt_url': 'https://pay.stripe.com/receipts/payment/CAcaFwoVYWNjdF8xSENtRTlCaVpZZU4xR3YzKLqH2LQGMgaUqeLkS9s6LBYysebQpXRgIhoxBjY-HFp_84k_zaA3nRxuxTMo8fuAevmgEydJMUmjpizq',
        'refunded': False,
        'review': None,
        'shipping': None,
        'source': {
          'id': 'card_1HCoaWBiZYeN1Gv3pUKPmme0',
          'object': 'card',
          'address_city': None,
          'address_country': None,
          'address_line1': None,
          'address_line1_check': None,
          'address_line2': None,
          'address_state': None,
          'address_zip': None,
          'address_zip_check': None,
          'brand': 'Visa',
          'country': 'US',
          'customer': None,
          'cvc_check': 'pass',
          'dynamic_last4': None,
          'exp_month': 2,
          'exp_year': 2022,
          'fingerprint': 'EcjpXEmZnyBe8iTI',
          'funding': 'credit',
          'last4': '4242',
          'metadata': {
            
          },
          'name': 'dog@dog.com',
          'tokenization_method': None,
          'wallet': None
        },
        'source_transfer': None,
        'statement_descriptor': None,
        'statement_descriptor_suffix': None,
        'status': 'succeeded',
        'transfer_data': None,
        'transfer_group': None
      },
"""

from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

full_schema = StructType([
    StructField("id", StringType(), True),
    StructField("object", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("amount_captured", IntegerType(), True),
    StructField("amount_refunded", IntegerType(), True),
    StructField("application", StringType(), True),
    StructField("application_fee", StringType(), True),
    StructField("application_fee_amount", StringType(), True),
    StructField("balance_transaction", StringType(), True),
    StructField("billing_details", StringType(), True),
    StructField("calculated_statement_descriptor", StringType(), True),
    StructField("captured", StringType(), True),
    StructField("created", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("description", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("dispute", StringType(), True),
    StructField("disputed", StringType(), True),
    StructField("failure_balance_transaction", StringType(), True),
    StructField("failure_code", StringType(), True),
    StructField("failure_message", StringType(), True),
    StructField("fraud_details", StringType(), True),
    StructField("invoice", StringType(), True),
    StructField("livemode", StringType(), True),
    StructField("metadata", StringType(), True),
    StructField("on_behalf_of", StringType(), True),
    StructField("order", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("paid", StringType(), True),
    StructField("payment_intent", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_method_details", StringType(), True),
    StructField("receipt_email", StringType(), True),
    StructField("receipt_number", StringType(), True),
    StructField("receipt_url", StringType(), True),
    StructField("refunded", StringType(), True),
    StructField("review", StringType(), True),
    StructField("shipping", StringType(), True),
    StructField("source", StringType(), True),
    StructField("source_transfer", StringType(), True),
    StructField("statement_descriptor", StringType(), True),
    StructField("statement_descriptor_suffix", StringType(), True),
    StructField("status", StringType(), True),
    StructField("transfer_data", StringType(), True),
    StructField("transfer_group", StringType(),True)
])


# # connect to postgres
# conn = psycopg2.connect(
#     dbname='postgres',
#     user='postgres',
#     password='password',
#     host='postgres',
#     port='5432'
# )

# cur = conn.cursor()

# connect to kafka
# info should match the kafka-producer
kafka_nodes = 'kafka:9092'
my_topic = 'transactions'

consumer = KafkaConsumer(my_topic,
                        bootstrap_servers=kafka_nodes, api_version=(2, 0, 2),
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

chosen_vars = []
schema_types_chosen = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("created", StringType(), True),
    StructField("network_status", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("seller_message", StringType(), True),
    StructField("paid", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("receipt_number", StringType(), True),
    StructField("receipt_url", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_nodes) \
    .option("subscribe", my_topic) \
    .load()

# keep for now. not sure if needed
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), full_schema).alias("data")) \
    .select("data.*")

processed_df = df.filter(col("amount") > 100)

# Write processed data to console for testing
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()

# Stop Spark session
spark.stop()

# empty_rdd = spark.sparkContext.emptyRDD()
# df = spark.createDataFrame(empty_rdd, full_schema)

# for message in consumer:
#     all_transactions: list = message.value.get('transactions').get('data')
    
#     if all_transactions:
#         for transaction in all_transactions:
#             transaction_id = transaction.get('id')
#             amount = transaction.get('amount')
#             currency = transaction.get('currency')
#             created = datetime.fromtimestamp(transaction.get('created'))
#             network_status = transaction.get('outcome').get('network_status')
#             risk_level = transaction.get('outcome').get('risk_level')
#             risk_score = transaction.get('outcome').get('risk_score')
#             seller_message = transaction.get('outcome').get('seller_message')
#             paid = transaction.get('paid')
#             card_brand = transaction.get('payment_method_details').get('card').get('brand')
#             receipt_number = transaction.get('receipt_number')
#             receipt_url = transaction.get('receipt_url')
#             chosen_vars.append({
#                 'transaction_id': transaction_id,
#                 'amount': amount,
#                 'currency': currency,
#                 'created': created,
#                 'network_status': network_status,
#                 'risk_level': risk_level,
#                 'risk_score': risk_score,
#                 'seller_message': seller_message,
#                 'paid': paid,
#                 'card_brand': card_brand,
#                 'receipt_number': receipt_number,
#                 'receipt_url': receipt_url
#             })

# # Stop the Spark session when done
# spark.stop()
    