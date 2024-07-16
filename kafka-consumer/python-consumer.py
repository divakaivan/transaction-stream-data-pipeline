
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
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

kafka_nodes = 'kafka:9092'
my_topic = 'transactions'

conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='password',
    host='postgres',
    port='5432'
)
cur = conn.cursor()

consumer = KafkaConsumer(my_topic,
                         bootstrap_servers=kafka_nodes,
                         api_version=(2, 0, 2),
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

def insert_transactions(transactions):
    query = """
    INSERT INTO transactions (
        transaction_id, amount, currency, created, network_status, 
        risk_level, risk_score, seller_message, paid, card_brand, 
        receipt_number, receipt_url
    ) VALUES %s
    ON CONFLICT (transaction_id) DO NOTHING
    """
    
    values = [
        (
            t['transaction_id'],
            t['amount'],
            t['currency'],
            t['created'],
            t['network_status'],
            t['risk_level'],
            t['risk_score'],
            t['seller_message'],
            t['paid'],
            t['card_brand'],
            t['receipt_number'],
            t['receipt_url']
        ) for t in transactions
    ]
    
    execute_values(cur, query, values)
    conn.commit()

for message in consumer:
    all_transactions = message.value.get('transactions')
    
    chosen_vars = []
    for transaction in all_transactions:
        transaction_id = transaction.get('id')
        amount = transaction.get('amount')
        currency = transaction.get('currency')
        created = datetime.fromtimestamp(transaction.get('created'))
        network_status = transaction.get('outcome', {}).get('network_status')
        risk_level = transaction.get('outcome', {}).get('risk_level')
        risk_score = transaction.get('outcome', {}).get('risk_score')
        seller_message = transaction.get('outcome', {}).get('seller_message')
        paid = transaction.get('paid')
        card_brand = transaction.get('payment_method_details', {}).get('card', {}).get('brand')
        receipt_number = transaction.get('receipt_number')
        receipt_url = transaction.get('receipt_url')
        
        new_transaction = {
            'transaction_id': transaction_id,
            'amount': amount,
            'currency': currency,
            'created': created,
            'network_status': network_status,
            'risk_level': risk_level,
            'risk_score': risk_score,
            'seller_message': seller_message,
            'paid': paid,
            'card_brand': card_brand,
            'receipt_number': receipt_number,
            'receipt_url': receipt_url
        }
        
        chosen_vars.append(new_transaction)
    
    if chosen_vars:
        insert_transactions(chosen_vars)

cur.close()
conn.close()
