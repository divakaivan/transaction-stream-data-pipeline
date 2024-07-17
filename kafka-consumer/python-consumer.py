import os

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import from_json, col, explode, from_unixtime

from spark_schema import schema

kafka_nodes = os.getenv('KAFKA_SERVER')
my_topic = os.getenv('KAFKA_TOPIC')

spark = SparkSession.builder \
                    .appName("KafkaConsumer") \
                    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_nodes) \
    .option("subscribe", my_topic) \
    .option("startingOffsets", "latest") \
    .load()

df = df.withColumn("value", col("value").cast("string"))

df_parsed = df.withColumn("parsed_value", from_json(col("value"), schema)) \
    .select("parsed_value.*")

df_exploded = df_parsed.select(explode(col("transactions")).alias("transaction"))

df_final = df_exploded.selectExpr(
    "transaction.amount as amount",
    "transaction.amount_captured as amount_captured",
    "transaction.amount_refunded as amount_refunded",
    "transaction.application as application",
    "transaction.application_fee application_fee",
    "transaction.application_fee_amount as application_fee_amount",
    "transaction.balance_transaction as balance_transaction",
    "transaction.billing_details.address.city as billing_details_address_city",
    "transaction.billing_details.address.country as billing_details_address_country",
    "transaction.billing_details.address.line1 as billing_details_address_line1",
    "transaction.billing_details.address.line2 as billing_details_address_line2",
    "transaction.billing_details.address.postal_code as billing_details_address_postal_code",
    "transaction.billing_details.address.state as billing_details_address_state",
    "transaction.billing_details.email as billing_details_email",
    "transaction.billing_details.name as billing_details_name",
    "transaction.billing_details.phone as billing_details_phone",
    "transaction.calculated_statement_descriptor as calculated_statement_descriptor",
    "transaction.captured as captured",
    "transaction.created as created",
    "transaction.currency as currency",
    "transaction.customer as customer",
    "transaction.description as description",
    "transaction.destination as destination",
    "transaction.dispute as dispute",
    "transaction.disputed as disputed",
    "transaction.failure_balance_transaction as failure_balance_transaction",
    "transaction.failure_code as failure_code",
    "transaction.failure_message as failure_message",
    "transaction.id as id",
    "transaction.invoice as invoice",
    "transaction.livemode as livemode",
    "transaction.object as object",
    "transaction.on_behalf_of as on_behalf_of",
    "transaction.order as order",
    "transaction.outcome.network_status as outcome_network_status",
    "transaction.outcome.reason as outcome_reason",
    "transaction.outcome.risk_level as outcome_risk_level",
    "transaction.outcome.risk_score as outcome_risk_score",
    "transaction.outcome.seller_message as outcome_seller_message",
    "transaction.outcome.type as outcome_type",
    "transaction.paid as paid",
    "transaction.payment_intent as payment_intent",
    "transaction.payment_method as payment_method",
    "transaction.payment_method_details.card.amount_authorized as payment_method_details_card_amount_authorized",
    "transaction.payment_method_details.card.brand as payment_method_details_card_brand",
    "transaction.payment_method_details.card.checks.address_line1_check as payment_method_details_card_checks_address_line1_check",
    "transaction.payment_method_details.card.checks.address_postal_code_check as payment_method_details_card_checks_address_postal_code_check",
    "transaction.payment_method_details.card.checks.cvc_check as payment_method_details_card_checks_cvc_check",
    "transaction.payment_method_details.card.country as payment_method_details_card_country",
    "transaction.payment_method_details.card.exp_month as payment_method_details_card_exp_month",
    "transaction.payment_method_details.card.exp_year as payment_method_details_card_exp_year",
    "transaction.payment_method_details.card.extended_authorization.status as payment_method_details_card_extended_authorization_status",
    "transaction.payment_method_details.card.fingerprint as payment_method_details_card_fingerprint",
    "transaction.payment_method_details.card.funding as payment_method_details_card_funding",
    "transaction.payment_method_details.card.incremental_authorization.status as payment_method_details_card_incremental_authorization_status",
    "transaction.payment_method_details.card.installments as payment_method_details_card_installments",
    "transaction.payment_method_details.card.last4 as payment_method_details_card_last4",
    "transaction.payment_method_details.card.mandate as payment_method_details_card_mandate",
    "transaction.payment_method_details.card.multicapture.status as payment_method_details_card_multicapture_status",
    "transaction.payment_method_details.card.network as payment_method_details_card_network",
    "transaction.payment_method_details.card.network_token.used as payment_method_details_card_network_token_used",
    "transaction.payment_method_details.card.overcapture.maximum_amount_capturable as payment_method_details_card_overcapture_maximum_amount",
    "transaction.payment_method_details.card.overcapture.status as payment_method_details_card_overcapture_status",
    "transaction.payment_method_details.card.three_d_secure as payment_method_details_card_three_d_secure",
    "transaction.payment_method_details.card.wallet as payment_method_details_card_wallet",
    "transaction.payment_method_details.type as payment_method_details_type",
    "transaction.receipt_email as receipt_email",
    "transaction.receipt_number as receipt_number",
    "transaction.receipt_url as receipt_url",
    "transaction.refunded as refunded",
    "transaction.review as review",
    "transaction.shipping as shipping",
    "transaction.source.address_city as source_address_city",
    "transaction.source.address_country as source_address_country",
    "transaction.source.address_line1 as source_address_line1",
    "transaction.source.address_line1_check as source_address_line1_check",
    "transaction.source.address_line2 as source_address_line2",
    "transaction.source.address_state as source_address_state",
    "transaction.source.address_zip as source_address_zip",
    "transaction.source.address_zip_check as source_address_zip_check",
    "transaction.source.brand as source_brand",
    "transaction.source.country as source_country",
    "transaction.source.customer as source_customer",
    "transaction.source.cvc_check as source_cvc_check",
    "transaction.source.dynamic_last4 as source_dynamic_last4",
    "transaction.source.exp_month as source_exp_month",
    "transaction.source.exp_year as source_exp_year",
    "transaction.source.fingerprint as source_fingerprint",
    "transaction.source.funding as source_funding",
    "transaction.source.id as source_id",
    "transaction.source.last4 as source_last4",
    "transaction.source.name as source_name",
    "transaction.source.object as source_object",
    "transaction.source.tokenization_method as source_tokenization_method",
    "transaction.source.wallet as source_wallet",
    "transaction.source_transfer as source_transfer",
    "transaction.statement_descriptor as statement_descriptor",
    "transaction.statement_descriptor_suffix as statement_descriptor_suffix",
    "transaction.status as status",
    "transaction.transfer_data as transfer_data",
    "transaction.transfer_group as transfer_group"
)

df_final.printSchema()
df_final = df_final.withColumn("created", from_unixtime(col("created").cast(LongType())).cast("timestamp"))

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")

pg_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

pg_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, epoch_id):
    df.write \
        .jdbc(url=pg_url, table="transactions", mode="append", properties=pg_properties)

query = df_final \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
