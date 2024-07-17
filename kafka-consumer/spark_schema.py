from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType, MapType

schema = StructType([
    StructField("transactions", ArrayType(
        StructType([
            StructField("amount", LongType(), True),
            StructField("amount_captured", LongType(), True),
            StructField("amount_refunded", LongType(), True),
            StructField("application", StringType(), True),
            StructField("application_fee", StringType(), True),
            StructField("application_fee_amount", StringType(), True),
            StructField("balance_transaction", StringType(), True),
            StructField("billing_details", StructType([
                StructField("address", StructType([
                    StructField("city", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("line1", StringType(), True),
                    StructField("line2", StringType(), True),
                    StructField("postal_code", StringType(), True),
                    StructField("state", StringType(), True)
                ]), True),
                StructField("email", StringType(), True),
                StructField("name", StringType(), True),
                StructField("phone", StringType(), True)
            ]), True),
            StructField("calculated_statement_descriptor", StringType(), True),
            StructField("captured", BooleanType(), True),
            StructField("created", LongType(), True),
            StructField("currency", StringType(), True),
            StructField("customer", StringType(), True),
            StructField("description", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("dispute", StringType(), True),
            StructField("disputed", BooleanType(), True),
            StructField("failure_balance_transaction", StringType(), True),
            StructField("failure_code", StringType(), True),
            StructField("failure_message", StringType(), True),
            StructField("fraud_details", MapType(StringType(), StringType()), True),
            StructField("id", StringType(), True),
            StructField("invoice", StringType(), True),
            StructField("livemode", BooleanType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("object", StringType(), True),
            StructField("on_behalf_of", StringType(), True),
            StructField("order", StringType(), True),
            StructField("outcome", StructType([
                StructField("network_status", StringType(), True),
                StructField("reason", StringType(), True),
                StructField("risk_level", StringType(), True),
                StructField("risk_score", LongType(), True),
                StructField("seller_message", StringType(), True),
                StructField("type", StringType(), True)
            ]), True),
            StructField("paid", BooleanType(), True),
            StructField("payment_intent", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_method_details", StructType([
                StructField("card", StructType([
                    StructField("amount_authorized", LongType(), True),
                    StructField("brand", StringType(), True),
                    StructField("checks", StructType([
                        StructField("address_line1_check", StringType(), True),
                        StructField("address_postal_code_check", StringType(), True),
                        StructField("cvc_check", StringType(), True)
                    ]), True),
                    StructField("country", StringType(), True),
                    StructField("exp_month", LongType(), True),
                    StructField("exp_year", LongType(), True),
                    StructField("extended_authorization", StructType([
                        StructField("status", StringType(), True)
                    ]), True),
                    StructField("fingerprint", StringType(), True),
                    StructField("funding", StringType(), True),
                    StructField("incremental_authorization", StructType([
                        StructField("status", StringType(), True)
                    ]), True),
                    StructField("installments", StringType(), True),
                    StructField("last4", StringType(), True),
                    StructField("mandate", StringType(), True),
                    StructField("multicapture", StructType([
                        StructField("status", StringType(), True)
                    ]), True),
                    StructField("network", StringType(), True),
                    StructField("network_token", StructType([
                        StructField("used", BooleanType(), True)
                    ]), True),
                    StructField("overcapture", StructType([
                        StructField("maximum_amount_capturable", LongType(), True),
                        StructField("status", StringType(), True)
                    ]), True),
                    StructField("three_d_secure", StringType(), True),
                    StructField("wallet", StringType(), True)
                ]), True),
                StructField("type", StringType(), True)
            ]), True),
            StructField("receipt_email", StringType(), True),
            StructField("receipt_number", StringType(), True),
            StructField("receipt_url", StringType(), True),
            StructField("refunded", BooleanType(), True),
            StructField("review", StringType(), True),
            StructField("shipping", StringType(), True),
            StructField("source", StructType([
                StructField("address_city", StringType(), True),
                StructField("address_country", StringType(), True),
                StructField("address_line1", StringType(), True),
                StructField("address_line1_check", StringType(), True),
                StructField("address_line2", StringType(), True),
                StructField("address_state", StringType(), True),
                StructField("address_zip", StringType(), True),
                StructField("address_zip_check", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("country", StringType(), True),
                StructField("customer", StringType(), True),
                StructField("cvc_check", StringType(), True),
                StructField("dynamic_last4", StringType(), True),
                StructField("exp_month", LongType(), True),
                StructField("exp_year", LongType(), True),
                StructField("fingerprint", StringType(), True),
                StructField("funding", StringType(), True),
                StructField("id", StringType(), True),
                StructField("last4", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
                StructField("name", StringType(), True),
                StructField("object", StringType(), True),
                StructField("tokenization_method", StringType(), True),
                StructField("wallet", StringType(), True)
            ]), True),
            StructField("source_transfer", StringType(), True),
            StructField("statement_descriptor", StringType(), True),
            StructField("statement_descriptor_suffix", StringType(), True),
            StructField("status", StringType(), True),
            StructField("transfer_data", StringType(), True),
            StructField("transfer_group", StringType(), True)
        ])
    ))
])
