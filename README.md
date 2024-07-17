# Project overview

![transactions_stream_project_diagram](/project-png/transactions_stream_project_diagram.png)

* **Stripe**: Using Stripe's API as the source of generating real-like transaction data.

* **Apache Kafka**: Stripe transaction data is streamed into Apache Kafka. It handles the data streams and ensures they are processed in real-time.

* **Apache ZooKeeper**: ZooKeeper is used alongside Kafka to manage and coordinate the Kafka brokers. ZooKeeper helps maintain configuration information, naming, synchronization, and group services.

* **PySpark**: The data from Kafka is then processed using PySpark Structured Streaming. This involves transforming individual transaction data into rows fit for a database.

* **Forex API**: GBP/x exchange rates are taken from an online API and updates every 24 hours.

* **PostgreSQL**: After processing, the data is stored in PostgreSQL.

* **dbt (Data Build Tool)**: dbt is used to manage and transform data within PostgreSQL. Data is split into dimension and a fact tables.

* **Grafana**: Finally, the data stored in PostgreSQL is visualized using Grafana.


# Data model

![transactions_stream_data_model](/project-png/transactions_stream_data_model.png)

# dbt documentation

[Link to the docs](https://transaction-stream-data-docs.netlify.app/)

#### dbt lineage

<img width="1202" alt="image" src="https://github.com/user-attachments/assets/e3a619e8-ff5d-44ca-a589-39d2de03467f">

# Visualisation

<img width="1482" alt="image" src="https://github.com/user-attachments/assets/c5941d25-59e2-4029-88be-2aae36d4baea">

# Considerations for improvements

* add PySpark tests
* use an orchestrator
* use more data for a better Grafana visualisation
  * maybe find an alternative transactions data source because the Stripe API has a 25 rate limit
  * also many of the generated values in a transaction from the Stripe API are null
