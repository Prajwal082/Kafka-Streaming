# Realtime Data Platform using Kafka

## Project Overview
A complete data pipeline for processing simulated order data from streaming sources into a real-time dashboard using Kafka, leveraging Spark Structured Streaming for processing, Snowflake for warehousing, DBT for transformation, and a Streamlit web app for dashboard hosting.

## Architecture
![diagram-export-3-18-2025-4_37_12-PM](https://github.com/user-attachments/assets/8ae867ca-7cbe-44b7-9510-0ace6406a678)


1. Kafka: A Kafka cluster hosted on Redpanda will receive messages from a producer application simulating order data.
2. Spark: Spark Structured Streaming will consume messages from the assigned topic, apply transformations and schema, and write the processed data to a Snowflake-managed 
          Iceberg table whoose metadata and data would be stored in ADLS.
3. Snowflake: Snowflake will serve as the data warehouse, storing the data in a denormalized format.
4. dbt: Used to perform transformations and aggregate data to generate key metrics.
5. Streamlit: Hosts the final web application on top of Snowflake for visualization.
6. Airflow: To Orchestrate the Entire Data Pipeline.
