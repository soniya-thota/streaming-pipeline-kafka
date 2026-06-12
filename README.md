# Real-Time Streaming Pipeline with Kafka and Spark

## Overview
This project demonstrates a real-time streaming data pipeline that simulates e-commerce transaction events, publishes them to Apache Kafka, and processes them using Spark Structured Streaming for live analytics.

The goal of this project is to understand how streaming data systems handle continuous event ingestion, message brokering, windowed aggregations, offsets, lag, and real-time analytical processing.

## Architecture

Producer
→ Generates simulated e-commerce transaction events in JSON format every second.

Kafka Topic
→ Acts as the message broker for real-time event ingestion.

Spark Structured Streaming Consumer
→ Reads events from Kafka, parses JSON messages, performs aggregations, and outputs live analytics.

## Pipeline Flow
Simulated Transactions
→ Kafka Producer
→ Kafka Topic
→ Spark Structured Streaming
→ Windowed Aggregations
→ Real-Time Analytics Output

## Tech Stack
- Python
- Apache Kafka
- Apache Spark
- PySpark
- Spark Structured Streaming
- Pandas
- JSON
- Docker

## Key Features
- Simulated real-time e-commerce transaction event generation
- Kafka producer for continuous JSON event publishing
- Kafka topic for streaming event ingestion
- Spark Structured Streaming consumer for real-time processing
- Windowed aggregations for live transaction analytics
- Monitoring of offsets, lag, and back-pressure behavior
- Modular producer and consumer scripts for local testing

## Example Use Cases
- Real-time transaction monitoring
- Fraud detection pipeline foundation
- Live sales analytics
- Event-driven data processing
- Streaming data platform learning project

## How to Run

1. Start Kafka and Zookeeper locally.

2. Run the Kafka producer:
   `python producer.py`

3. Run the Spark streaming consumer:
   `spark-submit consumer.py`

4. View real-time analytics output in the console.

## Resume Summary
Built a real-time streaming data pipeline using Kafka producers, Kafka topics, and Spark Structured Streaming consumers to process simulated e-commerce transactions with windowed aggregations and live analytics.
