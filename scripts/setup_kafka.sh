#!/bin/bash
echo "--- Configuring Kafka Topics ---"
/usr/local/kafka/bin/kafka-topics.sh --create --topic weather_forecast --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
/usr/local/kafka/bin/kafka-topics.sh --create --topic openaq_air_quality --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
echo "✅ Topics ready."
