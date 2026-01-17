#!/bin/bash
echo "Starting Hadoop, YARN, and Kafka..."
/usr/local/hadoop/sbin/start-all.sh
/usr/local/kafka/bin/zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
sudo systemctl start influxdb
sudo systemctl start grafana-server
echo "Services are warming up..."
