#!/bin/bash

~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties &
echo "ZooKeeper started successfully"
sleep 10
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &
echo "Kafka Server started sucessfully"
sleep 10
~/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication 1 --partitions 1 --topic scala &
echo "Kafka Topic Created"
sleep 10
~/cassandra-3.11.0/bin/cassandra 
echo "Cassandra started sucessfully"