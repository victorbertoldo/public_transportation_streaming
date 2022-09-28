#!/bin/bash

TOPICS=$(docker-compose exec kafka0 kafka-topics --list --bootstrap-server localhost:9092)

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
    docker-compose exec kafka0 kafka-topics --zookeeper localhost:2181/kafka --delete --topic $T
  fi
done

