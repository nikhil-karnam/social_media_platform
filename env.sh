#!/bin/bash
export API_ADDR=:50051
export MYSQL_USER=root
export MYSQL_PASSWORD=
export MYSQL_ADDR=127.0.0.1:3306
export MYSQL_DB=twitter
export CASSANDRA_ADDRS=127.0.0.1:9042,127.0.0.1:9043,127.0.0.1:9044
export CASSANDRA_JAVA_HOME="$(ls -d /usr/lib/jvm/java-17-openjdk-* | head -n 1)"
export KAFKA_ADDRS=127.0.0.1:9092
export KAFKA_TOPIC=fanout
export KAFKA_GROUP=fanout-consumers
export REDIS_ADDR=127.0.0.1:6379
export LANES=4
