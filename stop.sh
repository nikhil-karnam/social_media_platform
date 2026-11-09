#!/bin/bash
cd "$(dirname "$0")"
source env.sh

# api, consumers, tweet nodes
pkill -f 'go run ./cmd/(api|consumer|server)'
pkill -f '/go-build[^ ]*/(api|consumer|server)( |$)'
while pgrep -f 'go run ./cmd/(api|consumer|server)|/go-build[^ ]*/(api|consumer|server)( |$)' > /dev/null; do sleep 1; done

# wait for kafka to drop the stopped consumers from the group
if pgrep -f kafka.Kafka > /dev/null; then
  until kafka-consumer-groups.sh --bootstrap-server "$KAFKA_ADDRS" --describe --group "$KAFKA_GROUP" --state 2>&1 \
    | grep -qE 'Empty|Dead|does not exist'; do sleep 1; done
fi

# kafka
kafka-server-stop.sh > /dev/null 2>&1
while pgrep -f kafka.Kafka > /dev/null; do sleep 1; done

# mysql nodes, secondaries first so the primary doesn't hand off
for n in 3 2 1; do
  mysqladmin -uroot --socket="$PWD/data/mysql$n.sock" shutdown 2> /dev/null
done
while pgrep -x mysqld > /dev/null; do sleep 1; done

# redis
redis-cli -p "$(echo "${REDIS_ADDR##*:}")" shutdown > /dev/null 2>&1

echo "stop complete!"