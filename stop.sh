#!/bin/bash
cd "$(dirname "$0")"
source env.sh

# api, consumers, tweet nodes
pkill -f 'go run ./cmd/(api|consumer|server)'
pkill -f '/exe/(api|consumer|server)( |$)'

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
