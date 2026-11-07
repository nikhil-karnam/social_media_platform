#!/bin/bash
cd "$(dirname "$0")"
source env.sh

sql() { mysql -uroot --socket="$PWD/data/mysql$1.sock" -N -e "$2"; }
port() { echo "${1##*:}"; }

# 1. start redis
if ! redis-cli -p "$(port "$REDIS_ADDR")" ping > /dev/null 2>&1; then
  redis-server --port "$(port "$REDIS_ADDR")" --daemonize yes
fi

# 2. lsm data folders
mkdir -p data/tweets0 data/tweets1 data/tweets2

# 3. mysql data folders
for n in 1 2 3; do
  if [ ! -d "data/mysql$n" ]; then
    mysqld --initialize-insecure --datadir="$PWD/data/mysql$n"
  fi
done

# 4. kafka data folder
KAFKA_CONFIG="$(dirname "$(command -v kafka-server-start.sh)")/../config/server.properties"
sed "s|^log.dirs=.*|log.dirs=$PWD/data/kafka|" "$KAFKA_CONFIG" > data/kafka.properties
if [ ! -f data/kafka/meta.properties ]; then
  kafka-storage.sh format --standalone -t "$(kafka-storage.sh random-uuid)" -c data/kafka.properties
fi

# 5. grpc tweet nodes
for i in 0 1 2; do
  addr_var="TWEET${i}_ADDR"
  setsid go run ./cmd/server "data/tweets$i" ":$(port "${!addr_var}")" &
done

# 6. mysql nodes
base=$(port "$MYSQL_ADDR")
for n in 1 2 3; do
  mysqld \
    --datadir="$PWD/data/mysql$n" \
    --port=$((base + n - 1)) \
    --socket="$PWD/data/mysql$n.sock" \
    --mysqlx=OFF \
    --server-id=$n \
    --gtid-mode=ON \
    --enforce-gtid-consistency=ON \
    --report-host=127.0.0.1 \
    --plugin-load-add=group_replication.so \
    --group-replication-group-name=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
    --group-replication-start-on-boot=OFF \
    --group-replication-local-address=127.0.0.1:$((33060 + n)) \
    --group-replication-group-seeds=127.0.0.1:33061,127.0.0.1:33062,127.0.0.1:33063 \
    --group-replication-recovery-get-public-key=ON &
done
for n in 1 2 3; do
  until mysqladmin -uroot --socket="$PWD/data/mysql$n.sock" ping > /dev/null 2>&1; do sleep 1; done
done

# 7. link them with group replication
RPL="SET SQL_LOG_BIN=0;
CREATE USER IF NOT EXISTS rpl_user@'%' IDENTIFIED BY 'rpl_pass';
GRANT REPLICATION SLAVE, CONNECTION_ADMIN, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO rpl_user@'%';
SET SQL_LOG_BIN=1;
CHANGE REPLICATION SOURCE TO SOURCE_USER='rpl_user', SOURCE_PASSWORD='rpl_pass' FOR CHANNEL 'group_replication_recovery';"
sql 1 "$RPL SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group=OFF;"
until [ "$(sql 1 "SELECT COUNT(*) FROM performance_schema.replication_group_members WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'")" = 1 ]; do sleep 1; done
sql 2 "$RPL START GROUP_REPLICATION;"
sql 3 "$RPL START GROUP_REPLICATION;"

# 8. load schema
mysql -uroot --socket="$PWD/data/mysql1.sock" < schema.sql

# 9. start kafka
setsid kafka-server-start.sh data/kafka.properties &
until kafka-topics.sh --bootstrap-server "$KAFKA_ADDRS" --list > /dev/null 2>&1; do sleep 1; done

# 10. fanout topic with LANES partitions
kafka-topics.sh --bootstrap-server "$KAFKA_ADDRS" --create --if-not-exists \
  --topic "$KAFKA_TOPIC" --partitions "$LANES" --replication-factor 1

# 11. LANES consumers
for i in $(seq 1 "$LANES"); do
  setsid go run ./cmd/consumer &
done

# 12. api
setsid go run ./cmd/api &
echo "start complete!"
