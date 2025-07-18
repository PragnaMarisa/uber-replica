#!/bin/bash

# Topic configuration
TOPIC1="driver_status"
TOPIC2="driver_current_status"
PARTITIONS=3
REPLICATION=1
BOOTSTRAP_SERVER="localhost:9092"

echo "🔄 Deleting topic: $TOPIC1 ..."
kafka-topics \
  --delete \
  --topic "$TOPIC1" \
  --bootstrap-server "$BOOTSTRAP_SERVER"

# Small wait to ensure deletion completes
sleep 2

echo "✅ Recreating topic: $TOPIC1 ..."
kafka-topics \
  --create \
  --topic "$TOPIC1" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --bootstrap-server "$BOOTSTRAP_SERVER"

echo "🎯 Topic reset complete."

echo "🔄 Deleting topic: $TOPIC2 ..."
kafka-topics \
  --delete \
  --topic "$TOPIC2" \
  --bootstrap-server "$BOOTSTRAP_SERVER"

# Small wait to ensure deletion completes
sleep 2

echo "✅ Recreating topic: $TOPIC2 ..."
kafka-topics \
  --create \
  --topic "$TOPIC2" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --bootstrap-server "$BOOTSTRAP_SERVER"

echo "🎯 Topic reset complete."
