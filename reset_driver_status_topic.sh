#!/bin/bash

# Topic configuration
TOPIC="driver_status"
PARTITIONS=3
REPLICATION=1
BOOTSTRAP_SERVER="localhost:9092"

echo "🔄 Deleting topic: $TOPIC ..."
kafka-topics \
  --delete \
  --topic "$TOPIC" \
  --bootstrap-server "$BOOTSTRAP_SERVER"

# Small wait to ensure deletion completes
sleep 2

echo "✅ Recreating topic: $TOPIC ..."
kafka-topics \
  --create \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION" \
  --bootstrap-server "$BOOTSTRAP_SERVER"

echo "🎯 Topic reset complete."
