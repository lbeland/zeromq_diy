#!/bin/bash

# Start consumer in background
python consumer.py &
CONSUMER_PID=$!

# Wait a moment for consumer to initialize
sleep 1

# Start producer in background
python producer.py &
PRODUCER_PID=$!

echo "Started consumer (PID: $CONSUMER_PID) and producer (PID: $PRODUCER_PID)"
echo "Press Ctrl+C to stop both processes"

wait