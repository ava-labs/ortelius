#!/bin/bash

# Run orteliusd
# Extracted from docker/docker-compose.yml
# For some reason it's ignoring the -c flag

./build/orteliusd  stream-producer &
./build/orteliusd  stream-consumer &
./build/orteliusd  api &