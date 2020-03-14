#!/bin/bash -e

DOCKER_DIR="$(dirname "${BASH_SOURCE[0]}")/../docker/dev_env"

cd $DOCKER_DIR && docker-compose up
