#!/usr/bin/env bash
set -euo pipefail

# Usage: IMAGE=kotstantin/amo-airbyte TAG=1.0.0 ./build.sh
IMAGE="${IMAGE:-kotstantin/amo-airbyte}"
TAG="${TAG:-1.0.0}"

# Конвертация CRLF → LF для Linux-контейнера
sed -i 's/\r$//' Dockerfile build.sh main.py requirements.txt
find source_amo_custom -name "*.py" -exec sed -i 's/\r$//' {} \;

docker build -t "${IMAGE}:${TAG}" .
docker push "${IMAGE}:${TAG}"
