#!/usr/bin/env bash
set -euo pipefail

# Отменяем локальные изменения (CRLF конверсии от прошлых сборок), 
# иначе git pull выдаст ошибку конфликта
git restore . 2>/dev/null || git checkout . 2>/dev/null

# Подтягиваем свежий код
echo "Скачиваем последние изменения с GitHub..."
git pull origin main

# Usage: IMAGE=kotstantin/amo-airbyte TAG=1.0.0 ./build.sh
IMAGE="${IMAGE:-kotstantin/amo-airbyte}"
TAG="${TAG:-1.0.0}"

# Конвертация CRLF → LF для Linux-контейнера
sed -i 's/\r$//' Dockerfile build.sh main.py requirements.txt
find source_amo_custom -name "*.py" -exec sed -i 's/\r$//' {} \;

docker build -t "${IMAGE}:${TAG}" .
docker push "${IMAGE}:${TAG}"
