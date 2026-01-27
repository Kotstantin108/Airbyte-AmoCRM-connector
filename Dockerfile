FROM python:3.10-slim

WORKDIR /airbyte/integration_code

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY source_amo_custom ./source_amo_custom
COPY main.py .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"

ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
