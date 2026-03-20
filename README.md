# AmoCRM Custom Connector для Airbyte

Python CDK коннектор для синхронизации данных из AmoCRM в PostgreSQL через Airbyte.

## Архитектура (активная)

```
AmoCRM API
    │
    ▼  (HTTP REST, OAuth2 token из PG)
Custom Python Connector (Docker в Airbyte)
    │
    ▼  (Airbyte sync job → записывает в PG)
airbyte_raw (L1) — сырые данные (sigmasz / concepta / entrum)
    │
    ▼  (PostgreSQL AFTER INSERT/UPDATE триггеры — мгновенно)
prod_sync (L2) — нормализованные таблицы
    │
    └──▶ amo_support_schema (PROD) — копия на другом сервере
            (PostgreSQL FDW + функции sync_*_smart(), запускаются через n8n каждые 5–10 мин)
```

**Домены:** `sigmasz`, `concepta`, `entrum`

## Структура проекта

```
source_amo_custom/   — Python код кастомного коннектора
Dockerfile           — сборка Docker-образа
build.sh             — сборка и публикация образа
requirements.txt     — зависимости Python

sql/
  00_bootstrap_schemas_and_tables.sql          — схемы, таблицы L2, вспомогательные функции
  01_rebuild_functions_and_triggers_static.sql — триггеры и функции L1→L2 (все домены)
  02_prod_fdw_sync.sql                         — функции L2→PROD FDW sync (все домены)
  archive/                                     — устаревшие и переходные скрипты

docs/
  00_overview.md              — обзор архитектуры
  01_help_custom_connector.md — детали коннектора
  02_help_L1_triggers.md      — детали триггеров L1→L2
  03_help_L2_L3_sync.md       — детали FDW синхронизации
  archive/                    — устаревшие документы
```

## Порядок установки с нуля

### Шаг 1. Настройка PostgreSQL (Analytics-сервер)

Создайте таблицу для OAuth-токенов:

```sql
CREATE TABLE amo_tokens (
    domain VARCHAR(255) PRIMARY KEY,
    access_token TEXT,
    refresh_token TEXT NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    client_secret VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Шаг 2. Запустить bootstrap (схемы и таблицы L2)

```bash
psql -f sql/00_bootstrap_schemas_and_tables.sql
```

### Шаг 3. Запустить первую синхронизацию Airbyte

Airbyte создаёт схему `airbyte_raw` и таблицы в ней автоматически при первом sync.

### Шаг 4. Создать функции и триггеры L1→L2

```bash
psql -f sql/01_rebuild_functions_and_triggers_static.sql
```

После этого триггеры автоматически переносят данные из `airbyte_raw` в `prod_sync` при каждой синхронизации Airbyte.

Если таблицы для `concepta` / `entrum` в `airbyte_raw` уже существуют, подключите триггеры вручную:

```sql
SELECT prod_sync.attach_domain_triggers('concepta');
SELECT prod_sync.attach_domain_triggers('entrum');
```

### Шаг 5. Настройка FDW и PROD-синхронизации (PROD-сервер)

Выполнить **на PROD-сервере** (где расположена `amo_support_schema`):

```sql
-- 1. Настроить FDW (один раз)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER airbyte_analytics_server FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '<analytics_host>', port '5432', dbname '<analytics_db>');
CREATE USER MAPPING FOR CURRENT_USER SERVER airbyte_analytics_server
    OPTIONS (user '<readonly_user>', password '<password>');

-- 2. Импортировать foreign tables
CREATE SCHEMA IF NOT EXISTS airbyte_remote;
IMPORT FOREIGN SCHEMA "prod_sync"
    LIMIT TO (sigmasz_leads, sigmasz_contacts, sigmasz_lead_contacts,
              sigmasz_contact_phones, sigmasz_contact_emails,
              concepta_leads, concepta_contacts, concepta_lead_contacts,
              concepta_contact_phones, concepta_contact_emails,
              entrum_leads, entrum_contacts, entrum_lead_contacts,
              entrum_contact_phones, entrum_contact_emails)
    FROM SERVER airbyte_analytics_server INTO airbyte_remote;

-- 3. Создать sync-функции
\i sql/02_prod_fdw_sync.sql
```

### Шаг 6. Запланировать синхронизацию через n8n

Добавить в n8n workflow (каждые 5–10 минут):

```sql
SELECT * FROM amo_support_schema.sync_sigmasz_smart();
SELECT * FROM amo_support_schema.sync_concepta_smart();
SELECT * FROM amo_support_schema.sync_entrum_smart();
```

## Сборка и публикация коннектора

```bash
./build.sh

# или явно задать образ
IMAGE=kotstantin/amo-airbyte ./build.sh

# или вручную задать TAG
IMAGE=kotstantin/amo-airbyte TAG=3.9.0 ./build.sh
```

Если `TAG` не передан, `build.sh` генерирует его автоматически в формате
`YYYYMMDD-HHMMSS-<short_sha>`, например `20260320-110907-ae44436`.

Скрипт автоматически конвертирует CRLF → LF для всех файлов.

## Использование в Airbyte

1. Создайте **Custom Source** в Airbyte UI
2. Укажите Docker образ: `kotstantin/amo-airbyte:3.9.0`
3. Заполните параметры подключения:
   - **Domain** — субдомен AmoCRM (например, `sigmasz`)
   - **Client ID** и **Client Secret** — из настроек интеграции AmoCRM
   - **PostgreSQL credentials** — для доступа к таблице `amo_tokens`
   - **Start Date** — Unix timestamp начала синхронизации:
     - `≤ 1420070400` (1 янв 2015) — полная загрузка всех данных
     - `> 1420070400` — инкрементальная синхронизация с указанной даты

## Потоки данных

| Поток | Тип | Описание |
|-------|-----|----------|
| `leads` | Incremental | Сделки с контактами |
| `contacts` | Incremental | Контакты |
| `events` | Incremental | События удаления (`lead_deleted`, `contact_deleted`) |
| `pipelines` | Full Refresh | Воронки продаж |
| `custom_fields_leads` | Full Refresh | Кастомные поля для сделок |
| `custom_fields_contacts` | Full Refresh | Кастомные поля для контактов |
| `users` | Full Refresh | Пользователи |

## Особенности

- **Rate limiting**: задержка 0.1 сек между запросами (лимит AmoCRM: 7 req/sec)
- **Токены**: автоматическое обновление через PostgreSQL с блокировкой
- **Пагинация**: максимум 250 записей на страницу (лимит AmoCRM API)
- **Обработка ошибок**: retry для 429 (rate limit) и 5xx ошибок
- **Tombstone Shield**: защита от повторной вставки удалённых записей
- **Dead Letter Queue**: карантин для строк с ошибками обработки
- **Ghost Busting**: периодическая (раз в час) очистка удалённых записей в PROD
- **Advisory lock**: защита от параллельных sync-запусков

## Разработка

```bash
# Установка зависимостей
pip install -r requirements.txt

# Тестирование spec
python main.py spec

# Тестирование discover
python main.py discover --config '{"domain":"test","client_id":"...","client_secret":"...","db_host":"...","db_port":5432,"db_name":"...","db_user":"...","db_password":"..."}'
```
