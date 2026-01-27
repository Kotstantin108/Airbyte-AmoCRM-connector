# AmoCRM Custom Connector для Airbyte

Python CDK коннектор для синхронизации данных из AmoCRM в PostgreSQL через Airbyte.

## Структура проекта

- `main.py` — точка входа для Airbyte CDK
- `source_amo_custom/source.py` — основная логика коннектора:
  - OAuth2 аутентификация через PostgreSQL (таблица `amo_tokens`)
  - Потоки данных: `leads`, `contacts`, `events`, `pipelines`, `custom_fields_leads`, `custom_fields_contacts`, `users`
  - Инкрементальная синхронизация для `leads`, `contacts`, `events`
  - Full refresh для справочников
- `Dockerfile` — сборка Docker образа
- `build.sh` — скрипт сборки и публикации образа
- `requirements.txt` — зависимости Python
- `sql/triggers_is_deleted.sql` — триггеры PostgreSQL для обработки удалённых записей

## Требования

- PostgreSQL база с таблицей `amo_tokens` для хранения OAuth токенов
- AmoCRM интеграция с `client_id` и `client_secret`
- Airbyte (self-hosted через `abctl` или cloud)

## Настройка PostgreSQL

Создайте таблицу для токенов:

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

## Сборка и публикация

```bash
IMAGE=kotstantin/amo-airbyte TAG=3.9.0 ./build.sh
```

Скрипт автоматически конвертирует CRLF → LF для всех файлов.

## Использование в Airbyte

1. Создайте **Custom Source** в Airbyte UI
2. Укажите Docker образ: `kotstantin/amo-airbyte:3.9.0`
3. Заполните параметры подключения:
   - **Domain** — субдомен AmoCRM (например, `mycompany`)
   - **Client ID** и **Client Secret** — из настроек интеграции AmoCRM
   - **PostgreSQL credentials** — для доступа к таблице `amo_tokens`
   - **Start Date** — Unix timestamp начала синхронизации:
     - `≤ 1420070400` (1 янв 2015) — полная загрузка всех данных
     - `> 1420070400` — инкрементальная синхронизация с указанной даты

## Режимы синхронизации

### Full Load Mode (`start_date ≤ 1420070400`)
- Загружает **все** записи без фильтров по дате
- Использует `order[id]=asc` для стабильной пагинации
- Игнорирует сохранённое состояние (cursor)

### Incremental Mode (`start_date > 1420070400`)
- Загружает только изменённые записи с `updated_at >= start_date`
- Использует cursor для отслеживания прогресса
- Применяет фильтр `filter[updated_at][from]`

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

## Обработка удалённых записей

После синхронизации выполните SQL скрипт для проставления `is_deleted`:

```sql
-- Создать триггеры и функции
\i sql/triggers_is_deleted.sql

-- Обработать все events после перезагрузки
SELECT * FROM airbyte_raw.process_all_deleted_events();
```

## Особенности

- **Rate limiting**: задержка 0.1 сек между запросами (лимит AmoCRM: 7 req/sec)
- **Токены**: автоматическое обновление через PostgreSQL с блокировкой
- **Пагинация**: максимум 250 записей на страницу (лимит AmoCRM API)
- **Обработка ошибок**: retry для 429 (rate limit) и 5xx ошибок

## Разработка

Для локальной разработки:

```bash
# Установка зависимостей
pip install -r requirements.txt

# Тестирование spec
python main.py spec

# Тестирование discover
python main.py discover --config '{"domain":"test","client_id":"...","client_secret":"...","db_host":"...","db_port":5432,"db_name":"...","db_user":"...","db_password":"..."}'
```
