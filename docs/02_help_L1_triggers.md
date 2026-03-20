# Help: L1 Триггеры (airbyte_raw → prod_sync)

## Что это

Автоматические PostgreSQL триггеры, которые срабатывают при записи данных из Airbyte в слой L1 (`airbyte_raw`). Каждый триггер распаковывает «сырые» записи в нормализованные таблицы слоя L2 (`prod_sync`).

## Схема работы

```mermaid
flowchart TD
    subgraph L1["L1 (airbyte_raw)"]
        RAW_L["sigmasz_leads"]
        RAW_C["sigmasz_contacts"]
        RAW_E["sigmasz_events"]
    end

    subgraph Triggers["Триггеры (AFTER INSERT OR UPDATE)"]
        T1["unpack_sigmasz_leads_l2()"]
        T2["unpack_sigmasz_contacts_l2()"]
        T3["propagate_deleted_to_l2()"]
    end

    subgraph L2["L2 (prod_sync)"]
        PS_L["sigmasz_leads"]
        PS_C["sigmasz_contacts"]
        PS_LC["sigmasz_lead_contacts"]
        PS_PH["sigmasz_contact_phones"]
        PS_EM["sigmasz_contact_emails"]
        PS_DEL["deleted_entities_log<br/>(Tombstone Shield)"]
    end

    subgraph DLQ["Обработка ошибок"]
        DL["l2_dead_letter_queue<br/>(карантин битых строк)"]
    end

    RAW_L -- "INSERT/UPDATE" --> T1
    RAW_C -- "INSERT/UPDATE" --> T2
    RAW_E -- "INSERT/UPDATE" --> T3

    T1 --> PS_L
    T1 --> PS_C
    T1 --> PS_LC
    T2 --> PS_C
    T2 --> PS_PH
    T2 --> PS_EM
    T3 --> PS_DEL
    T3 --> PS_L
    T3 --> PS_C

    T1 -.-> DLQ
    T2 -.-> DLQ

    T1 -- "проверяет" --> PS_DEL
    T2 -- "проверяет" --> PS_DEL
```

## Файл исходного кода

📄 [01_rebuild_functions_and_triggers_static.sql](../sql/01_rebuild_functions_and_triggers_static.sql)

---

## Инфраструктурные таблицы (BLOCK 0)

| Таблица | Описание |
|---|---|
| `prod_sync.deleted_entities_log` | **Tombstone Shield** — реестр удалённых сущностей. PK: `(domain, entity_type, entity_id)`. Проверяется перед любым UPSERT в L2 |
| `airbyte_raw.l2_dead_letter_queue` | **Dead Letter Queue** — карантин для записей, вызывающих ошибки. Содержит `raw_record`, `error_message`, `sqlstate` |
| `prod_sync.domain_registry` | Реестр активных доменов (`sigmasz`, `concepta`, `entrum`) для автопровижининга и привязки триггеров |

---

## Вспомогательные функции (BLOCK 1–2)

| Функция | Описание |
|---|---|
| `prod_sync.register_tombstone(domain, entity_type, entity_id)` | Регистрирует удаление сущности в `deleted_entities_log` (UPSERT) |
| `prod_sync.is_tombstoned(domain, entity_type, entity_id)` | Проверяет, находится ли сущность в реестре удалённых |
| `prod_sync.safe_cf_to_timestamp(val)` | Безопасная конвертация строки в `TIMESTAMPTZ`. Поддерживает Unix timestamp (10/13 цифр) |
| `public.normalize_phone(p_raw)` | Нормализация телефона: оставляет только цифры, `8` → `7` для РФ/СНГ |

---

## Триггерные функции (BLOCK 3)

### 1. `airbyte_raw.propagate_deleted_to_l2()` — Tombstone Writer

**Триггер**: `trg_propagate_deleted_to_l2` на `airbyte_raw.sigmasz_events`

**Логика**:
1. Если событие `lead_deleted` → регистрирует tombstone + ставит `is_deleted=TRUE` в `prod_sync.sigmasz_leads`
2. Если событие `contact_deleted` → регистрирует tombstone + ставит `is_deleted=TRUE` в `prod_sync.sigmasz_contacts`
3. При ошибке — `RAISE WARNING` (не блокирует транзакцию)

---

### 2. `airbyte_raw.unpack_sigmasz_leads_l2()` — Распаковщик лидов

**Триггер**: `trg_unpack_sigmasz_leads_l2` на `airbyte_raw.sigmasz_leads`

**Логика**:
1. Проверяет `id` (не NULL) и Tombstone Shield (не удалён?)
2. Если `is_deleted=TRUE` → регистрирует tombstone, удаляет запись и связи из L2, выходит
3. Конвертирует `created_at` и `updated_at` в `TIMESTAMPTZ` (через `safe_cf_to_timestamp`)
4. Безопасно парсит `_embedded` (JSONB object) и `custom_fields_values` (JSONB array)
5. Собирает `raw_json` — объединение ключевых полей для хранения в L2
6. Выполняет monotonic `INSERT ... ON CONFLICT DO UPDATE` в `prod_sync.sigmasz_leads` с guard по `updated_at`
7. Обрабатывает `_embedded.contacts` → вызывает `process_embedded_contacts()`
8. **При ошибках данных** (bad data) → записывает в DLQ, `RAISE WARNING`
9. **При транзиентных ошибках** → `RAISE EXCEPTION` (Airbyte повторит)

---

### 3. `airbyte_raw.unpack_sigmasz_contacts_l2()` — Распаковщик контактов

**Триггер**: `trg_unpack_sigmasz_contacts_l2` на `airbyte_raw.sigmasz_contacts`

**Логика**:
1. Проверяет `id` и Tombstone Shield
2. Если `is_deleted=TRUE` → tombstone + удаляет запись и связи из L2
3. Конвертирует `updated_at`, парсит `custom_fields_values`
4. Monotonic `INSERT ... ON CONFLICT DO UPDATE` в `prod_sync.sigmasz_contacts`
5. Извлекает **телефоны** из кастомных полей (`field_code = 'PHONE'`):
   - Нормализует через `normalize_phone()`
   - Фильтрует: длина ≥ 10 цифр
   - Пересоздаёт все телефоны для контакта (DELETE + INSERT)
6. Извлекает **emails** (`field_code = 'EMAIL'`):
   - Приводит к lower case, trim
   - Пересоздаёт все emails для контакта
7. Ошибки → DLQ / RAISE EXCEPTION

---

## Утилитарные функции (BLOCK 4)

### `prod_sync.process_embedded_contacts(p_contacts_json, p_lead_id, p_explicit_empty)`

Обрабатывает массив контактов из `_embedded.contacts` лида:

1. Если `p_explicit_empty=TRUE` (пустой массив) → удаляет все связи лида с контактами
2. Для каждого контакта в JSON:
   - Проверяет Tombstone Shield
   - UPSERT в `prod_sync.sigmasz_contacts`
   - UPSERT в `prod_sync.sigmasz_lead_contacts` (с полем `is_main`)
3. Удаляет старые связи: контакты, которых нет в новом JSON

---

## Триггеры DDL (BLOCK 5)

| Триггер | Таблица | Функция |
|---|---|---|
| `trg_unpack_sigmasz_leads_l2` | `airbyte_raw.sigmasz_leads` | `unpack_sigmasz_leads_l2()` |
| `trg_unpack_sigmasz_contacts_l2` | `airbyte_raw.sigmasz_contacts` | `unpack_sigmasz_contacts_l2()` |
| `trg_propagate_deleted_to_l2` | `airbyte_raw.sigmasz_events` | `propagate_deleted_to_l2()` |

---

## Индексы (BLOCK 6)

| Индекс | Таблица | Колонки | Назначение |
|---|---|---|---|
| `idx_sigmasz_leads_composite_wm` | `prod_sync.sigmasz_leads` | `(_synced_at, lead_id)` | Composite watermark для L2-потребителей |
| `idx_sigmasz_contacts_composite_wm` | `prod_sync.sigmasz_contacts` | `(_synced_at, contact_id)` | Composite watermark |
| `idx_dlq_active_entity` | `l2_dead_letter_queue` | `(stream_name, entity_id) WHERE resolved=FALSE` | Уникальный для ON CONFLICT в DLQ |
| `idx_dlq_unresolved` | `l2_dead_letter_queue` | `(stream_name, failed_at) WHERE resolved=FALSE` | Быстрый поиск активных ошибок |
| `idx_dlq_failed_at_unresolved` | `l2_dead_letter_queue` | `(failed_at DESC) WHERE resolved=FALSE` | Диагностика по времени |
| `idx_sigmasz_leads_is_deleted` | `prod_sync.sigmasz_leads` | `(is_deleted) WHERE TRUE` | Быстрый поиск удалённых |
| `idx_sigmasz_contacts_is_deleted` | `prod_sync.sigmasz_contacts` | `(is_deleted) WHERE TRUE` | Быстрый поиск удалённых |

---

## Диагностика

```sql
-- Последние удаления?
SELECT * FROM prod_sync.v_recent_tombstones;

-- Неразрешённые ошибки?
SELECT * FROM airbyte_raw.l2_dead_letter_queue WHERE resolved = FALSE ORDER BY failed_at DESC;
```
