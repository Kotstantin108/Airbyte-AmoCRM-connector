# Итоговая проверка структуры и плана

## ✅ Структура таблиц проверена

По файлу `columns_202601301054.csv` все таблицы имеют **нормализованную структуру** (данные в колонках, не в одной JSON-колонке).

---

## 📊 Сводка по таблицам

| Таблица | Структура | Используется в плане |
|---------|-----------|---------------------|
| `airbyte_raw.sigmasz_leads` | Колонки: `id`, `name`, `price` (bigint), `status_id` (bigint), `pipeline_id` (bigint), `created_at` (bigint), `updated_at` (bigint), `is_deleted`, `_embedded` (jsonb), `custom_fields_values` (jsonb) | ✅ `NEW.id`, `NEW.name`, `NEW.price`, `NEW._embedded`, `NEW.custom_fields_values` |
| `airbyte_raw.sigmasz_contacts` | Колонки: `id`, `name`, `updated_at` (bigint), `is_deleted`, `_embedded` (jsonb), `custom_fields_values` (jsonb) | ✅ `NEW.id`, `NEW.name`, `NEW._embedded`, `NEW.custom_fields_values` |
| `airbyte_raw.sigmasz_events` | Колонки: `type` (varchar), `entity_id` (bigint), `entity_type` (varchar) | ✅ `NEW.type`, `NEW.entity_id`, `NEW.entity_type` |
| `airbyte_raw.sigmasz_custom_fields_leads` | Колонки: `id` (bigint), `name` (varchar), `type` (varchar) | ✅ `cf.id`, `cf.name`, `NEW.id`, `NEW.type` |
| `airbyte_raw.sigmasz_custom_fields_contacts` | Колонки: `id` (bigint), `name` (varchar), `type` (varchar) | ✅ `cf.id`, `cf.name`, `NEW.id`, `NEW.type` |
| `prod_sync.sigmasz_leads` | `lead_id`, `name`, `status_id` (integer), `pipeline_id` (integer), `price` (numeric), `created_at`, `updated_at`, `raw_json`, `is_deleted`, `_synced_at` | ✅ Соответствует |
| `prod_sync.sigmasz_contacts` | `contact_id`, `name`, `updated_at`, `raw_json`, `is_deleted`, `_synced_at` | ✅ Соответствует |
| `analytics.sigmasz_leads` | `lead_id`, `name`, `status_id` (integer), `pipeline_id` (integer), `price` (numeric), `created_at`, `updated_at`, `is_deleted`, `_synced_at` + динамические `f_*` | ✅ Соответствует |
| `analytics.sigmasz_contacts` | `contact_id`, `name`, `updated_at`, `is_deleted`, `_synced_at` + динамические `f_*` | ✅ Соответствует |

---

## ✅ План соответствует структуре

**Все функции в `setup_from_scratch_normalized.sql` используют правильные колонки:**

1. ✅ **L1→L2 (leads/contacts):** `NEW.id`, `NEW.name`, `NEW._embedded`, `NEW.custom_fields_values` — правильно (не `_airbyte_data`).
2. ✅ **Events:** `NEW.type`, `NEW.entity_id`, `NEW.entity_type` — правильно (не `_airbyte_meta ->> 'type'`).
3. ✅ **Custom_fields:** `cf.id`, `cf.name`, `NEW.id`, `NEW.type` — правильно (не `cf._airbyte_meta`).
4. ✅ **Проверка is_deleted:** `e.type = 'lead_deleted'` — правильно (колонка, не `_airbyte_meta`).

---

## ✅ Удалённые сделки и контакты не хранятся в prod_sync и analytics

**Требование:** Ни в продуктовой (`prod_sync`), ни в аналитической (`analytics`) таблицах не должно быть удалённых сделок и контактов.

**Реализация:**
- При обнаружении удаления (флаг `is_deleted` или событие `lead_deleted`/`contact_deleted` в events) выполняется **физическое удаление** строк из `prod_sync` и `analytics`; строка не вставляется и не обновляется с флагом удаления.
- В триггерах L1→L2 (`unpack_sigmasz_leads_l2`, `unpack_sigmasz_contacts_l2`): если сделка/контакт удалён — только `DELETE` из prod_sync и analytics, затем `RETURN`; иначе — `INSERT` с `is_deleted = FALSE`.
- В триггере на events (`propagate_deleted_to_l2`): при событии удаления — только `DELETE` из prod_sync и analytics.
- В backfill (`backfill_initial_normalized.sql`, `run_all_normalized.sql`): для удалённых — `DELETE` и пропуск вставки; вставляются только неудалённые с `is_deleted = FALSE`.

В результате в `prod_sync` и `analytics` присутствуют только активные сделки и контакты; колонка `is_deleted` в этих таблицах фактически всегда `FALSE` (оставлена для совместимости схемы).

---

## ⚠️ Потенциальные проблемы с типами

### 1. price: bigint → numeric

**Проблема:** В `airbyte_raw.sigmasz_leads` колонка `price` имеет тип **bigint**, а в `prod_sync.sigmasz_leads` и `analytics.sigmasz_leads` — **numeric**.

**Текущий код:** `NEW.price` передаётся напрямую в INSERT.

**Решение:** PostgreSQL автоматически приведёт bigint → numeric при INSERT. Если возникнут ошибки типа `ERROR: column "price" is of type numeric but expression is of type bigint`, добавьте явное приведение: `NEW.price::NUMERIC`.

---

### 2. status_id, pipeline_id: bigint → integer

**Проблема:** В `airbyte_raw.sigmasz_leads` колонки `status_id` и `pipeline_id` имеют тип **bigint**, а в `prod_sync.sigmasz_leads` и `analytics.sigmasz_leads` — **integer**.

**Текущий код:** `NEW.status_id`, `NEW.pipeline_id` передаются напрямую.

**Решение:** PostgreSQL автоматически приведёт bigint → integer при INSERT (если значение помещается в int4, т.е. ≤ 2^31-1). Если возникнут ошибки или значения > 2^31, можно:
- Оставить bigint в prod_sync/analytics (изменить структуру таблиц)
- Или явно привести: `NEW.status_id::INT`, `NEW.pipeline_id::INT` (с потерей данных, если > 2^31)

---

## 🎯 Что запускать и в каком порядке

**Если БД/объекты снесены (установка с нуля):**

| Шаг | Скрипт | Что делает |
|-----|--------|------------|
| **0** | `sql/00_bootstrap_schemas_and_tables.sql` | Схемы prod_sync, analytics; таблицы L2 и L3; `normalize_phone()`, `process_embedded_contacts()`. |
| **1** | `sql/setup_from_scratch_normalized.sql` | Триггеры и функции L1→L2, L2→L3, view. |
| **2** | `sql/backfill_initial_normalized.sql` | Перенос данных из L1 в L2 и L3. |

Схему `airbyte_raw` создаёт Airbyte при синхронизации — перед шагом 1 должен быть хотя бы один успешный sync.

```bash
psql -f sql/00_bootstrap_schemas_and_tables.sql
psql -f sql/setup_from_scratch_normalized.sql
psql -f sql/backfill_initial_normalized.sql
```

**Если схемы и таблицы L2/L3 уже есть** (только пересоздать триггеры):

| Шаг | Скрипт |
|-----|--------|
| 1 | `sql/setup_from_scratch_normalized.sql` |
| 2 | `sql/backfill_initial_normalized.sql` |

**Альтернатива (один файл):** вместо шагов 1–2 можно запустить `sql/run_all_normalized.sql` — настройка триггеров/функций + backfill (но не создаёт схемы/таблицы с нуля).

---

## 🎯 Рекомендации

### 1. Запустить setup_from_scratch_normalized.sql

**Файл:** `sql/setup_from_scratch_normalized.sql`

Он должен работать без ошибок, т.к. все колонки используются правильно.

```bash
psql -f sql/setup_from_scratch_normalized.sql
```

---

### 2. Если появятся ошибки приведения типов

Добавьте явное приведение в функции `unpack_sigmasz_leads_l2`:

```sql
-- В строке 39 замените:
INSERT INTO prod_sync.sigmasz_leads (..., status_id, pipeline_id, price, ...)
VALUES (..., NEW.status_id::INT, NEW.pipeline_id::INT, NEW.price::NUMERIC, ...)
```

Но сначала попробуйте без явного приведения — PostgreSQL обычно справляется автоматически.

---

### 3. После успешного запуска setup

Запустите backfill для переноса уже существующих данных:

```bash
psql -f sql/backfill_initial_normalized.sql
```

---

## ✅ Итог

**План полностью соответствует структуре таблиц.** Все функции используют правильные колонки. Запускайте `setup_from_scratch_normalized.sql` — он должен работать.
