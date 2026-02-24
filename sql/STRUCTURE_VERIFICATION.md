# Проверка структуры таблиц и соответствия плану

## ✅ Результаты проверки

### 1. airbyte_raw.sigmasz_leads (нормализованная структура)

**Колонки:**
- ✅ `id` (bigint) — используется `NEW.id`
- ✅ `name` (varchar) — используется `NEW.name`
- ✅ `status_id` (bigint) — используется `NEW.status_id` → приведение к INT в prod_sync/analytics
- ✅ `pipeline_id` (bigint) — используется `NEW.pipeline_id` → приведение к INT
- ✅ `price` (bigint) — используется `NEW.price` → приведение к NUMERIC
- ✅ `created_at` (bigint) — используется `to_timestamp(NEW.created_at::BIGINT)`
- ✅ `updated_at` (bigint) — используется `to_timestamp(NEW.updated_at::BIGINT)`
- ✅ `is_deleted` (boolean) — используется `NEW.is_deleted`
- ✅ `_embedded` (jsonb) — используется `NEW._embedded`
- ✅ `custom_fields_values` (jsonb) — используется `NEW.custom_fields_values`

**План:** ✅ Правильно — используется нормализованная структура (колонки, не `_airbyte_data`).

---

### 2. airbyte_raw.sigmasz_contacts (нормализованная структура)

**Колонки:**
- ✅ `id` (bigint) — используется `NEW.id`
- ✅ `name` (varchar) — используется `NEW.name`
- ✅ `updated_at` (bigint) — используется `to_timestamp(NEW.updated_at::BIGINT)`
- ✅ `is_deleted` (boolean) — используется `NEW.is_deleted`
- ✅ `_embedded` (jsonb) — используется `NEW._embedded`
- ✅ `custom_fields_values` (jsonb) — используется `NEW.custom_fields_values`

**План:** ✅ Правильно.

---

### 3. airbyte_raw.sigmasz_events (нормализованная структура)

**Колонки:**
- ✅ `type` (varchar) — используется `NEW.type` (не `NEW._airbyte_meta ->> 'type'`)
- ✅ `entity_id` (bigint) — используется `NEW.entity_id`
- ✅ `entity_type` (varchar) — используется `NEW.entity_type`

**План:** ✅ Правильно — в `setup_from_scratch_normalized.sql` используется `NEW.type`, `NEW.entity_id`, `NEW.entity_type`.

**Проверка is_deleted в unpack_sigmasz_leads_l2/contacts_l2:**
- ✅ Используется `e.type = 'lead_deleted'` (колонка, не `_airbyte_meta`)

---

### 4. airbyte_raw.sigmasz_custom_fields_leads (нормализованная структура)

**Колонки:**
- ✅ `id` (bigint) — используется `cf.id` и `NEW.id`
- ✅ `name` (varchar) — используется `cf.name`
- ✅ `type` (varchar) — используется `NEW.type`

**План:** ✅ Правильно — используется `cf.id`, `cf.name` (не `cf._airbyte_meta`).

---

### 5. airbyte_raw.sigmasz_custom_fields_contacts (нормализованная структура)

**Колонки:**
- ✅ `id` (bigint) — используется `cf.id` и `NEW.id`
- ✅ `name` (varchar) — используется `cf.name`
- ✅ `type` (varchar) — используется `NEW.type`

**План:** ✅ Правильно.

---

### 6. prod_sync.sigmasz_leads

**Колонки:**
- ✅ `lead_id` (bigint) — PRIMARY KEY
- ✅ `name` (text)
- ✅ `status_id` (integer) — из bigint в airbyte_raw
- ✅ `pipeline_id` (integer) — из bigint в airbyte_raw
- ✅ `price` (numeric) — из bigint в airbyte_raw
- ✅ `created_at` (timestamptz)
- ✅ `updated_at` (timestamptz)
- ✅ `raw_json` (jsonb)
- ✅ `is_deleted` (boolean)
- ✅ `_synced_at` (timestamptz)

**План:** ✅ Правильно. PostgreSQL автоматически приведёт bigint → integer/numeric при INSERT.

---

### 7. prod_sync.sigmasz_contacts

**Колонки:**
- ✅ `contact_id` (bigint) — PRIMARY KEY
- ✅ `name` (text)
- ✅ `updated_at` (timestamptz)
- ✅ `raw_json` (jsonb)
- ✅ `is_deleted` (boolean)
- ✅ `_synced_at` (timestamptz)

**План:** ✅ Правильно.

---

### 8. analytics.sigmasz_leads

**Колонки:**
- ✅ `lead_id` (bigint) — PRIMARY KEY
- ✅ `name` (text)
- ✅ `status_id` (integer)
- ✅ `pipeline_id` (integer)
- ✅ `price` (numeric)
- ✅ `created_at` (timestamptz)
- ✅ `updated_at` (timestamptz)
- ✅ `is_deleted` (boolean)
- ✅ `_synced_at` (timestamptz)
- ➕ Динамические колонки `f_XXXXX` добавляются триггером

**План:** ✅ Правильно.

---

### 9. analytics.sigmasz_contacts

**Колонки:**
- ✅ `contact_id` (bigint) — PRIMARY KEY
- ✅ `name` (text)
- ✅ `updated_at` (timestamptz)
- ✅ `is_deleted` (boolean)
- ✅ `_synced_at` (timestamptz)
- ➕ Динамические колонки `f_XXXXX` добавляются триггером

**План:** ✅ Правильно.

---

## ⚠️ Потенциальные проблемы с типами

### 1. price: bigint → numeric

В `airbyte_raw.sigmasz_leads` колонка `price` имеет тип **bigint**, а в `prod_sync.sigmasz_leads` и `analytics.sigmasz_leads` — **numeric**.

**Текущий код:** `NEW.price` передаётся напрямую.

**Решение:** PostgreSQL автоматически приведёт bigint → numeric при INSERT. Если возникнут ошибки, можно явно привести: `NEW.price::NUMERIC`.

---

### 2. status_id, pipeline_id: bigint → integer

В `airbyte_raw.sigmasz_leads` колонки `status_id` и `pipeline_id` имеют тип **bigint**, а в `prod_sync.sigmasz_leads` и `analytics.sigmasz_leads` — **integer**.

**Текущий код:** `NEW.status_id`, `NEW.pipeline_id` передаются напрямую.

**Решение:** PostgreSQL автоматически приведёт bigint → integer при INSERT (если значение помещается в int4). Если возникнут ошибки или значения > 2^31, можно оставить bigint в prod_sync/analytics или явно привести: `NEW.status_id::INT`.

---

## ✅ Итоговый вердикт

**План соответствует структуре таблиц.**

Все функции в `setup_from_scratch_normalized.sql` используют правильные колонки:
- ✅ `NEW.id`, `NEW.name`, `NEW._embedded`, `NEW.custom_fields_values` (не `_airbyte_data`)
- ✅ `NEW.type`, `NEW.entity_id`, `NEW.entity_type` для events (не `_airbyte_meta`)
- ✅ `cf.id`, `cf.name` для custom_fields (не `cf._airbyte_meta`)

**Рекомендация:** Запустите `sql/setup_from_scratch_normalized.sql` — он должен работать без ошибок. Если появятся ошибки приведения типов (bigint → integer/numeric), добавьте явное приведение в функциях.
