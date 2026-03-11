-- ============================================================================
-- Вариант B: L1 (Airbyte) -> buffer (PROD) -> trigger(builder) -> PROD table
-- Таблица: airbyte_raw.sigmasz_leads
--
-- Зачем:
-- - В Airbyte (L1) данные уже нормализованы по колонкам + jsonb в _embedded/custom_fields_values
-- - На PROD хотим таблицу в формате "как у нас принято": типизированные колонки + raw_json jsonb
--
-- КРИТИЧНО:
-- - Для срабатывания триггеров на подписчике нужен ENABLE REPLICA/ALWAYS TRIGGER.
-- - Для обработки DELETE нужен AFTER DELETE (либо общий триггер на INSERT/UPDATE/DELETE).
--
-- ВНИМАНИЕ по рискам:
-- - Logical replication держит replication slot на источнике. Если подписчик/сеть падают, WAL копится.
--   Мониторьте pg_replication_slots и место на диске на Airbyte-сервере.
-- ============================================================================

-- ============================================================================
-- ЧАСТЬ 0 (PROD): помощник для unix timestamp -> timestamptz
--  - Поддерживает секунды и миллисекунды
--  - Возвращает NULL для NULL/нерелевантных значений
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS buffer;

CREATE OR REPLACE FUNCTION buffer.safe_unix_to_timestamptz(p_unix BIGINT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_sec DOUBLE PRECISION;
BEGIN
  IF p_unix IS NULL THEN
    RETURN NULL;
  END IF;

  -- миллисекунды (13+ цифр) -> секунды
  IF p_unix >= 1000000000000 THEN
    v_sec := (p_unix / 1000.0);
  ELSE
    v_sec := p_unix::DOUBLE PRECISION;
  END IF;

  RETURN to_timestamp(v_sec);
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$;

-- ============================================================================
-- ЧАСТЬ 1 (PROD): буферная таблица (1:1 к источнику)
--  - Важно: типы и набор колонок должны совпадать с airbyte_raw.sigmasz_leads
--  - Рекомендуется сгенерировать DDL с Airbyte через pg_dump --schema-only -t ...
-- ============================================================================
DROP TABLE IF EXISTS buffer.incoming_sigmasz_leads CASCADE;

CREATE TABLE buffer.incoming_sigmasz_leads (
  -- служебные поля Airbyte
  _airbyte_raw_id TEXT,
  _airbyte_extracted_at TIMESTAMPTZ,
  _airbyte_meta JSONB,
  _airbyte_generation_id BIGINT,

  -- полезные поля (как в airbyte_raw.sigmasz_leads)
  id BIGINT PRIMARY KEY,
  name TEXT,
  price BIGINT,
  score BIGINT,
  group_id BIGINT,
  _embedded JSONB,
  closed_at BIGINT,
  status_id BIGINT,
  account_id BIGINT,
  created_at BIGINT,
  created_by BIGINT,
  is_deleted BOOLEAN,
  updated_at BIGINT,
  updated_by BIGINT,
  pipeline_id BIGINT,
  loss_reason_id BIGINT,
  closest_task_at BIGINT,
  responsible_user_id BIGINT,
  custom_fields_values JSONB
);

-- ============================================================================
-- ЧАСТЬ 2 (PROD): целевая таблица (куда пишем “как привыкли”)
--  Если у вас уже есть prod_sync.sigmasz_leads на PROD — этот блок можно пропустить.
--  Схема/таблица взяты из вашего репо (sql/00_bootstrap_schemas_and_tables.sql).
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS prod_sync;

CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_leads (
  lead_id BIGINT PRIMARY KEY,
  name TEXT,
  status_id INT,
  pipeline_id INT,
  price NUMERIC,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  raw_json JSONB NOT NULL,
  is_deleted BOOLEAN DEFAULT FALSE,
  _synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- ЧАСТЬ 3 (PROD): builder (триггер) — INSERT/UPDATE/DELETE из buffer -> prod_sync
-- ============================================================================
CREATE OR REPLACE FUNCTION buffer.sync_incoming_sigmasz_leads_to_prod()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  json_payload JSONB;
BEGIN
  -- ФИЗИЧЕСКОЕ УДАЛЕНИЕ (DELETE)
  IF TG_OP = 'DELETE' THEN
    DELETE FROM prod_sync.sigmasz_leads WHERE lead_id = OLD.id;
    RETURN OLD;
  END IF;

  -- ЛОГИЧЕСКОЕ УДАЛЕНИЕ (is_deleted = true)
  IF NEW.is_deleted IS TRUE THEN
    DELETE FROM prod_sync.sigmasz_leads WHERE lead_id = NEW.id;
    RETURN NEW;
  END IF;

  -- Сборка raw_json (держим unix int в JSON как в источнике, а колонки — timestamptz)
  json_payload := jsonb_build_object(
    'id', NEW.id,
    'name', NEW.name,
    'price', NEW.price,
    'score', NEW.score,
    'group_id', NEW.group_id,
    'closed_at', NEW.closed_at,
    'status_id', NEW.status_id,
    'account_id', NEW.account_id,
    'created_at', NEW.created_at,
    'created_by', NEW.created_by,
    'is_deleted', NEW.is_deleted,
    'updated_at', NEW.updated_at,
    'updated_by', NEW.updated_by,
    'pipeline_id', NEW.pipeline_id,
    'loss_reason_id', NEW.loss_reason_id,
    'closest_task_at', NEW.closest_task_at,
    'responsible_user_id', NEW.responsible_user_id,
    '_embedded', COALESCE(NEW._embedded, '{}'::jsonb),
    'custom_fields_values', COALESCE(NEW.custom_fields_values, '[]'::jsonb)
  );

  INSERT INTO prod_sync.sigmasz_leads (
    lead_id,
    name,
    status_id,
    pipeline_id,
    price,
    created_at,
    updated_at,
    raw_json,
    is_deleted,
    _synced_at
  )
  VALUES (
    NEW.id,
    NEW.name,
    NEW.status_id::INT,
    NEW.pipeline_id::INT,
    NEW.price::NUMERIC,
    buffer.safe_unix_to_timestamptz(NEW.created_at),
    buffer.safe_unix_to_timestamptz(NEW.updated_at),
    json_payload,
    FALSE,
    NOW()
  )
  ON CONFLICT (lead_id) DO UPDATE SET
    name = EXCLUDED.name,
    status_id = EXCLUDED.status_id,
    pipeline_id = EXCLUDED.pipeline_id,
    price = EXCLUDED.price,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    raw_json = EXCLUDED.raw_json,
    is_deleted = FALSE,
    _synced_at = NOW();

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_sigmasz_leads ON buffer.incoming_sigmasz_leads;

CREATE TRIGGER trg_sync_sigmasz_leads
AFTER INSERT OR UPDATE OR DELETE ON buffer.incoming_sigmasz_leads
FOR EACH ROW
EXECUTE FUNCTION buffer.sync_incoming_sigmasz_leads_to_prod();

-- КРИТИЧНО: разрешаем триггеру срабатывать от apply-worker подписки
ALTER TABLE buffer.incoming_sigmasz_leads ENABLE ALWAYS TRIGGER trg_sync_sigmasz_leads;

-- ============================================================================
-- ЧАСТЬ 4: публикация/подписка
--  Эти команды выполняются в РАЗНЫХ местах. Здесь они как “шпаргалка”.
-- ============================================================================

-- (AIRBYTE / SOURCE) выполнить на Airbyte-сервере:
-- ---------------------------------------------------------------------------
-- Требования на источнике:
--   wal_level=logical, max_wal_senders>0, max_replication_slots>0, pg_hba.conf для пользователя репликации
--
-- CREATE PUBLICATION pub_airbyte_sigmasz_leads_v1
-- FOR TABLE airbyte_raw.sigmasz_leads
-- WITH (publish = 'insert, update, delete');

-- (PROD / SUBSCRIBER) выполнить на PROD-сервере:
-- ---------------------------------------------------------------------------
-- DROP SUBSCRIPTION IF EXISTS sub_airbyte_sigmasz_leads_v1;
--
-- CREATE SUBSCRIPTION sub_airbyte_sigmasz_leads_v1
-- CONNECTION 'host=<AIRBYTE_HOST> port=5432 dbname=<DB_NAME> user=<REPL_USER> password=<REPL_PASS>'
-- PUBLICATION pub_airbyte_sigmasz_leads_v1
-- WITH (
--   copy_data = true,
--   create_slot = true,
--   enabled = true,
--   synchronous_commit = 'off'
-- );

-- ============================================================================
-- ЧАСТЬ 5 (PROD): быстрые проверки
-- ============================================================================
-- 1) Буфер заполняется?
-- SELECT COUNT(*) FROM buffer.incoming_sigmasz_leads;
--
-- 2) Builder работает?
-- SELECT COUNT(*) FROM prod_sync.sigmasz_leads;
--
-- 3) Один пример:
-- SELECT lead_id, updated_at, raw_json->'custom_fields_values' AS cf
-- FROM prod_sync.sigmasz_leads
-- ORDER BY updated_at DESC
-- LIMIT 5;

