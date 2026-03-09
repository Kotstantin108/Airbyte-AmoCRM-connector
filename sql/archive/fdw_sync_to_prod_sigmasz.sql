-- ============================================================================
-- FDW-синхронизация L2 (prod_sync на Analytics) -> PROD (amo_support_schema)
-- Домен: sigmasz
--
-- Предпосылки:
--   - L2 = prod_sync.* на Analytics-сервере (источник истины)
--   - На PROD уже настроен FDW к Analytics и есть foreign-таблицы:
--       airbyte_remote.sigmasz_leads
--       airbyte_remote.sigmasz_contacts
--       airbyte_remote.sigmasz_lead_contacts
--   - На PROD уже существуют продуктовые таблицы:
--       amo_support_schema.sigmasz_leads
--       amo_support_schema.sigmasz_contacts
--       amo_support_schema.sigmasz_lead_contacts
--
-- Цель:
--   - Регулярно приводить PROD к состоянию L2:
--       * upsert всех строк из L2
--       * удалить строки, которых больше нет в L2 (или is_deleted = true)
--   - Удаления по вебхуку в n8n допускаются:
--       * синк может временно "вернуть" лид, пока L2 ещё его считает живым
--       * после того, как удаление дойдёт до L2, синк его снова удалит
--
-- ВАЖНО:
--   - Эти функции не используют промежуточную локальную копию (airbyte_sync.*),
--     а читают напрямую из airbyte_remote.* и пишут сразу в amo_support_schema.*
--   - Вызываются из n8n как обычный SELECT, например:
--       SELECT * FROM amo_support_schema.sync_sigmasz_all_from_l2_full();
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS amo_support_schema;

-- ============================================================================
-- 0. Приведение PROD-структуры к ожидаемой (добавляем недостающие поля)
-- ============================================================================

-- Лиды: добавляем price/created_at/is_deleted, если их нет
ALTER TABLE amo_support_schema.sigmasz_leads
  ADD COLUMN IF NOT EXISTS price NUMERIC,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;

-- Контакты: убеждаемся, что есть raw_json для хранения снапшота L2
ALTER TABLE amo_support_schema.sigmasz_contacts
  ADD COLUMN IF NOT EXISTS raw_json TEXT;

-- Телефоны и email: базовые таблицы, если ещё не созданы
CREATE TABLE IF NOT EXISTS amo_support_schema.sigmasz_contact_phones (
  contact_id BIGINT NOT NULL,
  phone TEXT NOT NULL,
  PRIMARY KEY (contact_id, phone)
);

CREATE TABLE IF NOT EXISTS amo_support_schema.sigmasz_contact_emails (
  contact_id BIGINT NOT NULL,
  email TEXT NOT NULL,
  PRIMARY KEY (contact_id, email)
);

-- ============================================================================
-- 1. Синхронизация LEADS: airbyte_remote.sigmasz_leads -> amo_support_schema.sigmasz_leads
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_leads_from_l2_full()
RETURNS TABLE (
  inserted_count BIGINT,
  updated_count BIGINT,
  deleted_count BIGINT
) AS $$
DECLARE
  v_ins BIGINT := 0;
  v_upd BIGINT := 0;
  v_del BIGINT := 0;
BEGIN
  -- Upsert всех НЕудалённых лидов из L2
  WITH src AS (
    SELECT
      l.lead_id,
      l.name,
      l.status_id,
      l.pipeline_id,
      l.price,
      l.created_at,
      l.updated_at,
      l.raw_json,
      l.is_deleted
    FROM airbyte_remote.sigmasz_leads l
    WHERE l.is_deleted IS FALSE
  ),
  upserted AS (
    INSERT INTO amo_support_schema.sigmasz_leads (
      lead_id,
      name,
      status_id,
      pipeline_id,
      updated_at,
      raw_json
      -- при необходимости можно добавить price/created_at/is_deleted,
      -- если они есть в структуре amo_support_schema.sigmasz_leads
    )
    SELECT
      s.lead_id,
      s.name,
      s.status_id,
      s.pipeline_id,
      s.updated_at,
      s.raw_json::TEXT
    FROM src s
    ON CONFLICT (lead_id) DO UPDATE SET
      name       = EXCLUDED.name,
      status_id  = EXCLUDED.status_id,
      pipeline_id= EXCLUDED.pipeline_id,
      updated_at = EXCLUDED.updated_at,
      raw_json   = EXCLUDED.raw_json
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COUNT(*) FILTER (WHERE inserted),
    COUNT(*) FILTER (WHERE NOT inserted)
  INTO v_ins, v_upd
  FROM upserted;

  -- Удаляем из PROD всех лидов, которых нет в L2
  WITH l2_ids AS (
    SELECT l.lead_id
    FROM airbyte_remote.sigmasz_leads l
    WHERE l.is_deleted IS FALSE
  ),
  deleted AS (
    DELETE FROM amo_support_schema.sigmasz_leads p
    WHERE NOT EXISTS (
      SELECT 1 FROM l2_ids s WHERE s.lead_id = p.lead_id
    )
    RETURNING p.lead_id
  )
  SELECT COUNT(*) INTO v_del FROM deleted;

  inserted_count := v_ins;
  updated_count  := v_upd;
  deleted_count  := v_del;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 3. Синхронизация CONTACT_PHONES:
--      airbyte_remote.sigmasz_contact_phones -> amo_support_schema.sigmasz_contact_phones
--  Подход: полная замена (TRUNCATE + INSERT) для производной таблицы
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_contact_phones_from_l2_full()
RETURNS TABLE (
  inserted_count BIGINT
) AS $$
DECLARE
  v_ins BIGINT := 0;
BEGIN
  TRUNCATE TABLE amo_support_schema.sigmasz_contact_phones;

  WITH src AS (
    SELECT DISTINCT
      p.contact_id,
      p.phone
    FROM airbyte_remote.sigmasz_contact_phones p
    JOIN airbyte_remote.sigmasz_contacts c ON c.contact_id = p.contact_id
    WHERE c.is_deleted IS FALSE
  ),
  inserted AS (
    INSERT INTO amo_support_schema.sigmasz_contact_phones (
      contact_id,
      phone
    )
    SELECT
      s.contact_id,
      s.phone
    FROM src s
    RETURNING contact_id
  )
  SELECT COUNT(*) INTO v_ins FROM inserted;

  inserted_count := v_ins;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 4. Синхронизация CONTACT_EMAILS:
--      airbyte_remote.sigmasz_contact_emails -> amo_support_schema.sigmasz_contact_emails
--  Подход: полная замена (TRUNCATE + INSERT)
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_contact_emails_from_l2_full()
RETURNS TABLE (
  inserted_count BIGINT
) AS $$
DECLARE
  v_ins BIGINT := 0;
BEGIN
  TRUNCATE TABLE amo_support_schema.sigmasz_contact_emails;

  WITH src AS (
    SELECT DISTINCT
      e.contact_id,
      e.email
    FROM airbyte_remote.sigmasz_contact_emails e
    JOIN airbyte_remote.sigmasz_contacts c ON c.contact_id = e.contact_id
    WHERE c.is_deleted IS FALSE
  ),
  inserted AS (
    INSERT INTO amo_support_schema.sigmasz_contact_emails (
      contact_id,
      email
    )
    SELECT
      s.contact_id,
      s.email
    FROM src s
    RETURNING contact_id
  )
  SELECT COUNT(*) INTO v_ins FROM inserted;

  inserted_count := v_ins;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 2. Синхронизация CONTACTS: airbyte_remote.sigmasz_contacts -> amo_support_schema.sigmasz_contacts
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_contacts_from_l2_full()
RETURNS TABLE (
  inserted_count BIGINT,
  updated_count BIGINT,
  deleted_count BIGINT
) AS $$
DECLARE
  v_ins BIGINT := 0;
  v_upd BIGINT := 0;
  v_del BIGINT := 0;
BEGIN
  WITH src AS (
    SELECT
      c.contact_id,
      c.name,
      c.updated_at,
      c.raw_json,
      c.is_deleted
    FROM airbyte_remote.sigmasz_contacts c
    WHERE c.is_deleted IS FALSE
  ),
  upserted AS (
    INSERT INTO amo_support_schema.sigmasz_contacts (
      contact_id,
      name,
      updated_at,
      raw_json
    )
    SELECT
      s.contact_id,
      s.name,
      s.updated_at,
      s.raw_json::TEXT
    FROM src s
    ON CONFLICT (contact_id) DO UPDATE SET
      name       = EXCLUDED.name,
      updated_at = EXCLUDED.updated_at,
      raw_json   = EXCLUDED.raw_json
    RETURNING (xmax = 0) AS inserted
  )
  SELECT
    COUNT(*) FILTER (WHERE inserted),
    COUNT(*) FILTER (WHERE NOT inserted)
  INTO v_ins, v_upd
  FROM upserted;

  WITH l2_ids AS (
    SELECT c.contact_id
    FROM airbyte_remote.sigmasz_contacts c
    WHERE c.is_deleted IS FALSE
  ),
  deleted AS (
    DELETE FROM amo_support_schema.sigmasz_contacts p
    WHERE NOT EXISTS (
      SELECT 1 FROM l2_ids s WHERE s.contact_id = p.contact_id
    )
    RETURNING p.contact_id
  )
  SELECT COUNT(*) INTO v_del FROM deleted;

  inserted_count := v_ins;
  updated_count  := v_upd;
  deleted_count  := v_del;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 5. Синхронизация LEAD_CONTACTS (связи):
--      airbyte_remote.sigmasz_lead_contacts -> amo_support_schema.sigmasz_lead_contacts
--  Подход: полная замена (TRUNCATE + INSERT)
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_lead_contacts_from_l2_full()
RETURNS TABLE (
  inserted_count BIGINT
) AS $$
DECLARE
  v_ins BIGINT := 0;
BEGIN
  -- Полностью пересоздаём связи. Допустимо, т.к. это производная таблица.
  TRUNCATE TABLE amo_support_schema.sigmasz_lead_contacts;

  WITH src AS (
    SELECT DISTINCT
      lc.lead_id,
      lc.contact_id
    FROM airbyte_remote.sigmasz_lead_contacts lc
    JOIN airbyte_remote.sigmasz_leads    l ON l.lead_id    = lc.lead_id
    JOIN airbyte_remote.sigmasz_contacts c ON c.contact_id = lc.contact_id
    WHERE l.is_deleted IS FALSE
      AND c.is_deleted IS FALSE
  ),
  inserted AS (
    INSERT INTO amo_support_schema.sigmasz_lead_contacts (
      lead_id,
      contact_id
    )
    SELECT
      s.lead_id,
      s.contact_id
    FROM src s
    RETURNING lead_id
  )
  SELECT COUNT(*) INTO v_ins FROM inserted;

  inserted_count := v_ins;
  RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 4. Общая функция: синхронизировать всё за один вызов
--    Вызывать её из n8n:
--      SELECT * FROM amo_support_schema.sync_sigmasz_all_from_l2_full();
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_all_from_l2_full()
RETURNS TABLE (
  entity_name TEXT,
  inserted BIGINT,
  updated BIGINT,
  deleted BIGINT
) AS $$
DECLARE
  v_leads    RECORD;
  v_contacts RECORD;
  v_phones   RECORD;
  v_emails   RECORD;
  v_links    RECORD;
BEGIN
  -- 1. Leads
  SELECT * INTO v_leads
  FROM amo_support_schema.sync_sigmasz_leads_from_l2_full();

  RETURN QUERY
  SELECT 'sigmasz_leads'::TEXT,
         v_leads.inserted_count,
         v_leads.updated_count,
         v_leads.deleted_count;

  -- 2. Contacts
  SELECT * INTO v_contacts
  FROM amo_support_schema.sync_sigmasz_contacts_from_l2_full();

  RETURN QUERY
  SELECT 'sigmasz_contacts'::TEXT,
         v_contacts.inserted_count,
         v_contacts.updated_count,
         v_contacts.deleted_count;

  -- 3. Contact phones
  SELECT * INTO v_phones
  FROM amo_support_schema.sync_sigmasz_contact_phones_from_l2_full();

  RETURN QUERY
  SELECT 'sigmasz_contact_phones'::TEXT,
         v_phones.inserted_count,
         0::BIGINT,
         0::BIGINT;

  -- 4. Contact emails
  SELECT * INTO v_emails
  FROM amo_support_schema.sync_sigmasz_contact_emails_from_l2_full();

  RETURN QUERY
  SELECT 'sigmasz_contact_emails'::TEXT,
         v_emails.inserted_count,
         0::BIGINT,
         0::BIGINT;

  -- 5. Lead -> Contacts (связи)
  SELECT * INTO v_links
  FROM amo_support_schema.sync_sigmasz_lead_contacts_from_l2_full();

  RETURN QUERY
  SELECT 'sigmasz_lead_contacts'::TEXT,
         v_links.inserted_count,
         0::BIGINT,
         0::BIGINT;

END;
$$ LANGUAGE plpgsql;

