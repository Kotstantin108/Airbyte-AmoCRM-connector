-- =============================================================================
-- НОРМАЛИЗАЦИЯ ТЕЛЕФОНА (Оставляем только цифры, 8 -> 7 для РФ/СНГ)
-- =============================================================================
DROP FUNCTION IF EXISTS public.normalize_phone(text) CASCADE;

CREATE FUNCTION public.normalize_phone(p_raw text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_clean text;
BEGIN
    IF p_raw IS NULL THEN 
        RETURN NULL; 
    END IF;
    
    -- Оставляем ТОЛЬКО цифры
    v_clean := regexp_replace(p_raw, '\D', '', 'g');
    
    IF v_clean = '' THEN 
        RETURN NULL; 
    END IF;

    -- Приводим номера вида 8999... к стандарту 7999...
    IF length(v_clean) = 11 AND left(v_clean, 1) = '8' THEN
        v_clean := '7' || substr(v_clean, 2);
    END IF;
    
    RETURN v_clean;
END;
$$;
-- =============================================================================
-- DWH FULL REBUILD: Hardened Event-Driven Architecture (FINAL MERGED VERSION)
-- Layers: L1 (airbyte_raw) → L2 (prod_sync)
-- Pattern: Tombstone Shield + Dead Letter Queue
-- =============================================================================
-- EXECUTION ORDER:
--   1. BLOCK 0: Infrastructure (tables, DLQ)
--   2. BLOCK 1: Shield helper-functions
--   3. BLOCK 2: Utility functions (safe_cf_to_timestamp, get_best_contact)
--   4. BLOCK 3: L1→L2 trigger functions (events, leads, contacts)
--   5. BLOCK 4: L2 utility functions (process_embedded_contacts)
--   6. BLOCK 5: Trigger DDL (привязка функций к таблицам)
--   7. BLOCK 6: Индексы для производительности
--   8. BLOCK 7: Вспомогательные view для мониторинга
-- =============================================================================

-- =============================================================================
-- BLOCK 0: INFRASTRUCTURE
-- =============================================================================

-- Tombstone Shield: единственный источник истины об удалённых сущностях
CREATE TABLE IF NOT EXISTS prod_sync.deleted_entities_log (
    entity_type  VARCHAR(50)  NOT NULL,
    entity_id    BIGINT       NOT NULL,
    deleted_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    _synced_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_type, entity_id)
);
COMMENT ON TABLE prod_sync.deleted_entities_log IS
    'Tombstone Shield. Единственный источник истины об удалённых сущностях. '
    'Проверяется перед любым UPSERT в L2. Никогда не очищается автоматически.';

-- Dead Letter Queue: карантин для битых строк из L1
CREATE TABLE IF NOT EXISTS airbyte_raw.l2_dead_letter_queue (
    id            BIGSERIAL    PRIMARY KEY,
    stream_name   TEXT         NOT NULL,
    entity_id     BIGINT,
    raw_record    JSONB,
    error_message TEXT         NOT NULL,
    sqlstate      TEXT,
    failed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    retry_count   INT          NOT NULL DEFAULT 0,
    resolved      BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dlq_unresolved
    ON airbyte_raw.l2_dead_letter_queue (stream_name, failed_at)
    WHERE resolved = FALSE;
COMMENT ON TABLE airbyte_raw.l2_dead_letter_queue IS
    'Dead Letter Queue для строк L1/L2, которые вызвали ошибку. '
    'resolved=FALSE — требует ручного разбора.';


-- =============================================================================
-- BLOCK 1: TOMBSTONE SHIELD — HELPER FUNCTIONS
-- =============================================================================

-- NOTE: register_tombstone signature with domain parameter is defined in dwh_multidomain_core_part1.sql
-- This file should be executed AFTER dwh_multidomain_core_part1.sql
-- Keeping only the documentation here to avoid redefining the function
-- 
-- CREATE OR REPLACE FUNCTION prod_sync.register_tombstone(
--     p_domain TEXT,
--     p_entity_type TEXT,
--     p_entity_id BIGINT
-- )
-- See dwh_multidomain_core_part1.sql for implementation

-- NOTE: is_tombstoned signature with domain parameter is defined in dwh_multidomain_core_part1.sql
-- This file should be executed AFTER dwh_multidomain_core_part1.sql
-- Keeping only the documentation here to avoid redefining the function
--
-- CREATE OR REPLACE FUNCTION prod_sync.is_tombstoned(
--     p_domain TEXT,
--     p_entity_type TEXT,
--     p_entity_id BIGINT
-- )
-- See dwh_multidomain_core_part1.sql for implementation

-- =============================================================================
-- BLOCK 2: UTILITY FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION prod_sync.safe_cf_to_timestamp(val TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_big BIGINT;
    v_sec BIGINT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    val := BTRIM(val);
    IF val = '' OR val !~ '^\d+$' THEN RETURN NULL; END IF;
    IF length(val) > 14 THEN RETURN NULL; END IF;
    BEGIN
        v_big := val::BIGINT;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
    IF length(val) = 13 THEN
        v_sec := v_big / 1000;
    ELSIF length(val) BETWEEN 1 AND 12 THEN
        v_sec := v_big;
    ELSE
        RETURN NULL;
    END IF;
    RETURN to_timestamp(v_sec::DOUBLE PRECISION);
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION prod_sync.get_best_contact_for_lead(p_lead_id BIGINT)
RETURNS TABLE (
    c_id    BIGINT,
    c_name  TEXT,
    c_phone TEXT,
    c_email TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.contact_id,
        c.name::TEXT,
        (SELECT cp.phone FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id ORDER BY cp.phone LIMIT 1),
        (SELECT ce.email FROM prod_sync.sigmasz_contact_emails ce WHERE ce.contact_id = c.contact_id ORDER BY ce.email LIMIT 1)
    FROM       prod_sync.sigmasz_lead_contacts lc
    INNER JOIN prod_sync.sigmasz_contacts c ON c.contact_id = lc.contact_id
    WHERE  lc.lead_id = p_lead_id
      AND  COALESCE(c.is_deleted, FALSE) IS FALSE
      AND  NOT prod_sync.is_tombstoned('sigmasz', 'contact', c.contact_id)
    ORDER BY COALESCE(lc.is_main, FALSE) DESC, c.contact_id ASC
    LIMIT 1;
END;
$$;

-- =============================================================================
-- BLOCK 3: L1 → L2 TRIGGER FUNCTIONS
-- =============================================================================

-- 3.1. Tombstone Writer
CREATE OR REPLACE FUNCTION airbyte_raw.propagate_deleted_to_l2()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_sigmasz'));
    IF NEW.type = 'lead_deleted' AND NEW.entity_type = 'lead' THEN
        PERFORM prod_sync.register_tombstone('lead', NEW.entity_id);
        UPDATE prod_sync.sigmasz_leads SET is_deleted = TRUE, _synced_at = NOW() WHERE lead_id = NEW.entity_id;
        UPDATE airbyte_raw.sigmasz_leads SET is_deleted = TRUE WHERE id = NEW.entity_id AND (is_deleted IS NULL OR is_deleted = FALSE);
    END IF;
    IF NEW.type = 'contact_deleted' AND NEW.entity_type = 'contact' THEN
        PERFORM prod_sync.register_tombstone('contact', NEW.entity_id);
        UPDATE prod_sync.sigmasz_contacts SET is_deleted = TRUE, _synced_at = NOW() WHERE contact_id = NEW.entity_id;
        UPDATE airbyte_raw.sigmasz_contacts SET is_deleted = TRUE WHERE id = NEW.entity_id AND (is_deleted IS NULL OR is_deleted = FALSE);
    END IF;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '[propagate_deleted_to_l2] entity=% id=% error=% state=%', NEW.entity_type, NEW.entity_id, SQLERRM, SQLSTATE;
    RETURN NEW;
END;
$$;

-- 3.2. РАСПАКОВЩИК ЛИДОВ (с обработкой явного обнуления)
CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_lead_id            BIGINT;
    v_created_ts         TIMESTAMPTZ;
    v_updated_ts         TIMESTAMPTZ;
    v_embedded_safe      JSONB := '{}'::JSONB;
    v_custom_fields_safe JSONB := '[]'::JSONB;
    v_raw_json           JSONB;
    v_embedded_contacts  JSONB;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_sigmasz'));
    v_lead_id := NEW.id;
    IF v_lead_id IS NULL THEN RETURN NEW; END IF;

    IF prod_sync.is_tombstoned('sigmasz', 'lead', v_lead_id) THEN RETURN NEW; END IF;

    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN
        PERFORM prod_sync.register_tombstone('sigmasz', 'lead', v_lead_id);
        DELETE FROM prod_sync.sigmasz_lead_contacts WHERE lead_id = v_lead_id;
        DELETE FROM prod_sync.sigmasz_leads WHERE lead_id = v_lead_id;
        RETURN NEW;
    END IF;

    -- Check again before INSERT/UPDATE to prevent ghost entities from race condition
    IF prod_sync.is_tombstoned('sigmasz', 'lead', v_lead_id) THEN RETURN NEW; END IF;

    BEGIN
        IF pg_typeof(NEW.created_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_created_ts := prod_sync.safe_cf_to_timestamp(NEW.created_at::TEXT);
        ELSE
            v_created_ts := NEW.created_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_created_ts := NULL; END;

    IF v_created_ts IS NULL THEN
        BEGIN
            SELECT prod_sync.safe_cf_to_timestamp(l.created_at::TEXT) INTO v_created_ts
            FROM airbyte_raw.sigmasz_leads l
            WHERE l.id = v_lead_id AND l.created_at IS NOT NULL
            ORDER BY l.created_at ASC LIMIT 1;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END IF;

    BEGIN
        IF pg_typeof(NEW.updated_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_updated_ts := prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT);
        ELSE
            v_updated_ts := NEW.updated_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_updated_ts := NULL; END;

    BEGIN
        IF NEW._embedded IS NOT NULL AND BTRIM(NEW._embedded::TEXT) NOT IN ('', 'null') THEN
            v_embedded_safe := NEW._embedded::JSONB;
            IF jsonb_typeof(v_embedded_safe) <> 'object' THEN v_embedded_safe := '{}'::JSONB; END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_embedded_safe := '{}'::JSONB; END;

    BEGIN
        IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN ('', 'null') THEN
            v_custom_fields_safe := NEW.custom_fields_values::JSONB;
            IF jsonb_typeof(v_custom_fields_safe) <> 'array' THEN v_custom_fields_safe := '[]'::JSONB; END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_custom_fields_safe := '[]'::JSONB; END;

    v_raw_json := jsonb_build_object(
        'id', NEW.id, 'name', NEW.name, 'status_id', NEW.status_id, 'pipeline_id', NEW.pipeline_id, 'price', NEW.price,
        'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted,
        '_embedded', v_embedded_safe, 'custom_fields_values', v_custom_fields_safe
    );

    INSERT INTO prod_sync.sigmasz_leads
        (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json, is_deleted, _synced_at)
    VALUES
        (v_lead_id, NEW.name, NEW.status_id, NEW.pipeline_id, NEW.price, v_created_ts, v_updated_ts, v_raw_json, FALSE, NOW())
    ON CONFLICT (lead_id) DO UPDATE SET
        name        = EXCLUDED.name,
        status_id   = EXCLUDED.status_id,
        pipeline_id = EXCLUDED.pipeline_id,
        price       = EXCLUDED.price,
        created_at  = COALESCE(GREATEST(EXCLUDED.created_at, sigmasz_leads.created_at), EXCLUDED.created_at, sigmasz_leads.created_at),
        updated_at  = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_leads.updated_at), EXCLUDED.updated_at, sigmasz_leads.updated_at),
        raw_json    = EXCLUDED.raw_json,
        -- Don't update is_deleted if record is tombstoned - preserve the deletion mark
        is_deleted  = CASE WHEN prod_sync.is_tombstoned('sigmasz', 'lead', sigmasz_leads.lead_id) 
                            THEN sigmasz_leads.is_deleted 
                            ELSE FALSE 
                      END,
        _synced_at  = NOW();

    v_embedded_contacts := v_embedded_safe -> 'contacts';
    IF v_embedded_contacts IS NOT NULL AND jsonb_typeof(v_embedded_contacts) = 'array' THEN
        PERFORM prod_sync.process_embedded_contacts(
            v_embedded_contacts,
            v_lead_id,
            jsonb_array_length(v_embedded_contacts) = 0
        );
    END IF;

    RETURN NEW;
EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN
        BEGIN
            INSERT INTO airbyte_raw.l2_dead_letter_queue (stream_name, entity_id, raw_record, error_message, sqlstate)
            VALUES ('sigmasz_leads', v_lead_id, to_jsonb(NEW), SQLERRM, SQLSTATE);
        EXCEPTION WHEN OTHERS THEN NULL; END;
        RAISE WARNING '[unpack_sigmasz_leads_l2] BAD DATA lead_id=% → DLQ. error=%', v_lead_id, SQLERRM;
        RETURN NEW;
    WHEN OTHERS THEN
        RAISE EXCEPTION '[unpack_sigmasz_leads_l2] TRANSIENT ERROR lead_id=%: % (STATE: %)', v_lead_id, SQLERRM, SQLSTATE;
END;
$$;

-- 3.3. РАСПАКОВЩИК КОНТАКТОВ
CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_contact_id         BIGINT;
    v_updated_ts         TIMESTAMPTZ;
    v_custom_fields_safe JSONB := '[]'::JSONB;
    v_raw_json           JSONB;
    v_phone              TEXT;
    v_email              TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_sigmasz'));
    v_contact_id := NEW.id;
    IF v_contact_id IS NULL THEN RETURN NEW; END IF;

    IF prod_sync.is_tombstoned('sigmasz', 'contact', v_contact_id) THEN RETURN NEW; END IF;

    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN
        PERFORM prod_sync.register_tombstone('sigmasz', 'contact', v_contact_id);
        DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_contact_id;
        DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_contact_id;
        DELETE FROM prod_sync.sigmasz_lead_contacts WHERE contact_id = v_contact_id;
        DELETE FROM prod_sync.sigmasz_contacts WHERE contact_id = v_contact_id;
        RETURN NEW;
    END IF;

    -- Check again before INSERT/UPDATE to prevent ghost entities from race condition
    IF prod_sync.is_tombstoned('sigmasz', 'contact', v_contact_id) THEN RETURN NEW; END IF;

    BEGIN
        IF pg_typeof(NEW.updated_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_updated_ts := prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT);
        ELSE
            v_updated_ts := NEW.updated_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_updated_ts := NULL; END;

    BEGIN
        IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN ('', 'null') THEN
            v_custom_fields_safe := NEW.custom_fields_values::JSONB;
            IF jsonb_typeof(v_custom_fields_safe) <> 'array' THEN v_custom_fields_safe := '[]'::JSONB; END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_custom_fields_safe := '[]'::JSONB; END;

    v_raw_json := jsonb_build_object(
        'id', NEW.id, 'name', NEW.name, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, 'custom_fields_values', v_custom_fields_safe
    );

    INSERT INTO prod_sync.sigmasz_contacts
        (contact_id, name, updated_at, raw_json, is_deleted, _synced_at)
    VALUES
        (v_contact_id, NEW.name, v_updated_ts, v_raw_json, FALSE, NOW())
    ON CONFLICT (contact_id) DO UPDATE SET
        name       = EXCLUDED.name,
        updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_contacts.updated_at), EXCLUDED.updated_at, sigmasz_contacts.updated_at),
        raw_json   = EXCLUDED.raw_json,
        -- Don't update is_deleted if record is tombstoned - preserve the deletion mark
        is_deleted = CASE WHEN prod_sync.is_tombstoned('sigmasz', 'contact', sigmasz_contacts.contact_id)
                            THEN sigmasz_contacts.is_deleted
                            ELSE FALSE
                      END,
        _synced_at = NOW();

    -- FIX: Не стираем телефоны/email, если custom_fields_values не пришли (partial update)
    IF v_custom_fields_safe != '[]'::JSONB THEN
        DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_contact_id;
        FOR v_phone IN
            SELECT DISTINCT public.normalize_phone(v.value->>'value')
            FROM   jsonb_array_elements(v_custom_fields_safe) AS cf
            CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]'::JSONB END) AS v
            WHERE  cf->>'field_code' = 'PHONE' AND public.normalize_phone(v.value->>'value') IS NOT NULL
        LOOP
            INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone) VALUES (v_contact_id, v_phone) ON CONFLICT DO NOTHING;
        END LOOP;

        DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_contact_id;
        FOR v_email IN
            SELECT DISTINCT lower(BTRIM(v.value->>'value'))
            FROM   jsonb_array_elements(v_custom_fields_safe) AS cf
            CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]'::JSONB END) AS v
            WHERE  cf->>'field_code' = 'EMAIL' AND NULLIF(BTRIM(v.value->>'value'), '') IS NOT NULL
        LOOP
            INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email) VALUES (v_contact_id, v_email) ON CONFLICT DO NOTHING;
        END LOOP;
    END IF;

    RETURN NEW;
EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN
        BEGIN
            INSERT INTO airbyte_raw.l2_dead_letter_queue (stream_name, entity_id, raw_record, error_message, sqlstate)
            VALUES ('sigmasz_contacts', v_contact_id, to_jsonb(NEW), SQLERRM, SQLSTATE);
        EXCEPTION WHEN OTHERS THEN NULL; END;
        RAISE WARNING '[unpack_sigmasz_contacts_l2] BAD DATA contact_id=% → DLQ. error=% state=%', v_contact_id, SQLERRM, SQLSTATE;
        RETURN NEW;
    WHEN OTHERS THEN
        RAISE EXCEPTION '[unpack_sigmasz_contacts_l2] TRANSIENT ERROR contact_id=%: % (STATE: %)', v_contact_id, SQLERRM, SQLSTATE;
END;
$$;

-- =============================================================================
-- BLOCK 4: L2 UTILITY FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION prod_sync.process_embedded_contacts(
    p_contacts_json  JSONB,
    p_lead_id        BIGINT,
    p_explicit_empty BOOLEAN DEFAULT FALSE
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_contact     JSONB;
    v_contact_id  BIGINT;
    v_updated_ts  TIMESTAMPTZ;
    v_is_main     BOOLEAN;
    v_current_ids BIGINT[] := ARRAY[]::BIGINT[];
BEGIN
    IF p_explicit_empty THEN
        DELETE FROM prod_sync.sigmasz_lead_contacts WHERE lead_id = p_lead_id;
        RETURN;
    END IF;

    FOR v_contact IN SELECT value FROM jsonb_array_elements(p_contacts_json)
    LOOP
        IF jsonb_typeof(v_contact) <> 'object' THEN CONTINUE; END IF;
        BEGIN
            v_contact_id := NULLIF(BTRIM(COALESCE(v_contact ->> 'id', '')), '')::BIGINT;
        EXCEPTION WHEN OTHERS THEN CONTINUE; END;
        IF v_contact_id IS NULL THEN CONTINUE; END IF;
        IF prod_sync.is_tombstoned('sigmasz', 'contact', v_contact_id) THEN CONTINUE; END IF;

        v_updated_ts := prod_sync.safe_cf_to_timestamp(NULLIF(BTRIM(COALESCE(v_contact ->> 'updated_at', '')), ''));
        v_is_main := COALESCE((v_contact ->> 'is_main')::BOOLEAN, FALSE);

        INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at)
        VALUES (v_contact_id, v_contact ->> 'name', v_updated_ts, v_contact, FALSE, NOW())
        ON CONFLICT (contact_id) DO UPDATE SET
            name       = EXCLUDED.name,
            updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_contacts.updated_at), EXCLUDED.updated_at, sigmasz_contacts.updated_at),
            raw_json   = sigmasz_contacts.raw_json || EXCLUDED.raw_json,
            _synced_at = NOW();

        INSERT INTO prod_sync.sigmasz_lead_contacts (lead_id, contact_id, is_main)
        VALUES (p_lead_id, v_contact_id, v_is_main)
        ON CONFLICT (lead_id, contact_id) DO UPDATE SET is_main = EXCLUDED.is_main;

        v_current_ids := array_append(v_current_ids, v_contact_id);
    END LOOP;

    IF array_length(v_current_ids, 1) > 0 THEN
        DELETE FROM prod_sync.sigmasz_lead_contacts
        WHERE  lead_id    = p_lead_id
          AND  contact_id <> ALL(v_current_ids);
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '[process_embedded_contacts] lead_id=% contact_id=% error=% state=%', p_lead_id, v_contact_id, SQLERRM, SQLSTATE;
END;
$$;


-- =============================================================================
-- BLOCK 5: TRIGGER DDL
-- =============================================================================

DROP TRIGGER IF EXISTS trg_unpack_sigmasz_leads_l2 ON airbyte_raw.sigmasz_leads;
CREATE TRIGGER trg_unpack_sigmasz_leads_l2
    AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_leads
    FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2();

DROP TRIGGER IF EXISTS trg_unpack_sigmasz_contacts_l2 ON airbyte_raw.sigmasz_contacts;
CREATE TRIGGER trg_unpack_sigmasz_contacts_l2
    AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_contacts
    FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2();

DROP TRIGGER IF EXISTS trg_propagate_deleted_to_l2 ON airbyte_raw.sigmasz_events;
CREATE TRIGGER trg_propagate_deleted_to_l2
    AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_events
    FOR EACH ROW EXECUTE FUNCTION airbyte_raw.propagate_deleted_to_l2();



-- =============================================================================
-- BLOCK 6: ИНДЕКСЫ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ
-- =============================================================================

-- Индексы для composite watermark
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_composite_wm ON prod_sync.sigmasz_leads (_synced_at ASC, lead_id ASC);
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_composite_wm ON prod_sync.sigmasz_contacts (_synced_at ASC, contact_id ASC);

-- FIX-1: Уникальный частичный индекс для работы ON CONFLICT DO NOTHING в DLQ
CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_active_entity
    ON airbyte_raw.l2_dead_letter_queue (stream_name, entity_id)
    WHERE resolved = FALSE;

CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_is_deleted ON prod_sync.sigmasz_leads (is_deleted) WHERE is_deleted = TRUE;
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_is_deleted ON prod_sync.sigmasz_contacts (is_deleted) WHERE is_deleted = TRUE;
CREATE INDEX IF NOT EXISTS idx_sigmasz_lead_contacts_lead_id ON prod_sync.sigmasz_lead_contacts (lead_id);
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at_unresolved ON airbyte_raw.l2_dead_letter_queue (failed_at DESC) WHERE resolved = FALSE;

