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
-- Layers: L1 (airbyte_raw) → L2 (prod_sync) → L3 (analytics)
-- Pattern: Tombstone Shield + Dead Letter Queue + Batch-only L3
-- =============================================================================
-- EXECUTION ORDER:
--   1. BLOCK 0: Infrastructure (tables, DLQ)
--   2. BLOCK 1: Shield helper-functions
--   3. BLOCK 2: Utility functions (safe_cf_to_timestamp, get_best_contact)
--   4. BLOCK 3: L1→L2 trigger functions (events, leads, contacts)
--   5. BLOCK 4: L2 utility functions (process_embedded_contacts)
--   6. BLOCK 5: L3 batch functions (propagate_one_lead)
--   7. BLOCK 6: Batch entry-points (run_l3_batch_leads)
--   8. BLOCK 7: Trigger DDL (привязка функций к таблицам)
--   9. BLOCK 8: Индексы для производительности
--  10. BLOCK 9: Вспомогательные view для мониторинга
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

-- Таблица водяных меток для L3-батчей (с поддержкой composite watermark)
CREATE TABLE IF NOT EXISTS prod_sync.l3_batch_watermarks (
    stream_name    TEXT         PRIMARY KEY,
    last_synced_at TIMESTAMPTZ  NOT NULL DEFAULT '1970-01-01'::TIMESTAMPTZ,
    last_entity_id BIGINT       NOT NULL DEFAULT 0,
    last_run_at    TIMESTAMPTZ,
    rows_processed INT          NOT NULL DEFAULT 0
);

-- FIX-2: Миграция колонки для существующих инсталляций DWH
ALTER TABLE prod_sync.l3_batch_watermarks
    ADD COLUMN IF NOT EXISTS last_entity_id BIGINT NOT NULL DEFAULT 0;

INSERT INTO prod_sync.l3_batch_watermarks (stream_name)
VALUES ('sigmasz_leads')
ON CONFLICT DO NOTHING;
COMMENT ON TABLE prod_sync.l3_batch_watermarks IS
    'Водяные метки для инкрементальных L3-батчей. '
    'Используют composite watermark (_synced_at, entity_id) для stable pagination.';

-- =============================================================================
-- BLOCK 1: TOMBSTONE SHIELD — HELPER FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION prod_sync.register_tombstone(
    p_entity_type TEXT,
    p_entity_id   BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO prod_sync.deleted_entities_log
        (entity_type, entity_id, deleted_at, _synced_at)
    VALUES
        (p_entity_type, p_entity_id, NOW(), NOW())
    ON CONFLICT (entity_type, entity_id) DO UPDATE
        SET deleted_at = NOW(),
            _synced_at = NOW();
END;
$$;

CREATE OR REPLACE FUNCTION prod_sync.is_tombstoned(
    p_entity_type TEXT,
    p_entity_id   BIGINT
)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM   prod_sync.deleted_entities_log
        WHERE  entity_type = p_entity_type
          AND  entity_id   = p_entity_id
    );
$$;

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
    ELSIF length(val) BETWEEN 10 AND 12 THEN
        v_sec := v_big;
        IF length(val) = 10 AND v_big < 1000000000 THEN RETURN NULL; END IF;
    ELSIF length(val) BETWEEN 8 AND 9 THEN
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
      AND  NOT prod_sync.is_tombstoned('contact', c.contact_id)
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
    IF NEW.type = 'lead_deleted' AND NEW.entity_type = 'lead' THEN
        PERFORM prod_sync.register_tombstone('lead', NEW.entity_id);
        UPDATE prod_sync.sigmasz_leads SET is_deleted = TRUE, _synced_at = NOW() WHERE lead_id = NEW.entity_id;
    END IF;
    IF NEW.type = 'contact_deleted' AND NEW.entity_type = 'contact' THEN
        PERFORM prod_sync.register_tombstone('contact', NEW.entity_id);
        UPDATE prod_sync.sigmasz_contacts SET is_deleted = TRUE, _synced_at = NOW() WHERE contact_id = NEW.entity_id;
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
    v_lead_id := NEW.id;
    IF v_lead_id IS NULL THEN RETURN NEW; END IF;

    IF prod_sync.is_tombstoned('lead', v_lead_id) THEN RETURN NEW; END IF;

    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN
        PERFORM prod_sync.register_tombstone('lead', v_lead_id);
        UPDATE prod_sync.sigmasz_leads SET is_deleted = TRUE, _synced_at = NOW() WHERE lead_id = v_lead_id;
        RETURN NEW;
    END IF;

    BEGIN
        IF pg_typeof(NEW.created_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_created_ts := prod_sync.safe_cf_to_timestamp(NEW.created_at::TEXT);
        ELSE
            v_created_ts := NEW.created_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_created_ts := NULL; END;

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
        is_deleted  = FALSE,
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
    v_contact_id := NEW.id;
    IF v_contact_id IS NULL THEN RETURN NEW; END IF;

    IF prod_sync.is_tombstoned('contact', v_contact_id) THEN RETURN NEW; END IF;

    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN
        PERFORM prod_sync.register_tombstone('contact', v_contact_id);
        UPDATE prod_sync.sigmasz_contacts SET is_deleted = TRUE, _synced_at = NOW() WHERE contact_id = v_contact_id;
        RETURN NEW;
    END IF;

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
        is_deleted = FALSE,
        _synced_at = NOW();

    DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_contact_id;
    FOR v_phone IN
        SELECT DISTINCT public.normalize_phone(v.value->>'value')
        FROM   jsonb_array_elements(v_custom_fields_safe) AS cf
        CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]'::JSONB END) AS v
        WHERE  cf->>'field_code' = 'PHONE' AND public.normalize_phone(v.value->>'value') IS NOT NULL AND length(public.normalize_phone(v.value->>'value')) >= 10
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
        IF prod_sync.is_tombstoned('contact', v_contact_id) THEN CONTINUE; END IF;

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
-- BLOCK 5: L3 BATCH FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION prod_sync.propagate_one_lead_to_l3(
    p_lead_id     BIGINT,
    p_cols        TEXT[]  DEFAULT NULL,
    p_insert_cols TEXT    DEFAULT NULL,
    p_set_clause  TEXT    DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    r               prod_sync.sigmasz_leads%ROWTYPE;
    v_flat_json     JSONB;
    v_filtered_json JSONB;
    v_sql           TEXT;
    v_cols          TEXT[];
    v_insert_cols   TEXT;
    v_set_clause    TEXT;
    v_c_id          BIGINT;
    v_c_name        TEXT;
    v_c_phone       TEXT;
    v_c_email       TEXT;
BEGIN
    SELECT * INTO r FROM prod_sync.sigmasz_leads WHERE lead_id = p_lead_id;
    IF NOT FOUND THEN RETURN; END IF;

    IF COALESCE(r.is_deleted, FALSE) IS TRUE THEN
        DELETE FROM analytics.sigmasz_leads WHERE lead_id = p_lead_id;
        RETURN;
    END IF;

    SELECT c_id, c_name, c_phone, c_email INTO v_c_id, v_c_name, v_c_phone, v_c_email
    FROM prod_sync.get_best_contact_for_lead(p_lead_id);

    SELECT jsonb_object_agg(
               'f_' || COALESCE(elem.value ->> 'field_id', elem.value ->> 'id'),
               CASE WHEN cf.type IN ('date', 'date_time', 'birthday') THEN
                       CASE WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d+$'
                               THEN to_jsonb(prod_sync.safe_cf_to_timestamp(elem.value -> 'values' -> 0 ->> 'value'))
                            WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d{2}\.\d{2}\.\d{4}$'
                               THEN to_jsonb(to_timestamp(elem.value -> 'values' -> 0 ->> 'value', 'DD.MM.YYYY')::TIMESTAMPTZ)
                            ELSE to_jsonb(NULL::TIMESTAMPTZ) END
                    ELSE
                       CASE WHEN jsonb_typeof(elem.value -> 'values') = 'array' THEN (elem.value -> 'values' -> 0 -> 'value') ELSE NULL END
               END
           ) INTO v_flat_json
    FROM jsonb_array_elements(COALESCE(r.raw_json -> 'custom_fields_values', '[]'::JSONB)) elem(value)
    LEFT JOIN airbyte_raw.sigmasz_custom_fields_leads cf ON cf.id::TEXT = COALESCE(elem.value ->> 'field_id', elem.value ->> 'id')
    WHERE COALESCE(elem.value ->> 'field_id', elem.value ->> 'id') IS NOT NULL;

    IF v_flat_json IS NULL THEN v_flat_json := '{}'::JSONB; END IF;

    v_flat_json := v_flat_json || jsonb_build_object(
        'lead_id', r.lead_id, 'name', r.name, 'status_id', r.status_id, 'pipeline_id', r.pipeline_id, 'price', r.price,
        'created_at', r.created_at, 'updated_at', r.updated_at, 'is_deleted', r.is_deleted, '_synced_at', NOW(),
        'contact_id', v_c_id, 'contact_name', v_c_name, 'contact_phone', v_c_phone, 'contact_email', v_c_email
    );

    IF p_cols IS NOT NULL THEN
        v_cols := p_cols; v_insert_cols := p_insert_cols; v_set_clause := p_set_clause;
    ELSE
        SELECT array_agg(a.attname::TEXT ORDER BY a.attnum) INTO v_cols
        FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'analytics' AND c.relname = 'sigmasz_leads' AND a.attnum > 0 AND NOT a.attisdropped;

        SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
        SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col <> 'lead_id';
    END IF;

    IF v_set_clause IS NULL OR v_set_clause NOT LIKE '%_synced_at%' THEN
        v_set_clause := COALESCE(v_set_clause || ', ', '') || '_synced_at = NOW()';
    END IF;

    SELECT jsonb_object_agg(key, value) INTO v_filtered_json FROM jsonb_each(v_flat_json) WHERE key = ANY(v_cols);

    v_sql := format(
        'INSERT INTO analytics.%I (%s) SELECT * FROM jsonb_populate_record(NULL::analytics.%I, $1) ON CONFLICT (lead_id) DO UPDATE SET %s',
        'sigmasz_leads', v_insert_cols, 'sigmasz_leads', v_set_clause
    );
    EXECUTE v_sql USING COALESCE(v_filtered_json, '{}'::JSONB);
END;
$$;

-- =============================================================================
-- BLOCK 6: L3 BATCH ENTRY POINTS (Composite Watermark, Retry Quarantine, timeout)
-- =============================================================================

CREATE OR REPLACE FUNCTION prod_sync.run_l3_batch_leads(
    p_batch_size  INT     DEFAULT 1000,
    p_max_retries INT     DEFAULT 0
)
RETURNS TABLE (
    processed_count  INT,
    failed_count     INT,
    skipped_count    INT,
    last_watermark   TIMESTAMPTZ,
    has_more         BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts          TIMESTAMPTZ;
    v_from_id          BIGINT;
    v_to_ts            TIMESTAMPTZ;
    v_lead_id          BIGINT;
    v_lead_synced_at   TIMESTAMPTZ;
    v_max_ts           TIMESTAMPTZ;
    v_max_id           BIGINT;
    v_count            INT := 0;
    v_fail_count       INT := 0;
    v_skip_count       INT := 0;
    v_existing_retries INT;
    v_cols             TEXT[];
    v_insert_cols      TEXT;
    v_set_clause       TEXT;
BEGIN
    SET LOCAL lock_timeout = '5s';
    SELECT wm.last_synced_at, wm.last_entity_id INTO v_from_ts, v_from_id
    FROM prod_sync.l3_batch_watermarks wm WHERE wm.stream_name = 'sigmasz_leads' FOR UPDATE;

    v_to_ts  := NOW() - INTERVAL '5 seconds';
    v_max_ts := v_from_ts;
    v_max_id := v_from_id;

    SELECT array_agg(a.attname::TEXT ORDER BY a.attnum) INTO v_cols
    FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'analytics' AND c.relname = 'sigmasz_leads' AND a.attnum > 0 AND NOT a.attisdropped;

    SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
    SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col <> 'lead_id';
    IF v_set_clause IS NULL OR v_set_clause NOT LIKE '%_synced_at%' THEN
        v_set_clause := COALESCE(v_set_clause || ', ', '') || '_synced_at = NOW()';
    END IF;

    FOR v_lead_id, v_lead_synced_at IN
        SELECT lead_id, _synced_at FROM prod_sync.sigmasz_leads
        WHERE  (_synced_at, lead_id) > (v_from_ts, v_from_id) AND _synced_at < v_to_ts
        ORDER  BY _synced_at ASC, lead_id ASC
        LIMIT  p_batch_size
    LOOP
        BEGIN
            PERFORM prod_sync.propagate_one_lead_to_l3(v_lead_id, v_cols, v_insert_cols, v_set_clause);
            v_max_ts := v_lead_synced_at;
            v_max_id := v_lead_id;
            v_count  := v_count + 1;
        EXCEPTION WHEN OTHERS THEN
            v_fail_count := v_fail_count + 1;
            
            -- Используем DO NOTHING опираясь на частичный индекс idx_dlq_active_entity
            INSERT INTO airbyte_raw.l2_dead_letter_queue (stream_name, entity_id, error_message, sqlstate, retry_count)
            VALUES ('sigmasz_leads_l3', v_lead_id, SQLERRM, SQLSTATE, 1) 
            ON CONFLICT DO NOTHING;
            
            UPDATE airbyte_raw.l2_dead_letter_queue
            SET retry_count = retry_count + 1, error_message = SQLERRM, failed_at = NOW()
            WHERE stream_name = 'sigmasz_leads_l3' AND entity_id = v_lead_id AND resolved = FALSE;
            
            SELECT retry_count INTO v_existing_retries
            FROM airbyte_raw.l2_dead_letter_queue
            WHERE stream_name = 'sigmasz_leads_l3' AND entity_id = v_lead_id AND resolved = FALSE;
            
            RAISE WARNING '[run_l3_batch_leads] lead_id=% attempt=% error=%', v_lead_id, v_existing_retries, SQLERRM;
            IF p_max_retries > 0 AND v_existing_retries >= p_max_retries THEN
                v_max_ts := v_lead_synced_at; v_max_id := v_lead_id; v_skip_count := v_skip_count + 1;
                RAISE WARNING '[run_l3_batch_leads] lead_id=% QUARANTINED after % retries', v_lead_id, v_existing_retries;
            ELSE
                EXIT;
            END IF;
        END;
    END LOOP;

    IF v_max_ts > v_from_ts OR (v_max_ts = v_from_ts AND v_max_id > v_from_id) THEN
        UPDATE prod_sync.l3_batch_watermarks
        SET last_synced_at = v_max_ts, last_entity_id = v_max_id, last_run_at = NOW(), rows_processed = rows_processed + v_count
        WHERE stream_name = 'sigmasz_leads';
    ELSE
        UPDATE prod_sync.l3_batch_watermarks SET last_run_at = NOW() WHERE stream_name = 'sigmasz_leads';
    END IF;

    RETURN QUERY SELECT v_count, v_fail_count, v_skip_count, v_max_ts, (v_count = p_batch_size AND v_fail_count = 0);
END;
$$;
COMMENT ON FUNCTION prod_sync.run_l3_batch_leads IS
    'Инкрементальный L3-батч для лидов. '
    'Backlog-цикл (пока has_more) работает только при p_max_retries > 0 (quarantine mode). '
    'При p_max_retries=0 (fail-stop) has_more=FALSE при первой ошибке — это штатно и останавливает обработку.';

-- =============================================================================
-- BLOCK 7: TRIGGER DDL
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

CREATE OR REPLACE FUNCTION prod_sync.trg_update_lead_on_contact_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE prod_sync.sigmasz_leads SET _synced_at = NOW() WHERE lead_id = NEW.lead_id;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_update_lead_on_contact_link_change ON prod_sync.sigmasz_lead_contacts;
CREATE TRIGGER trg_update_lead_on_contact_link_change
    AFTER INSERT OR UPDATE ON prod_sync.sigmasz_lead_contacts
    FOR EACH ROW EXECUTE FUNCTION prod_sync.trg_update_lead_on_contact_change();

-- =============================================================================
-- BLOCK 8: ИНДЕКСЫ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ
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

-- =============================================================================
-- BLOCK 9: ВСПОМОГАТЕЛЬНЫЕ VIEW ДЛЯ МОНИТОРИНГА
-- =============================================================================

CREATE OR REPLACE VIEW airbyte_raw.v_dlq_summary AS
SELECT
    stream_name,
    COUNT(*) AS total_errors,
    COUNT(*) FILTER (WHERE NOT resolved) AS unresolved,
    MIN(failed_at) AS oldest_error,
    MAX(failed_at) AS latest_error
FROM airbyte_raw.l2_dead_letter_queue
GROUP BY stream_name;

CREATE OR REPLACE VIEW prod_sync.v_batch_status AS
SELECT
    w.stream_name,
    w.last_synced_at,
    w.last_entity_id,
    w.last_run_at,
    w.rows_processed,
    (SELECT COUNT(*) FROM prod_sync.sigmasz_leads WHERE (_synced_at, lead_id) > (w.last_synced_at, w.last_entity_id) AND _synced_at < NOW() - INTERVAL '5 seconds') AS pending_rows,
    EXTRACT(EPOCH FROM (NOW() - w.last_run_at)) / 60 AS lag_minutes,
    (SELECT COUNT(*) FROM airbyte_raw.l2_dead_letter_queue dlq WHERE dlq.stream_name LIKE w.stream_name || '%' AND dlq.resolved = FALSE) AS dlq_unresolved,
    (SELECT COUNT(*) FROM airbyte_raw.l2_dead_letter_queue dlq WHERE dlq.stream_name LIKE w.stream_name || '%' AND dlq.resolved = FALSE AND dlq.retry_count > 1) AS dlq_quarantined
FROM prod_sync.l3_batch_watermarks w;

CREATE OR REPLACE VIEW prod_sync.v_recent_tombstones AS
SELECT entity_type, entity_id, deleted_at, _synced_at
FROM prod_sync.deleted_entities_log
ORDER BY deleted_at DESC LIMIT 100;
