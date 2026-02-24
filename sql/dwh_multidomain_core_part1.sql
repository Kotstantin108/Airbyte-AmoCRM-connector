-- =============================================================================
-- MULTI-DOMAIN DWH — ЧАСТЬ 1: МИГРАЦИЯ ИНФРАСТРУКТУРЫ (ONE TIME)
-- =============================================================================
-- Выполняется один раз перед подключением новых доменов.
-- Не трогает данные sigmasz — только расширяет схему.
--
-- ПОРЯДОК ВЫПОЛНЕНИЯ:
--   1. Этот файл (dwh_multidomain_core_part1.sql)
--   2. dwh_multidomain_core_part2.sql (генераторы + авто-обнаружение)
-- =============================================================================

BEGIN;

-- =============================================================================
-- 0.1. Добавляем domain в Tombstone Shield
-- =============================================================================
ALTER TABLE prod_sync.deleted_entities_log 
    ADD COLUMN IF NOT EXISTS domain TEXT NOT NULL DEFAULT 'sigmasz';

ALTER TABLE prod_sync.deleted_entities_log DROP CONSTRAINT IF EXISTS deleted_entities_log_pkey;
ALTER TABLE prod_sync.deleted_entities_log ADD PRIMARY KEY (domain, entity_type, entity_id);

CREATE INDEX IF NOT EXISTS idx_deleted_entities_log_domain 
    ON prod_sync.deleted_entities_log (domain, entity_type);

COMMENT ON COLUMN prod_sync.deleted_entities_log.domain IS 'Субдомен AmoCRM-источника. Изолирует Shield между доменами.';

-- =============================================================================
-- 0.2. register_tombstone (TEXT, TEXT, BIGINT)
-- =============================================================================
DROP FUNCTION IF EXISTS prod_sync.register_tombstone(TEXT, BIGINT) CASCADE;
CREATE OR REPLACE FUNCTION prod_sync.register_tombstone(p_domain TEXT, p_entity_type TEXT, p_entity_id BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    INSERT INTO prod_sync.deleted_entities_log (domain, entity_type, entity_id, deleted_at, _synced_at)
    VALUES (p_domain, p_entity_type, p_entity_id, NOW(), NOW())
    ON CONFLICT (domain, entity_type, entity_id) DO UPDATE SET deleted_at = NOW(), _synced_at = NOW();
END;
$$;

-- =============================================================================
-- 0.3. is_tombstoned (TEXT, TEXT, BIGINT)
-- =============================================================================
DROP FUNCTION IF EXISTS prod_sync.is_tombstoned(TEXT, BIGINT) CASCADE;
CREATE OR REPLACE FUNCTION prod_sync.is_tombstoned(p_domain TEXT, p_entity_type TEXT, p_entity_id BIGINT)
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT EXISTS (SELECT 1 FROM prod_sync.deleted_entities_log WHERE domain = p_domain AND entity_type = p_entity_type AND entity_id = p_entity_id);
$$;

-- =============================================================================
-- 0.4. l3_batch_watermarks — миграция (только лиды)
-- =============================================================================
ALTER TABLE prod_sync.l3_batch_watermarks ADD COLUMN IF NOT EXISTS l2_table_name TEXT, ADD COLUMN IF NOT EXISTS l2_id_col TEXT;
UPDATE prod_sync.l3_batch_watermarks SET l2_table_name = 'sigmasz_leads', l2_id_col = 'lead_id' WHERE stream_name = 'sigmasz_leads' AND l2_table_name IS NULL;

-- =============================================================================
-- 0.5. v_batch_status и v_recent_tombstones
-- =============================================================================
CREATE OR REPLACE FUNCTION prod_sync.get_batch_pending_rows(p_stream_name TEXT)
RETURNS BIGINT LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_table TEXT; v_id_col TEXT; v_from_ts TIMESTAMPTZ; v_from_id BIGINT; v_count BIGINT;
BEGIN
    SELECT l2_table_name, l2_id_col, last_synced_at, last_entity_id INTO v_table, v_id_col, v_from_ts, v_from_id FROM prod_sync.l3_batch_watermarks WHERE stream_name = p_stream_name;
    IF v_table IS NULL THEN RETURN 0; END IF;
    EXECUTE format('SELECT COUNT(*) FROM prod_sync.%I WHERE (_synced_at, %I) > ($1, $2) AND _synced_at < NOW() - INTERVAL ''5 seconds''', v_table, v_id_col) INTO v_count USING v_from_ts, v_from_id;
    RETURN v_count;
END;
$$;

DROP VIEW IF EXISTS prod_sync.v_batch_status;
CREATE OR REPLACE VIEW prod_sync.v_batch_status AS
SELECT
    w.stream_name, w.l2_table_name, w.last_synced_at, w.last_entity_id, w.last_run_at, w.rows_processed,
    prod_sync.get_batch_pending_rows(w.stream_name) AS pending_rows,
    EXTRACT(EPOCH FROM (NOW() - w.last_run_at)) / 60 AS lag_minutes,
    (SELECT COUNT(*) FROM airbyte_raw.l2_dead_letter_queue dlq WHERE dlq.stream_name LIKE w.stream_name || '%' AND dlq.resolved = FALSE) AS dlq_unresolved,
    (SELECT COUNT(*) FROM airbyte_raw.l2_dead_letter_queue dlq WHERE dlq.stream_name LIKE w.stream_name || '%' AND dlq.resolved = FALSE AND dlq.retry_count > 1) AS dlq_quarantined
FROM prod_sync.l3_batch_watermarks w;

DROP VIEW IF EXISTS prod_sync.v_recent_tombstones;
CREATE OR REPLACE VIEW prod_sync.v_recent_tombstones AS
SELECT domain, entity_type, entity_id, deleted_at, _synced_at FROM prod_sync.deleted_entities_log ORDER BY deleted_at DESC LIMIT 100;

-- =============================================================================
-- 0.6. ОБНОВЛЕНИЕ ФУНКЦИЙ SIGMASZ (С НОВЫМ SHIELD И SEARCH_PATH)
-- =============================================================================
CREATE OR REPLACE FUNCTION airbyte_raw.propagate_deleted_to_l2() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    IF NEW.type = 'lead_deleted' AND NEW.entity_type = 'lead' THEN PERFORM prod_sync.register_tombstone('sigmasz', 'lead', NEW.entity_id); UPDATE prod_sync.sigmasz_leads SET is_deleted = TRUE, _synced_at = NOW() WHERE lead_id = NEW.entity_id; END IF;
    IF NEW.type = 'contact_deleted' AND NEW.entity_type = 'contact' THEN PERFORM prod_sync.register_tombstone('sigmasz', 'contact', NEW.entity_id); UPDATE prod_sync.sigmasz_contacts SET is_deleted = TRUE, _synced_at = NOW() WHERE contact_id = NEW.entity_id; END IF;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN RAISE WARNING '[propagate_deleted_to_l2] entity=% id=% error=%', NEW.entity_type, NEW.entity_id, SQLERRM; RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION prod_sync.process_embedded_contacts(p_contacts_json JSONB, p_lead_id BIGINT, p_explicit_empty BOOLEAN DEFAULT FALSE) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE v_c JSONB; v_cid BIGINT; v_ts TIMESTAMPTZ; v_main BOOLEAN; v_ids BIGINT[] := ARRAY[]::BIGINT[];
BEGIN
    IF p_explicit_empty THEN DELETE FROM prod_sync.sigmasz_lead_contacts WHERE lead_id = p_lead_id; RETURN; END IF;
    FOR v_c IN SELECT value FROM jsonb_array_elements(p_contacts_json) LOOP
        IF jsonb_typeof(v_c) <> 'object' THEN CONTINUE; END IF;
        BEGIN v_cid := NULLIF(BTRIM(COALESCE(v_c->>'id','')),'')::BIGINT; EXCEPTION WHEN OTHERS THEN CONTINUE; END;
        IF v_cid IS NULL OR prod_sync.is_tombstoned('sigmasz', 'contact', v_cid) THEN CONTINUE; END IF;
        v_ts := prod_sync.safe_cf_to_timestamp(NULLIF(BTRIM(COALESCE(v_c->>'updated_at','')),'')); v_main := COALESCE((v_c->>'is_main')::BOOLEAN,FALSE);
        INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at) VALUES (v_cid, v_c->>'name', v_ts, v_c, FALSE, NOW()) ON CONFLICT (contact_id) DO UPDATE SET name = EXCLUDED.name, updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_contacts.updated_at), EXCLUDED.updated_at, sigmasz_contacts.updated_at), raw_json = sigmasz_contacts.raw_json || EXCLUDED.raw_json, _synced_at = NOW();
        INSERT INTO prod_sync.sigmasz_lead_contacts (lead_id, contact_id, is_main) VALUES (p_lead_id, v_cid, v_main) ON CONFLICT (lead_id, contact_id) DO UPDATE SET is_main = EXCLUDED.is_main;
        v_ids := array_append(v_ids, v_cid);
    END LOOP;
    IF array_length(v_ids, 1) > 0 THEN DELETE FROM prod_sync.sigmasz_lead_contacts WHERE lead_id = p_lead_id AND contact_id <> ALL(v_ids); END IF;
END;
$$;

CREATE OR REPLACE FUNCTION prod_sync.get_best_contact_for_lead(p_lead_id BIGINT) RETURNS TABLE (c_id BIGINT, c_name TEXT, c_phone TEXT, c_email TEXT) LANGUAGE plpgsql STABLE AS $$
BEGIN RETURN QUERY
    SELECT c.contact_id, c.name::TEXT, (SELECT cp.phone FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id ORDER BY cp.phone LIMIT 1), (SELECT ce.email FROM prod_sync.sigmasz_contact_emails ce WHERE ce.contact_id = c.contact_id ORDER BY ce.email LIMIT 1)
    FROM prod_sync.sigmasz_lead_contacts lc JOIN prod_sync.sigmasz_contacts c ON c.contact_id = lc.contact_id
    WHERE lc.lead_id = p_lead_id AND COALESCE(c.is_deleted, FALSE) IS FALSE AND NOT prod_sync.is_tombstoned('sigmasz', 'contact', c.contact_id)
    ORDER BY COALESCE(lc.is_main, FALSE) DESC, c.contact_id ASC LIMIT 1;
END;
$$;

-- ИСПРАВЛЕНО: Добавлен SET search_path и ветка WHEN OTHERS
CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2() 
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER 
SET search_path = airbyte_raw, prod_sync, public 
AS $$
DECLARE v_lid BIGINT; v_cts TIMESTAMPTZ; v_uts TIMESTAMPTZ; v_emb JSONB := '{}'; v_cf JSONB := '[]'; v_rj JSONB; v_ec JSONB;
BEGIN
    v_lid := NEW.id; IF v_lid IS NULL OR prod_sync.is_tombstoned('sigmasz', 'lead', v_lid) THEN RETURN NEW; END IF;
    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN PERFORM prod_sync.register_tombstone('sigmasz', 'lead', v_lid); UPDATE prod_sync.sigmasz_leads SET is_deleted = TRUE, _synced_at = NOW() WHERE lead_id = v_lid; RETURN NEW; END IF;
    BEGIN IF pg_typeof(NEW.created_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN v_cts := prod_sync.safe_cf_to_timestamp(NEW.created_at::TEXT); ELSE v_cts := NEW.created_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_cts := NULL; END;
    BEGIN IF pg_typeof(NEW.updated_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN v_uts := prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT); ELSE v_uts := NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_uts := NULL; END;
    BEGIN IF NEW._embedded IS NOT NULL AND BTRIM(NEW._embedded::TEXT) NOT IN ('','null') THEN v_emb := NEW._embedded::JSONB; IF jsonb_typeof(v_emb) <> 'object' THEN v_emb := '{}'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_emb := '{}'; END;
    BEGIN IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN ('','null') THEN v_cf := NEW.custom_fields_values::JSONB; IF jsonb_typeof(v_cf) <> 'array' THEN v_cf := '[]'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_cf := '[]'; END;
    v_rj := jsonb_build_object('id', NEW.id, 'name', NEW.name, 'status_id', NEW.status_id, 'pipeline_id', NEW.pipeline_id, 'price', NEW.price, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, '_embedded', v_emb, 'custom_fields_values', v_cf);
    INSERT INTO prod_sync.sigmasz_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json, is_deleted, _synced_at) VALUES (v_lid, NEW.name, NEW.status_id, NEW.pipeline_id, NEW.price, v_cts, v_uts, v_rj, FALSE, NOW()) ON CONFLICT (lead_id) DO UPDATE SET name = EXCLUDED.name, status_id = EXCLUDED.status_id, pipeline_id = EXCLUDED.pipeline_id, price = EXCLUDED.price, created_at = COALESCE(GREATEST(EXCLUDED.created_at, sigmasz_leads.created_at), EXCLUDED.created_at, sigmasz_leads.created_at), updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_leads.updated_at), EXCLUDED.updated_at, sigmasz_leads.updated_at), raw_json = EXCLUDED.raw_json, is_deleted = FALSE, _synced_at = NOW();
    v_ec := v_emb -> 'contacts'; IF v_ec IS NOT NULL AND jsonb_typeof(v_ec) = 'array' THEN PERFORM prod_sync.process_embedded_contacts(v_ec, v_lid, jsonb_array_length(v_ec) = 0); END IF;
    RETURN NEW;
EXCEPTION 
    WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN
        BEGIN INSERT INTO airbyte_raw.l2_dead_letter_queue (stream_name, entity_id, raw_record, error_message, sqlstate) VALUES ('sigmasz_leads', v_lid, to_jsonb(NEW), SQLERRM, SQLSTATE); EXCEPTION WHEN OTHERS THEN NULL; END; 
        RAISE WARNING 'DLQ: %', SQLERRM; 
        RETURN NEW;
    WHEN OTHERS THEN
        RAISE EXCEPTION '[unpack_sigmasz_leads_l2] TRANSIENT ERROR lead_id=%: % (STATE: %)', v_lid, SQLERRM, SQLSTATE;
END;
$$;

-- ИСПРАВЛЕНО: Добавлен SET search_path и ветка WHEN OTHERS
CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2() 
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER 
SET search_path = airbyte_raw, prod_sync, public 
AS $$
DECLARE v_cid BIGINT; v_uts TIMESTAMPTZ; v_cf JSONB := '[]'; v_rj JSONB; v_ph TEXT; v_em TEXT;
BEGIN
    v_cid := NEW.id; IF v_cid IS NULL OR prod_sync.is_tombstoned('sigmasz', 'contact', v_cid) THEN RETURN NEW; END IF;
    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN PERFORM prod_sync.register_tombstone('sigmasz', 'contact', v_cid); UPDATE prod_sync.sigmasz_contacts SET is_deleted = TRUE, _synced_at = NOW() WHERE contact_id = v_cid; RETURN NEW; END IF;
    BEGIN IF pg_typeof(NEW.updated_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN v_uts := prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT); ELSE v_uts := NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_uts := NULL; END;
    BEGIN IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN ('','null') THEN v_cf := NEW.custom_fields_values::JSONB; IF jsonb_typeof(v_cf) <> 'array' THEN v_cf := '[]'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_cf := '[]'; END;
    v_rj := jsonb_build_object('id', NEW.id, 'name', NEW.name, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, 'custom_fields_values', v_cf);
    INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at) VALUES (v_cid, NEW.name, v_uts, v_rj, FALSE, NOW()) ON CONFLICT (contact_id) DO UPDATE SET name = EXCLUDED.name, updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_contacts.updated_at), EXCLUDED.updated_at, sigmasz_contacts.updated_at), raw_json = EXCLUDED.raw_json, is_deleted = FALSE, _synced_at = NOW();
    DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_cid;
    FOR v_ph IN SELECT DISTINCT public.normalize_phone(v.value->>'value') FROM jsonb_array_elements(v_cf) cf CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]' END) v WHERE cf->>'field_code' = 'PHONE' AND public.normalize_phone(v.value->>'value') IS NOT NULL AND length(public.normalize_phone(v.value->>'value')) >= 10 LOOP
        INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone) VALUES (v_cid, v_ph) ON CONFLICT DO NOTHING; END LOOP;
    DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_cid;
    FOR v_em IN SELECT DISTINCT lower(BTRIM(v.value->>'value')) FROM jsonb_array_elements(v_cf) cf CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]' END) v WHERE cf->>'field_code' = 'EMAIL' AND NULLIF(BTRIM(v.value->>'value'),'') IS NOT NULL LOOP
        INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email) VALUES (v_cid, v_em) ON CONFLICT DO NOTHING; END LOOP;
    RETURN NEW;
EXCEPTION 
    WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN
        BEGIN INSERT INTO airbyte_raw.l2_dead_letter_queue (stream_name, entity_id, raw_record, error_message, sqlstate) VALUES ('sigmasz_contacts', v_cid, to_jsonb(NEW), SQLERRM, SQLSTATE); EXCEPTION WHEN OTHERS THEN NULL; END; 
        RAISE WARNING 'DLQ: %', SQLERRM; 
        RETURN NEW;
    WHEN OTHERS THEN
        RAISE EXCEPTION '[unpack_sigmasz_contacts_l2] TRANSIENT ERROR contact_id=%: % (STATE: %)', v_cid, SQLERRM, SQLSTATE;
END;
$$;

COMMIT;

-- Конец Части 1. Продолжение: dwh_multidomain_core_part2.sql
