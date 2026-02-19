-- ============================================
-- НАСТРОЙКА С НУЛЯ (нормализованные таблицы Airbyte)
-- ============================================
-- 1. Удаляет все триггеры
-- 2. Создаёт/заменяет все функции (нормализованные: колонки id, name, type; events — type, entity_id, entity_type)
-- 3. Создаёт все триггеры заново
--
-- Требуется ДО запуска: схемы prod_sync, analytics; таблицы L2 и L3;
-- normalize_phone(), process_embedded_contacts() (из analysis_and_improvements.md).
--
-- После этого скрипта запустите backfill при необходимости: sql/backfill_initial_normalized.sql

-- =============================================================================
-- ШАГ 0: Удаление всех триггеров
-- =============================================================================

DROP TRIGGER IF EXISTS trg_sigmasz_leads_l2 ON airbyte_raw.sigmasz_leads;
DROP TRIGGER IF EXISTS trg_sigmasz_contacts_l2 ON airbyte_raw.sigmasz_contacts;
DROP TRIGGER IF EXISTS trg_propagate_deleted_l2 ON airbyte_raw.sigmasz_events;
DROP TRIGGER IF EXISTS trg_l2_l3_leads_sigmasz ON prod_sync.sigmasz_leads;
DROP TRIGGER IF EXISTS trg_l2_l3_contacts_sigmasz ON prod_sync.sigmasz_contacts;
DROP TRIGGER IF EXISTS trg_schema_sigmasz_leads ON airbyte_raw.sigmasz_custom_fields_leads;
DROP TRIGGER IF EXISTS trg_schema_sigmasz_contacts ON airbyte_raw.sigmasz_custom_fields_contacts;

-- =============================================================================
-- ШАГ 1: Функции L1→L2 (нормализованные leads/contacts; events по колонкам type, entity_id, entity_type)
-- =============================================================================

CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2()
RETURNS TRIGGER AS $$
DECLARE v_is_deleted BOOLEAN := FALSE; v_lead_id BIGINT; v_embedded_contacts JSONB; v_raw_json JSONB; v_created_ts TIMESTAMPTZ; v_updated_ts TIMESTAMPTZ;
BEGIN
    v_lead_id := NEW.id; IF v_lead_id IS NULL THEN RETURN NEW; END IF;
    SELECT EXISTS (SELECT 1 FROM airbyte_raw.sigmasz_events e WHERE e.type = 'lead_deleted' AND e.entity_type = 'lead' AND e.entity_id = v_lead_id) INTO v_is_deleted;
    IF NEW.is_deleted IS NOT NULL THEN v_is_deleted := COALESCE(NEW.is_deleted, v_is_deleted); END IF;
    -- Удалённые сделки не храним в prod_sync и analytics: удаляем строки, не вставляем
    IF v_is_deleted THEN
        DELETE FROM analytics.sigmasz_leads WHERE lead_id = v_lead_id;
        DELETE FROM prod_sync.sigmasz_leads WHERE lead_id = v_lead_id;
        RETURN NEW;
    END IF;
    BEGIN IF pg_typeof(NEW.created_at) = 'bigint'::regtype OR pg_typeof(NEW.created_at) = 'integer'::regtype THEN v_created_ts := to_timestamp(NEW.created_at::BIGINT); ELSE v_created_ts := NEW.created_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_created_ts := NULL; END;
    BEGIN IF pg_typeof(NEW.updated_at) = 'bigint'::regtype OR pg_typeof(NEW.updated_at) = 'integer'::regtype THEN v_updated_ts := to_timestamp(NEW.updated_at::BIGINT); ELSE v_updated_ts := NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_updated_ts := NULL; END;
    v_raw_json := jsonb_build_object('id', NEW.id, 'name', NEW.name, 'status_id', NEW.status_id, 'pipeline_id', NEW.pipeline_id, 'price', NEW.price, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, '_embedded', CASE WHEN pg_typeof(NEW._embedded) = 'jsonb'::regtype THEN NEW._embedded WHEN NEW._embedded IS NOT NULL THEN (NEW._embedded::TEXT)::JSONB ELSE '{}'::jsonb END, 'custom_fields_values', CASE WHEN pg_typeof(NEW.custom_fields_values) = 'jsonb'::regtype THEN NEW.custom_fields_values WHEN NEW.custom_fields_values IS NOT NULL THEN (NEW.custom_fields_values::TEXT)::JSONB ELSE '[]'::jsonb END);
    INSERT INTO prod_sync.sigmasz_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json, is_deleted, _synced_at) VALUES (v_lead_id, NEW.name, NEW.status_id, NEW.pipeline_id, NEW.price, v_created_ts, v_updated_ts, v_raw_json, FALSE, NOW()) ON CONFLICT (lead_id) DO UPDATE SET name = EXCLUDED.name, status_id = EXCLUDED.status_id, pipeline_id = EXCLUDED.pipeline_id, price = EXCLUDED.price, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json, is_deleted = FALSE, _synced_at = NOW();
    v_embedded_contacts := CASE WHEN pg_typeof(NEW._embedded) = 'jsonb'::regtype THEN NEW._embedded -> 'contacts' WHEN NEW._embedded IS NOT NULL THEN (NEW._embedded::TEXT)::JSONB -> 'contacts' ELSE NULL END;
    IF v_embedded_contacts IS NOT NULL AND jsonb_typeof(v_embedded_contacts) = 'array' THEN PERFORM prod_sync.process_embedded_contacts(v_embedded_contacts, v_lead_id); END IF;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN RAISE WARNING 'Error unpack_sigmasz_leads_l2 lead_id %: %', v_lead_id, SQLERRM; RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2()
RETURNS TRIGGER AS $$
DECLARE v_is_deleted BOOLEAN := FALSE; v_contact_id BIGINT; v_raw_json JSONB; v_updated_ts TIMESTAMPTZ; v_phones JSONB; v_emails JSONB; v_phone TEXT; v_email TEXT;
BEGIN
    v_contact_id := NEW.id; IF v_contact_id IS NULL THEN RETURN NEW; END IF;
    SELECT EXISTS (SELECT 1 FROM airbyte_raw.sigmasz_events e WHERE e.type = 'contact_deleted' AND e.entity_type = 'contact' AND e.entity_id = v_contact_id) INTO v_is_deleted;
    IF NEW.is_deleted IS NOT NULL THEN v_is_deleted := COALESCE(NEW.is_deleted, v_is_deleted); END IF;
    -- Удалённые контакты не храним в prod_sync и analytics: удаляем строки, не вставляем
    IF v_is_deleted THEN
        DELETE FROM analytics.sigmasz_contacts WHERE contact_id = v_contact_id;
        DELETE FROM prod_sync.sigmasz_contacts WHERE contact_id = v_contact_id;
        RETURN NEW;
    END IF;
    BEGIN IF pg_typeof(NEW.updated_at) = 'bigint'::regtype OR pg_typeof(NEW.updated_at) = 'integer'::regtype THEN v_updated_ts := to_timestamp(NEW.updated_at::BIGINT); ELSE v_updated_ts := NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_updated_ts := NULL; END;
    v_raw_json := jsonb_build_object('id', NEW.id, 'name', NEW.name, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, 'custom_fields_values', CASE WHEN pg_typeof(NEW.custom_fields_values) = 'jsonb'::regtype THEN NEW.custom_fields_values WHEN NEW.custom_fields_values IS NOT NULL THEN (NEW.custom_fields_values::TEXT)::JSONB ELSE '[]'::jsonb END);
    INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at) VALUES (v_contact_id, NEW.name, v_updated_ts, v_raw_json, FALSE, NOW()) ON CONFLICT (contact_id) DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json, is_deleted = FALSE, _synced_at = NOW();
    DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_contact_id; DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_contact_id;
    v_phones := CASE WHEN pg_typeof(NEW.custom_fields_values) = 'jsonb'::regtype THEN NEW.custom_fields_values WHEN NEW.custom_fields_values IS NOT NULL THEN (NEW.custom_fields_values::TEXT)::JSONB ELSE '[]'::jsonb END; v_emails := v_phones;
    IF v_phones IS NOT NULL AND jsonb_typeof(v_phones) = 'array' THEN FOR v_phone IN SELECT DISTINCT normalize_phone(v.value ->> 'value') FROM jsonb_array_elements(v_phones) AS cf CROSS JOIN LATERAL jsonb_array_elements(COALESCE(cf -> 'values', '[]'::jsonb)) AS v WHERE cf ->> 'field_code' = 'PHONE' AND normalize_phone(v.value ->> 'value') IS NOT NULL AND length(normalize_phone(v.value ->> 'value')) >= 10 LOOP INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone) VALUES (v_contact_id, v_phone) ON CONFLICT (contact_id, phone) DO NOTHING; END LOOP; END IF;
    IF v_emails IS NOT NULL AND jsonb_typeof(v_emails) = 'array' THEN FOR v_email IN SELECT DISTINCT lower(v.value ->> 'value') FROM jsonb_array_elements(v_emails) AS cf CROSS JOIN LATERAL jsonb_array_elements(COALESCE(cf -> 'values', '[]'::jsonb)) AS v WHERE cf ->> 'field_code' = 'EMAIL' AND v.value ->> 'value' IS NOT NULL LOOP INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email) VALUES (v_contact_id, v_email) ON CONFLICT DO NOTHING; END LOOP; END IF;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN RAISE WARNING 'Error unpack_sigmasz_contacts_l2 contact_id %: %', v_contact_id, SQLERRM; RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- events: нормализованная таблица (колонки type, entity_id, entity_type)
CREATE OR REPLACE FUNCTION airbyte_raw.propagate_deleted_to_l2()
RETURNS TRIGGER AS $$
DECLARE v_entity_id BIGINT; v_event_type TEXT; v_entity_type TEXT;
BEGIN
    v_event_type := NEW.type; v_entity_type := NEW.entity_type; v_entity_id := NEW.entity_id;
    -- Удалённые сделки/контакты убираем из prod_sync и analytics (не храним)
    IF v_event_type = 'lead_deleted' AND v_entity_type = 'lead' THEN
        DELETE FROM analytics.sigmasz_leads WHERE lead_id = v_entity_id;
        DELETE FROM prod_sync.sigmasz_leads WHERE lead_id = v_entity_id;
    END IF;
    IF v_event_type = 'contact_deleted' AND v_entity_type = 'contact' THEN
        DELETE FROM analytics.sigmasz_contacts WHERE contact_id = v_entity_id;
        DELETE FROM prod_sync.sigmasz_contacts WHERE contact_id = v_entity_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ШАГ 2: View и sync_schema (нормализованные custom_fields: cf.id, cf.name)
-- =============================================================================

CREATE OR REPLACE FUNCTION analytics.rebuild_view_sigmasz_leads()
RETURNS VOID AS $$
DECLARE v_cols TEXT;
BEGIN
    SELECT string_agg(format('l.%I AS %I', 'f_' || sub.id::TEXT, sub.alias), E',\n    ') INTO v_cols
    FROM (
        SELECT cf.id, CASE WHEN count(*) OVER (PARTITION BY cf.name) > 1 THEN cf.name || '_' || cf.id::TEXT ELSE cf.name END AS alias
        FROM airbyte_raw.sigmasz_custom_fields_leads cf
        WHERE EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'analytics' AND table_name = 'sigmasz_leads' AND column_name = 'f_' || cf.id::TEXT
        )
    ) sub;
    IF v_cols IS NULL THEN v_cols := 'NULL::text AS "Нет полей"'; END IF;
    EXECUTE 'DROP VIEW IF EXISTS analytics.sigmasz_leads_human CASCADE';
    EXECUTE format(
        'CREATE VIEW analytics.%I AS SELECT '
        'l.lead_id AS "ID", '
        'l.name AS "Название", '
        'l.status_id AS "Статус", '
        'l.pipeline_id AS "Воронка", '
        'l.price AS "Бюджет", '
        'l.created_at AS "Дата создания", '
        'l.updated_at AS "Дата обновления", '
        'l.is_deleted AS "Удален", '
        'l.contact_id AS "ID контакта", '
        'l.contact_name AS "Имя контакта", '
        'l.contact_phone AS "Телефон", '
        'l.contact_email AS "Email", '
        '%s FROM analytics.%I l',
        'sigmasz_leads_human', v_cols, 'sigmasz_leads');
    RAISE NOTICE 'rebuild_view_sigmasz_leads: done.';
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION analytics.rebuild_view_sigmasz_contacts()
RETURNS VOID AS $$
DECLARE v_cols TEXT;
BEGIN
    SELECT string_agg(format('c.%I AS %I', 'f_' || sub.id::TEXT, sub.alias), E',\n    ') INTO v_cols
    FROM (
        SELECT cf.id, CASE WHEN count(*) OVER (PARTITION BY cf.name) > 1 THEN cf.name || '_' || cf.id::TEXT ELSE cf.name END AS alias
        FROM airbyte_raw.sigmasz_custom_fields_contacts cf
        WHERE EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'analytics' AND table_name = 'sigmasz_contacts' AND column_name = 'f_' || cf.id::TEXT
        )
    ) sub;
    IF v_cols IS NULL THEN v_cols := 'NULL::text AS "Нет полей"'; END IF;
    EXECUTE 'DROP VIEW IF EXISTS analytics.sigmasz_contacts_human CASCADE';
    EXECUTE format('CREATE VIEW analytics.%I AS SELECT c.contact_id AS "ID", c.name AS "Название", c.updated_at AS "Дата обновления", c.is_deleted AS "Удален", %s FROM analytics.%I c', 'sigmasz_contacts_human', v_cols, 'sigmasz_contacts');
    RAISE NOTICE 'rebuild_view_sigmasz_contacts: done.';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analytics.sync_schema_sigmasz_leads()
RETURNS TRIGGER AS $$
DECLARE v_field_id TEXT; v_field_type TEXT; v_col_name TEXT; v_col_type TEXT; v_needs_rebuild BOOLEAN := FALSE; v_col_exists BOOLEAN;
BEGIN
    v_field_id := NEW.id::TEXT; v_col_name := 'f_' || v_field_id;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_leads' AND column_name = v_col_name) INTO v_col_exists;
    IF v_col_exists THEN RETURN NEW; END IF;
    v_field_type := COALESCE(NEW.type::TEXT, 'text');
    v_col_type := CASE WHEN v_field_type = 'numeric' THEN 'NUMERIC' WHEN v_field_type IN ('date', 'date_time', 'birthday') THEN 'TIMESTAMPTZ' WHEN v_field_type = 'checkbox' THEN 'BOOLEAN' WHEN v_field_type IN ('select', 'multiselect', 'radiobutton', 'text', 'textarea', 'url', 'tracking_data') THEN 'TEXT' ELSE 'TEXT' END;
    BEGIN EXECUTE format('ALTER TABLE analytics.%I ADD COLUMN %I %s', 'sigmasz_leads', v_col_name, v_col_type); v_needs_rebuild := TRUE; EXCEPTION WHEN duplicate_column THEN NULL; WHEN OTHERS THEN RAISE WARNING 'Error adding column %: %', v_col_name, SQLERRM; END;
    IF v_needs_rebuild THEN BEGIN PERFORM analytics.rebuild_view_sigmasz_leads(); EXCEPTION WHEN OTHERS THEN RAISE WARNING 'Error rebuilding view: %', SQLERRM; END; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analytics.sync_schema_sigmasz_contacts()
RETURNS TRIGGER AS $$
DECLARE v_field_id TEXT; v_field_type TEXT; v_col_name TEXT; v_col_type TEXT; v_needs_rebuild BOOLEAN := FALSE; v_col_exists BOOLEAN;
BEGIN
    v_field_id := NEW.id::TEXT; v_col_name := 'f_' || v_field_id;
    SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_contacts' AND column_name = v_col_name) INTO v_col_exists;
    IF v_col_exists THEN RETURN NEW; END IF;
    v_field_type := COALESCE(NEW.type::TEXT, 'text');
    v_col_type := CASE WHEN v_field_type = 'numeric' THEN 'NUMERIC' WHEN v_field_type IN ('date', 'date_time', 'birthday') THEN 'TIMESTAMPTZ' WHEN v_field_type = 'checkbox' THEN 'BOOLEAN' WHEN v_field_type IN ('select', 'multiselect', 'radiobutton', 'text', 'textarea', 'url', 'tracking_data') THEN 'TEXT' ELSE 'TEXT' END;
    BEGIN EXECUTE format('ALTER TABLE analytics.%I ADD COLUMN %I %s', 'sigmasz_contacts', v_col_name, v_col_type); v_needs_rebuild := TRUE; EXCEPTION WHEN duplicate_column THEN NULL; WHEN OTHERS THEN RAISE WARNING 'Error adding column %: %', v_col_name, SQLERRM; END;
    IF v_needs_rebuild THEN BEGIN PERFORM analytics.rebuild_view_sigmasz_contacts(); EXCEPTION WHEN OTHERS THEN RAISE WARNING 'Error rebuilding view: %', SQLERRM; END; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ШАГ 3: Функции L2→L3 (триггерные + для backfill)
-- =============================================================================

-- Безопасное преобразование значения кастомного поля в timestamp (без overflow/out of range)
CREATE OR REPLACE FUNCTION prod_sync.safe_cf_to_timestamp(val TEXT)
RETURNS TIMESTAMPTZ AS $$
DECLARE
    v_big BIGINT;
    v_sec BIGINT;
BEGIN
    IF val IS NULL OR val !~ '^\d+$' THEN RETURN NULL; END IF;
    IF length(val) > 15 THEN RETURN NULL; END IF;  -- больше 15 цифр — не timestamp
    BEGIN
        v_big := val::BIGINT;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;  -- value out of range for bigint
    END;
    IF length(val) = 13 THEN
        v_sec := v_big / 1000;  -- миллисекунды -> секунды
    ELSIF length(val) >= 10 AND length(val) <= 11 THEN
        v_sec := v_big;
        IF v_big < 1000000000 THEN RETURN NULL; END IF;  -- до 2001 года в сек
    ELSIF length(val) IN (8, 9) THEN
        v_sec := v_big;  -- Unix секунды (8–9 цифр, напр. 45349200 = 1971-06-14)
    ELSE
        RETURN NULL;  -- нестандартная длина — не трогаем
    END IF;
    IF v_sec > 2147483647 THEN RETURN NULL; END IF;  -- после 2038 в сек
    RETURN to_timestamp(v_sec::DOUBLE PRECISION);
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION prod_sync.propagate_one_lead_to_l3(p_lead_id BIGINT)
RETURNS VOID AS $$
DECLARE r prod_sync.sigmasz_leads%ROWTYPE; v_flat_json JSONB; v_sql TEXT; v_cols TEXT[]; v_set_clause TEXT; v_insert_cols TEXT; v_filtered_json JSONB;
BEGIN
    SELECT * INTO r FROM prod_sync.sigmasz_leads WHERE lead_id = p_lead_id; IF NOT FOUND THEN RETURN; END IF;
    SELECT jsonb_object_agg('f_' || COALESCE(elem.value ->> 'field_id', elem.value ->> 'id'),
      CASE WHEN cf.type IN ('date', 'date_time', 'birthday') THEN
        CASE WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d+$' THEN to_jsonb(prod_sync.safe_cf_to_timestamp(elem.value -> 'values' -> 0 ->> 'value'))
             WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_jsonb(to_timestamp(elem.value -> 'values' -> 0 ->> 'value', 'DD.MM.YYYY')::timestamptz)
             ELSE to_jsonb(NULL::timestamptz) END
      ELSE (elem.value -> 'values' -> 0 -> 'value') END) INTO v_flat_json
    FROM jsonb_array_elements(COALESCE(r.raw_json -> 'custom_fields_values', '[]'::jsonb)) elem(value)
    LEFT JOIN airbyte_raw.sigmasz_custom_fields_leads cf ON cf.id::text = COALESCE(elem.value ->> 'field_id', elem.value ->> 'id')
    WHERE COALESCE(elem.value ->> 'field_id', elem.value ->> 'id') IS NOT NULL;
    IF v_flat_json IS NULL THEN v_flat_json := '{}'::jsonb; END IF;
    v_flat_json := v_flat_json || jsonb_build_object('lead_id', r.lead_id, 'name', r.name, 'status_id', r.status_id, 'pipeline_id', r.pipeline_id, 'price', r.price, 'created_at', r.created_at, 'updated_at', r.updated_at, 'is_deleted', r.is_deleted, '_synced_at', NOW());
    SELECT array_agg(column_name::TEXT ORDER BY ordinal_position) INTO v_cols FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_leads';
    SELECT jsonb_object_agg(key, value) INTO v_filtered_json FROM jsonb_each(v_flat_json) WHERE key = ANY(v_cols);
    SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
    SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col != 'lead_id';
    v_sql := format('INSERT INTO analytics.%I (%s) SELECT * FROM jsonb_populate_record(NULL::analytics.%I, $1) ON CONFLICT (lead_id) DO UPDATE SET %s', 'sigmasz_leads', v_insert_cols, 'sigmasz_leads', v_set_clause);
    EXECUTE v_sql USING COALESCE(v_filtered_json, '{}'::jsonb);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION prod_sync.propagate_one_contact_to_l3(p_contact_id BIGINT)
RETURNS VOID AS $$
DECLARE r prod_sync.sigmasz_contacts%ROWTYPE; v_flat_json JSONB; v_sql TEXT; v_cols TEXT[]; v_set_clause TEXT; v_insert_cols TEXT; v_filtered_json JSONB;
BEGIN
    SELECT * INTO r FROM prod_sync.sigmasz_contacts WHERE contact_id = p_contact_id; IF NOT FOUND THEN RETURN; END IF;
    SELECT jsonb_object_agg('f_' || COALESCE(elem.value ->> 'field_id', elem.value ->> 'id'),
      CASE WHEN cf.type IN ('date', 'date_time', 'birthday') THEN
        CASE WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d+$' THEN to_jsonb(prod_sync.safe_cf_to_timestamp(elem.value -> 'values' -> 0 ->> 'value'))
             WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_jsonb(to_timestamp(elem.value -> 'values' -> 0 ->> 'value', 'DD.MM.YYYY')::timestamptz)
             ELSE to_jsonb(NULL::timestamptz) END
      ELSE (elem.value -> 'values' -> 0 -> 'value') END) INTO v_flat_json
    FROM jsonb_array_elements(COALESCE(r.raw_json -> 'custom_fields_values', '[]'::jsonb)) elem(value)
    LEFT JOIN airbyte_raw.sigmasz_custom_fields_contacts cf ON cf.id::text = COALESCE(elem.value ->> 'field_id', elem.value ->> 'id')
    WHERE COALESCE(elem.value ->> 'field_id', elem.value ->> 'id') IS NOT NULL;
    IF v_flat_json IS NULL THEN v_flat_json := '{}'::jsonb; END IF;
    v_flat_json := v_flat_json || jsonb_build_object('contact_id', r.contact_id, 'name', r.name, 'updated_at', r.updated_at, 'is_deleted', r.is_deleted, '_synced_at', NOW());
    SELECT array_agg(column_name::TEXT ORDER BY ordinal_position) INTO v_cols FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_contacts';
    SELECT jsonb_object_agg(key, value) INTO v_filtered_json FROM jsonb_each(v_flat_json) WHERE key = ANY(v_cols);
    SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
    SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col != 'contact_id';
    v_sql := format('INSERT INTO analytics.%I (%s) SELECT * FROM jsonb_populate_record(NULL::analytics.%I, $1) ON CONFLICT (contact_id) DO UPDATE SET %s', 'sigmasz_contacts', v_insert_cols, 'sigmasz_contacts', v_set_clause);
    EXECUTE v_sql USING COALESCE(v_filtered_json, '{}'::jsonb);
END;
$$ LANGUAGE plpgsql;

-- Функции для триггеров L2→L3 (из improved_migration_plan_fixed)
CREATE OR REPLACE FUNCTION prod_sync.propagate_sigmasz_leads_l3()
RETURNS TRIGGER AS $$
DECLARE v_flat_json JSONB; v_sql TEXT; v_cols TEXT[]; v_set_clause TEXT; v_insert_cols TEXT; v_filtered_json JSONB;
BEGIN
    SELECT jsonb_object_agg('f_' || COALESCE(elem.value ->> 'field_id', elem.value ->> 'id'),
      CASE WHEN cf.type IN ('date', 'date_time', 'birthday') THEN
        CASE WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d+$' THEN to_jsonb(prod_sync.safe_cf_to_timestamp(elem.value -> 'values' -> 0 ->> 'value'))
             WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_jsonb(to_timestamp(elem.value -> 'values' -> 0 ->> 'value', 'DD.MM.YYYY')::timestamptz)
             ELSE to_jsonb(NULL::timestamptz) END
      ELSE (elem.value -> 'values' -> 0 -> 'value') END) INTO v_flat_json
    FROM jsonb_array_elements(COALESCE(NEW.raw_json -> 'custom_fields_values', '[]'::jsonb)) elem(value)
    LEFT JOIN airbyte_raw.sigmasz_custom_fields_leads cf ON cf.id::text = COALESCE(elem.value ->> 'field_id', elem.value ->> 'id')
    WHERE COALESCE(elem.value ->> 'field_id', elem.value ->> 'id') IS NOT NULL;
    IF v_flat_json IS NULL THEN v_flat_json := '{}'::jsonb; END IF;
    v_flat_json := v_flat_json || jsonb_build_object('lead_id', NEW.lead_id, 'name', NEW.name, 'status_id', NEW.status_id, 'pipeline_id', NEW.pipeline_id, 'price', NEW.price, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, '_synced_at', NOW());
    SELECT array_agg(column_name::TEXT ORDER BY ordinal_position) INTO v_cols FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_leads';
    SELECT jsonb_object_agg(key, value) INTO v_filtered_json FROM jsonb_each(v_flat_json) WHERE key = ANY(v_cols);
    SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
    SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col != 'lead_id';
    v_sql := format('INSERT INTO analytics.%I (%s) SELECT * FROM jsonb_populate_record(NULL::analytics.%I, $1) ON CONFLICT (lead_id) DO UPDATE SET %s', 'sigmasz_leads', v_insert_cols, 'sigmasz_leads', v_set_clause);
    EXECUTE v_sql USING COALESCE(v_filtered_json, '{}'::jsonb); RETURN NEW;
EXCEPTION WHEN OTHERS THEN RAISE WARNING 'propagate_sigmasz_leads_l3 lead_id %: %', NEW.lead_id, SQLERRM; RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION prod_sync.propagate_sigmasz_contacts_l3()
RETURNS TRIGGER AS $$
DECLARE v_flat_json JSONB; v_sql TEXT; v_cols TEXT[]; v_set_clause TEXT; v_insert_cols TEXT; v_filtered_json JSONB;
BEGIN
    SELECT jsonb_object_agg('f_' || COALESCE(elem.value ->> 'field_id', elem.value ->> 'id'),
      CASE WHEN cf.type IN ('date', 'date_time', 'birthday') THEN
        CASE WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d+$' THEN to_jsonb(prod_sync.safe_cf_to_timestamp(elem.value -> 'values' -> 0 ->> 'value'))
             WHEN (elem.value -> 'values' -> 0 ->> 'value') ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_jsonb(to_timestamp(elem.value -> 'values' -> 0 ->> 'value', 'DD.MM.YYYY')::timestamptz)
             ELSE to_jsonb(NULL::timestamptz) END
      ELSE (elem.value -> 'values' -> 0 -> 'value') END) INTO v_flat_json
    FROM jsonb_array_elements(COALESCE(NEW.raw_json -> 'custom_fields_values', '[]'::jsonb)) elem(value)
    LEFT JOIN airbyte_raw.sigmasz_custom_fields_contacts cf ON cf.id::text = COALESCE(elem.value ->> 'field_id', elem.value ->> 'id')
    WHERE COALESCE(elem.value ->> 'field_id', elem.value ->> 'id') IS NOT NULL;
    IF v_flat_json IS NULL THEN v_flat_json := '{}'::jsonb; END IF;
    v_flat_json := v_flat_json || jsonb_build_object('contact_id', NEW.contact_id, 'name', NEW.name, 'updated_at', NEW.updated_at, 'is_deleted', NEW.is_deleted, '_synced_at', NOW());
    SELECT array_agg(column_name::TEXT ORDER BY ordinal_position) INTO v_cols FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_contacts';
    SELECT jsonb_object_agg(key, value) INTO v_filtered_json FROM jsonb_each(v_flat_json) WHERE key = ANY(v_cols);
    SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
    SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col != 'contact_id';
    v_sql := format('INSERT INTO analytics.%I (%s) SELECT * FROM jsonb_populate_record(NULL::analytics.%I, $1) ON CONFLICT (contact_id) DO UPDATE SET %s', 'sigmasz_contacts', v_insert_cols, 'sigmasz_contacts', v_set_clause);
    EXECUTE v_sql USING COALESCE(v_filtered_json, '{}'::jsonb); RETURN NEW;
EXCEPTION WHEN OTHERS THEN RAISE WARNING 'propagate_sigmasz_contacts_l3 contact_id %: %', NEW.contact_id, SQLERRM; RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- ШАГ 4: Создание всех триггеров
-- =============================================================================

CREATE TRIGGER trg_sigmasz_leads_l2 AFTER INSERT ON airbyte_raw.sigmasz_leads FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2();
CREATE TRIGGER trg_sigmasz_contacts_l2 AFTER INSERT ON airbyte_raw.sigmasz_contacts FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2();
CREATE TRIGGER trg_propagate_deleted_l2 AFTER INSERT ON airbyte_raw.sigmasz_events FOR EACH ROW WHEN (NEW.type IN ('lead_deleted', 'contact_deleted')) EXECUTE FUNCTION airbyte_raw.propagate_deleted_to_l2();

CREATE TRIGGER trg_l2_l3_leads_sigmasz AFTER INSERT OR UPDATE ON prod_sync.sigmasz_leads FOR EACH ROW EXECUTE FUNCTION prod_sync.propagate_sigmasz_leads_l3();
CREATE TRIGGER trg_l2_l3_contacts_sigmasz AFTER INSERT OR UPDATE ON prod_sync.sigmasz_contacts FOR EACH ROW EXECUTE FUNCTION prod_sync.propagate_sigmasz_contacts_l3();

CREATE TRIGGER trg_schema_sigmasz_leads AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_custom_fields_leads FOR EACH ROW EXECUTE FUNCTION analytics.sync_schema_sigmasz_leads();
CREATE TRIGGER trg_schema_sigmasz_contacts AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_custom_fields_contacts FOR EACH ROW EXECUTE FUNCTION analytics.sync_schema_sigmasz_contacts();

-- Пересоздать view
SELECT analytics.rebuild_view_sigmasz_leads();
SELECT analytics.rebuild_view_sigmasz_contacts();
