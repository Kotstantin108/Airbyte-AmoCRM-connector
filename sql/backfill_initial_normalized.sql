-- ============================================
-- СКРИПТ 1: МИГРАЦИЯ + BACKFILL
-- ============================================
-- Запускайте этот скрипт частями (выделяя блоки) или целиком (Alt+X).
-- Он разбит на логические шаги.
-- ============================================

-- ШАГ 0: Выключаем триггеры на время массовой вставки (для скорости)
-- -----------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_l2_l3_leads_sigmasz ON prod_sync.sigmasz_leads;
DROP TRIGGER IF EXISTS trg_l2_l3_contacts_sigmasz ON prod_sync.sigmasz_contacts;

-- -----------------------------------------------------------------------------
-- ШАГ 1: МИГРАЦИЯ СХЕМЫ L3 (Analytics)
-- -----------------------------------------------------------------------------

-- Добавляем колонки контакта в таблицу Сделок
ALTER TABLE analytics.sigmasz_leads ADD COLUMN IF NOT EXISTS contact_id BIGINT;
ALTER TABLE analytics.sigmasz_leads ADD COLUMN IF NOT EXISTS contact_name TEXT;
ALTER TABLE analytics.sigmasz_leads ADD COLUMN IF NOT EXISTS contact_phone TEXT;
ALTER TABLE analytics.sigmasz_leads ADD COLUMN IF NOT EXISTS contact_email TEXT;

-- -----------------------------------------------------------------------------
-- ШАГ 2: ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
-- -----------------------------------------------------------------------------
-- Функция поиска "лучшего" контакта для сделки
CREATE OR REPLACE FUNCTION prod_sync.get_best_contact_for_lead(p_lead_id BIGINT)
RETURNS TABLE (c_id BIGINT, c_name TEXT, c_phone TEXT, c_email TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.contact_id,
        c.name,
        (SELECT phone FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id LIMIT 1),
        (SELECT email FROM prod_sync.sigmasz_contact_emails ce WHERE ce.contact_id = c.contact_id LIMIT 1)
    FROM prod_sync.sigmasz_lead_contacts lc
    JOIN prod_sync.sigmasz_contacts c ON c.contact_id = lc.contact_id
    WHERE lc.lead_id = p_lead_id
    ORDER BY 
        COALESCE((c.raw_json ->> 'is_main')::BOOLEAN, FALSE) DESC, -- Сначала is_main=true
        c.contact_id ASC -- Потом просто самый старый
    LIMIT 1;
END;
$$ LANGUAGE plpgsql STABLE;

-- Обновленная функция "проталкивания" одной сделки в L3 (с контактами)
CREATE OR REPLACE FUNCTION prod_sync.propagate_one_lead_to_l3(p_lead_id BIGINT)
RETURNS VOID AS $$
DECLARE 
    r prod_sync.sigmasz_leads%ROWTYPE; 
    v_flat_json JSONB; 
    v_sql TEXT; v_cols TEXT[]; v_set_clause TEXT; v_insert_cols TEXT; v_filtered_json JSONB;
    v_c_id BIGINT; v_c_name TEXT; v_c_phone TEXT; v_c_email TEXT;
BEGIN
    SELECT * INTO r FROM prod_sync.sigmasz_leads WHERE lead_id = p_lead_id; 
    IF NOT FOUND THEN RETURN; END IF;

    -- Ищем контакт
    SELECT c_id, c_name, c_phone, c_email INTO v_c_id, v_c_name, v_c_phone, v_c_email 
    FROM prod_sync.get_best_contact_for_lead(p_lead_id);

    -- Распаковываем поля
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

    -- Собираем итоговый JSON для вставки
    v_flat_json := v_flat_json || jsonb_build_object(
        'lead_id', r.lead_id, 
        'name', r.name, 
        'status_id', r.status_id, 
        'pipeline_id', r.pipeline_id, 
        'price', r.price, 
        'created_at', r.created_at, 
        'updated_at', r.updated_at, 
        'is_deleted', r.is_deleted, 
        '_synced_at', NOW(),
        -- Новые поля
        'contact_id', v_c_id,
        'contact_name', v_c_name,
        'contact_phone', v_c_phone,
        'contact_email', v_c_email
    );

    -- Dynamic Insert
    SELECT array_agg(column_name::TEXT ORDER BY ordinal_position) INTO v_cols FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'sigmasz_leads';
    SELECT jsonb_object_agg(key, value) INTO v_filtered_json FROM jsonb_each(v_flat_json) WHERE key = ANY(v_cols);
    SELECT string_agg(format('%I', col), ', ') INTO v_insert_cols FROM unnest(v_cols) col;
    SELECT string_agg(format('%I = EXCLUDED.%I', col, col), ', ') INTO v_set_clause FROM unnest(v_cols) col WHERE col != 'lead_id';

    v_sql := format('INSERT INTO analytics.%I (%s) SELECT * FROM jsonb_populate_record(NULL::analytics.%I, $1) ON CONFLICT (lead_id) DO UPDATE SET %s', 'sigmasz_leads', v_insert_cols, 'sigmasz_leads', v_set_clause);
    EXECUTE v_sql USING COALESCE(v_filtered_json, '{}'::jsonb);
END;
$$ LANGUAGE plpgsql;

-- Триггерная функция (аналог propagate_one_lead_to_l3, но для триггера)
CREATE OR REPLACE FUNCTION prod_sync.propagate_sigmasz_leads_l3()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM prod_sync.propagate_one_lead_to_l3(NEW.lead_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- ШАГ 3: ЗАГРУЗКА ДАННЫХ В L2 (Контакты)
-- -----------------------------------------------------------------------------
DO $$
DECLARE 
    r RECORD; 
    v_contact_id BIGINT; v_updated_ts TIMESTAMPTZ; v_raw_json JSONB; v_phones JSONB; v_phone TEXT; v_email TEXT;
    v_counter INT := 0;
BEGIN
    RAISE NOTICE 'Starting L1->L2 Contacts sync...';
    FOR r IN SELECT id, name, updated_at, is_deleted, custom_fields_values FROM airbyte_raw.sigmasz_contacts WHERE id IS NOT NULL LOOP
        v_counter := v_counter + 1;
        IF v_counter % 10000 = 0 THEN RAISE NOTICE 'Processed % contacts...', v_counter; END IF;

        v_contact_id := r.id;
        
        -- Проверка на удаление (через events)
        IF EXISTS (SELECT 1 FROM airbyte_raw.sigmasz_events WHERE type = 'contact_deleted' AND entity_id = v_contact_id) THEN
             CONTINUE; -- Пропускаем удаленные
        END IF;

        BEGIN IF pg_typeof(r.updated_at) = 'bigint'::regtype OR pg_typeof(r.updated_at) = 'integer'::regtype THEN v_updated_ts := to_timestamp(r.updated_at::BIGINT); ELSE v_updated_ts := r.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_updated_ts := NULL; END;
        v_raw_json := jsonb_build_object('id', r.id, 'name', r.name, 'updated_at', r.updated_at, 'is_deleted', r.is_deleted, 'custom_fields_values', COALESCE(r.custom_fields_values::TEXT::JSONB, '[]'::jsonb));
        
        INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at) VALUES (v_contact_id, r.name, v_updated_ts, v_raw_json, FALSE, NOW()) ON CONFLICT (contact_id) DO UPDATE SET name=EXCLUDED.name, updated_at=EXCLUDED.updated_at, raw_json=EXCLUDED.raw_json;
        
        -- Телефоны/Email
        DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_contact_id;
        DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_contact_id;
        v_phones := COALESCE(r.custom_fields_values::TEXT::JSONB, '[]'::jsonb);
        IF jsonb_typeof(v_phones) = 'array' THEN 
            FOR v_phone IN SELECT DISTINCT normalize_phone(v.value ->> 'value') FROM jsonb_array_elements(v_phones) AS cf CROSS JOIN LATERAL jsonb_array_elements(COALESCE(cf -> 'values', '[]'::jsonb)) AS v WHERE cf ->> 'field_code' = 'PHONE' AND normalize_phone(v.value ->> 'value') IS NOT NULL AND length(normalize_phone(v.value ->> 'value')) >= 10 LOOP 
                INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone) VALUES (v_contact_id, v_phone) ON CONFLICT DO NOTHING; 
            END LOOP;
            FOR v_email IN SELECT DISTINCT lower(v.value ->> 'value') FROM jsonb_array_elements(v_phones) AS cf CROSS JOIN LATERAL jsonb_array_elements(COALESCE(cf -> 'values', '[]'::jsonb)) AS v WHERE cf ->> 'field_code' = 'EMAIL' AND v.value ->> 'value' IS NOT NULL LOOP 
                INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email) VALUES (v_contact_id, v_email) ON CONFLICT DO NOTHING; 
            END LOOP;
        END IF;
    END LOOP;
    RAISE NOTICE 'DONE Contacts L2 sync. Total: %', v_counter;
END;
$$;

-- -----------------------------------------------------------------------------
-- ШАГ 4: ЗАГРУЗКА ДАННЫХ В L2 (Сделки)
-- -----------------------------------------------------------------------------
DO $$
DECLARE 
    r RECORD; 
    v_lead_id BIGINT; v_created_ts TIMESTAMPTZ; v_updated_ts TIMESTAMPTZ; v_raw_json JSONB; v_embedded_contacts JSONB;
    v_counter INT := 0;
BEGIN
    RAISE NOTICE 'Starting L1->L2 Leads sync...';
    FOR r IN SELECT id, name, status_id, pipeline_id, price, created_at, updated_at, is_deleted, _embedded, custom_fields_values FROM airbyte_raw.sigmasz_leads WHERE id IS NOT NULL LOOP
        v_counter := v_counter + 1;
        IF v_counter % 10000 = 0 THEN RAISE NOTICE 'Processed % leads...', v_counter; END IF;

        v_lead_id := r.id;
        
        -- Проверка на удаление
        IF EXISTS (SELECT 1 FROM airbyte_raw.sigmasz_events WHERE type = 'lead_deleted' AND entity_id = v_lead_id) THEN
             CONTINUE; 
        END IF;

        BEGIN IF pg_typeof(r.created_at) = 'bigint'::regtype OR pg_typeof(r.created_at) = 'integer'::regtype THEN v_created_ts := to_timestamp(r.created_at::BIGINT); ELSE v_created_ts := r.created_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_created_ts := NULL; END;
        BEGIN IF pg_typeof(r.updated_at) = 'bigint'::regtype OR pg_typeof(r.updated_at) = 'integer'::regtype THEN v_updated_ts := to_timestamp(r.updated_at::BIGINT); ELSE v_updated_ts := r.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_updated_ts := NULL; END;

        v_raw_json := jsonb_build_object('id', r.id, 'name', r.name, 'status_id', r.status_id, 'pipeline_id', r.pipeline_id, 'price', r.price, 'created_at', r.created_at, 'updated_at', r.updated_at, 'is_deleted', r.is_deleted, '_embedded', COALESCE(r._embedded::TEXT::JSONB, '{}'::jsonb), 'custom_fields_values', COALESCE(r.custom_fields_values::TEXT::JSONB, '[]'::jsonb));
        
        INSERT INTO prod_sync.sigmasz_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json, is_deleted, _synced_at) 
        VALUES (v_lead_id, r.name, r.status_id, r.pipeline_id, r.price, v_created_ts, v_updated_ts, v_raw_json, FALSE, NOW()) 
        ON CONFLICT (lead_id) DO UPDATE SET name=EXCLUDED.name, status_id=EXCLUDED.status_id, pipeline_id=EXCLUDED.pipeline_id, price=EXCLUDED.price, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at, raw_json=EXCLUDED.raw_json;

        -- Связи Lead-Contact
        v_embedded_contacts := COALESCE(r._embedded::TEXT::JSONB, '{}'::jsonb) -> 'contacts';
        IF v_embedded_contacts IS NOT NULL AND jsonb_typeof(v_embedded_contacts) = 'array' THEN
            PERFORM prod_sync.process_embedded_contacts(v_embedded_contacts, v_lead_id);
        END IF;
    END LOOP;
    RAISE NOTICE 'DONE Leads L2 sync. Total: %', v_counter;
END;
$$;

-- -----------------------------------------------------------------------------
-- ШАГ 5: ВЫГРУЗКА В АНАЛИТИКУ (L3)
-- -----------------------------------------------------------------------------
DO $$
DECLARE 
    r RECORD;
    v_counter INT := 0;
BEGIN
    RAISE NOTICE 'Starting L2->L3 Leads (with contacts) sync...';
    FOR r IN SELECT lead_id FROM prod_sync.sigmasz_leads LOOP
        v_counter := v_counter + 1;
        IF v_counter % 5000 = 0 THEN RAISE NOTICE 'Propagated % to L3...', v_counter; END IF;
        
        PERFORM prod_sync.propagate_one_lead_to_l3(r.lead_id);
    END LOOP;
    RAISE NOTICE 'DONE L3 sync. Total: %', v_counter;
END;
$$;
