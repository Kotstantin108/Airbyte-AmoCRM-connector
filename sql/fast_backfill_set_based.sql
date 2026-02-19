-- ============================================
-- СКРИПТ 2 (ОПТИМИЗИРОВАННЫЙ): ТУРБО-BACKFILL
-- ============================================
-- Запускайте этот скрипт ВМЕСТО предыдущего loop-варианта.
-- Он использует массовые SQL-запросы (INSERT ... SELECT) и индексы.
-- ============================================

-- ШАГ 0: Создаем временные индексы для ускорения (ВКЛЮЧАЯ airbyte_raw)
-- -----------------------------------------------------------------------------
-- Индекс на события (удаления) — критически важен, иначе тормозит в 100 раз
CREATE INDEX IF NOT EXISTS idx_airbyte_events_type_entity ON airbyte_raw.sigmasz_events(type, entity_id);
-- Индексы на исходные таблицы для ускорения выборок
CREATE INDEX IF NOT EXISTS idx_airbyte_contacts_id ON airbyte_raw.sigmasz_contacts(id);
CREATE INDEX IF NOT EXISTS idx_airbyte_leads_id ON airbyte_raw.sigmasz_leads(id);

-- Выключаем триггеры на время массовой вставки (для скорости)
DROP TRIGGER IF EXISTS trg_l2_l3_leads_sigmasz ON prod_sync.sigmasz_leads;
DROP TRIGGER IF EXISTS trg_l2_l3_contacts_sigmasz ON prod_sync.sigmasz_contacts;

-- -----------------------------------------------------------------------------
-- ШАГ 1: МИГРАЦИЯ СХЕМЫ L3 (Analytics)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics.sigmasz_contacts CASCADE;
DROP VIEW IF EXISTS analytics.sigmasz_contacts_human CASCADE;

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
-- ШАГ 3 (БЫСТРЫЙ): ЗАГРУЗКА ДАННЫХ В L2 (Контакты)
-- -----------------------------------------------------------------------------
-- Очищаем таблицы перед полной загрузкой (чтобы не было конфликтов и дублей)
TRUNCATE prod_sync.sigmasz_contacts CASCADE;

-- DISTINCT ON берет только одну запись на каждый contact_id (самую свежую)
INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at)
SELECT DISTINCT ON (c.id)
    c.id,
    c.name,
    CASE 
        WHEN c.updated_at::TEXT ~ '^\d+$'
             THEN to_timestamp(c.updated_at::TEXT::double precision)
        ELSE c.updated_at::TEXT::TIMESTAMPTZ 
    END,
    jsonb_build_object(
        'id', c.id, 
        'name', c.name, 
        'updated_at', c.updated_at, 
        'is_deleted', c.is_deleted, 
        'custom_fields_values', COALESCE(c.custom_fields_values::TEXT::JSONB, '[]'::jsonb)
    ),
    FALSE,
    NOW()
FROM airbyte_raw.sigmasz_contacts c
WHERE c.id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM airbyte_raw.sigmasz_events e 
      WHERE e.type = 'contact_deleted' AND e.entity_id = c.id
  )
ORDER BY c.id, c.updated_at::TEXT DESC;

-- Телефоны
INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone)
SELECT DISTINCT 
    c.id, 
    normalize_phone(cv.val ->> 'value')
FROM (
    SELECT DISTINCT ON (id) id, custom_fields_values
    FROM airbyte_raw.sigmasz_contacts
    WHERE id IS NOT NULL
    ORDER BY id, updated_at::TEXT DESC
) c
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.custom_fields_values::TEXT::JSONB, '[]'::jsonb)) AS ccf(fld)
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ccf.fld -> 'values', '[]'::jsonb)) AS cv(val)
WHERE ccf.fld ->> 'field_code' = 'PHONE' 
  AND normalize_phone(cv.val ->> 'value') IS NOT NULL 
  AND length(normalize_phone(cv.val ->> 'value')) >= 10
  AND NOT EXISTS (
      SELECT 1 FROM airbyte_raw.sigmasz_events e 
      WHERE e.type = 'contact_deleted' AND e.entity_id = c.id
  )
ON CONFLICT DO NOTHING;

-- Email
INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email)
SELECT DISTINCT 
    c.id, 
    lower(cv.val ->> 'value')
FROM (
    SELECT DISTINCT ON (id) id, custom_fields_values
    FROM airbyte_raw.sigmasz_contacts
    WHERE id IS NOT NULL
    ORDER BY id, updated_at::TEXT DESC
) c
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.custom_fields_values::TEXT::JSONB, '[]'::jsonb)) AS ccf(fld) 
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ccf.fld -> 'values', '[]'::jsonb)) AS cv(val)
WHERE ccf.fld ->> 'field_code' = 'EMAIL' 
  AND cv.val ->> 'value' IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM airbyte_raw.sigmasz_events e 
      WHERE e.type = 'contact_deleted' AND e.entity_id = c.id
  )
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- ШАГ 4 (БЫСТРЫЙ): ЗАГРУЗКА ДАННЫХ В L2 (Сделки)
-- -----------------------------------------------------------------------------
TRUNCATE prod_sync.sigmasz_leads CASCADE;

-- DISTINCT ON (id) берет только последнюю версию каждой сделки (по updated_at)
INSERT INTO prod_sync.sigmasz_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json, is_deleted, _synced_at) 
SELECT DISTINCT ON (l.id)
    l.id,
    l.name,
    l.status_id,
    l.pipeline_id,
    l.price,
    CASE 
        WHEN l.created_at::TEXT ~ '^\d+$'
             THEN to_timestamp(l.created_at::TEXT::double precision)
        ELSE l.created_at::TEXT::TIMESTAMPTZ 
    END,
    CASE 
        WHEN l.updated_at::TEXT ~ '^\d+$'
             THEN to_timestamp(l.updated_at::TEXT::double precision)
        ELSE l.updated_at::TEXT::TIMESTAMPTZ 
    END,
    jsonb_build_object(
        'id', l.id, 
        'name', l.name, 
        'status_id', l.status_id, 
        'pipeline_id', l.pipeline_id, 
        'price', l.price, 
        'created_at', l.created_at, 
        'updated_at', l.updated_at, 
        'is_deleted', l.is_deleted, 
        '_embedded', COALESCE(l._embedded::TEXT::JSONB, '{}'::jsonb), 
        'custom_fields_values', COALESCE(l.custom_fields_values::TEXT::JSONB, '[]'::jsonb)
    ),
    FALSE,
    NOW()
FROM airbyte_raw.sigmasz_leads l
WHERE l.id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM airbyte_raw.sigmasz_events e 
      WHERE e.type = 'lead_deleted' AND e.entity_id = l.id
  )
ORDER BY l.id, l.updated_at::TEXT DESC;

-- Связи Lead-Contact (через распаковку _embedded)
-- JOIN с prod_sync.sigmasz_contacts гарантирует соблюдение FK (берем только известные контакты)
INSERT INTO prod_sync.sigmasz_lead_contacts (lead_id, contact_id)
SELECT DISTINCT
    l.id,
    contact_id_val
FROM (
    SELECT DISTINCT ON (l.id) l.id, l._embedded
    FROM airbyte_raw.sigmasz_leads l
    WHERE l.id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM airbyte_raw.sigmasz_events e 
          WHERE e.type = 'lead_deleted' AND e.entity_id = l.id
      )
    ORDER BY l.id, l.updated_at::TEXT DESC
) l
CROSS JOIN LATERAL (
    SELECT (c_elem.value ->> 'id')::BIGINT AS contact_id_val
    FROM jsonb_array_elements(COALESCE(l._embedded::TEXT::JSONB, '{}'::jsonb) -> 'contacts') AS c_elem
    WHERE (c_elem.value ->> 'id') IS NOT NULL
) ce
-- Только те контакты, которые реально есть в prod_sync (соблюдаем FK)
WHERE EXISTS (
    SELECT 1 FROM prod_sync.sigmasz_contacts sc WHERE sc.contact_id = ce.contact_id_val
)
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- ШАГ 5: ВЫГРУЗКА В АНАЛИТИКУ (L3)
-- -----------------------------------------------------------------------------
-- Здесь, к сожалению, нужна `propagate` loop или set-based JSONB магия.
-- Так как логика в `propagate_one_lead_to_l3` сложная (динамические поля + join с meta), 
-- оставим пока цикл, НО он пойдет по УЖЕ заполненным L2 таблицам, что быстрее.
-- Можно ускорить, запустив в несколько потоков, но пока Loop.

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
