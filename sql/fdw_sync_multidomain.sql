-- ============================================================================
-- MULTI-DOMAIN FDW SYNC: concepta + entrum
-- Шаг 1: Импорт FDW foreign tables
-- Шаг 2: Sync функции sync_concepta_smart() и sync_entrum_smart()
-- ============================================================================
-- Запускать на PRODUCTION сервере (где живёт amo_support_schema)
-- FDW сервер airbyte_analytics_server уже должен существовать (из fdw_sync_setup.sql)
-- ============================================================================

-- ============================================================================
-- ШАГ 1: ИМПОРТ FDW FOREIGN TABLES ДЛЯ CONCEPTA И ENTRUM
-- ============================================================================

-- concepta
IMPORT FOREIGN SCHEMA "prod_sync"
LIMIT TO (
    concepta_leads,
    concepta_contacts,
    concepta_lead_contacts,
    concepta_contact_phones,
    concepta_contact_emails
)
FROM SERVER airbyte_analytics_server
INTO airbyte_remote;

-- entrum
IMPORT FOREIGN SCHEMA "prod_sync"
LIMIT TO (
    entrum_leads,
    entrum_contacts,
    entrum_lead_contacts,
    entrum_contact_phones,
    entrum_contact_emails
)
FROM SERVER airbyte_analytics_server
INTO airbyte_remote;

-- Проверка
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_server_name = 'airbyte_analytics_server'
ORDER BY foreign_table_name;


-- ============================================================================
-- ШАГ 2: SYNC FUNCTIONS
-- ============================================================================

-- ============================================================================
-- sync_concepta_smart()
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_concepta_smart()
RETURNS TABLE (entity_name TEXT, inserted BIGINT, updated BIGINT, deleted BIGINT) AS $$
DECLARE
    v_log_id        BIGINT;
    v_from_ts       TIMESTAMPTZ;
    v_last_ghost    TIMESTAMPTZ;
    v_is_ghost_bust BOOLEAN := FALSE;
    v_sync_type     TEXT;

    v_prod_leads_count BIGINT;
    v_prod_cont_count  BIGINT;

    v_arr_leads     BIGINT[];
    v_arr_contacts  BIGINT[];
    v_arr_leads_len INT;
    v_arr_cont_len  INT;

    v_leads_ins BIGINT := 0; v_leads_upd BIGINT := 0; v_leads_del BIGINT := 0;
    v_cont_ins  BIGINT := 0; v_cont_upd  BIGINT := 0; v_cont_del  BIGINT := 0;
    v_phones_ins BIGINT := 0; v_emails_ins BIGINT := 0; v_links_ins BIGINT := 0;
BEGIN
    -- 1. Защита от параллельного запуска
    IF NOT pg_try_advisory_lock(hashtext('sync_concepta_smart')) THEN
        RETURN QUERY SELECT 'WARNING: Sync already running. Skipping.'::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
        RETURN;
    END IF;

    -- 2. Вычисляем время последнего успешного инкремента (с перекрытием 15 мин)
    SELECT COALESCE(MAX(started_at) - INTERVAL '15 minutes', NOW() - INTERVAL '1 day')
    INTO v_from_ts FROM amo_support_schema.sync_log WHERE status = 'success' AND domain = 'concepta';

    -- 3. Вычисляем, пора ли делать Ghost Busting (раз в час)
    SELECT MAX(started_at) INTO v_last_ghost
    FROM amo_support_schema.sync_log WHERE status = 'success' AND sync_type = 'smart_ghost' AND domain = 'concepta';

    IF v_last_ghost IS NULL OR v_last_ghost < NOW() - INTERVAL '1 hour' THEN
        v_is_ghost_bust := TRUE;
        v_sync_type := 'smart_ghost';
    ELSE
        v_sync_type := 'smart_inc';
    END IF;

    -- Логируем старт
    INSERT INTO amo_support_schema.sync_log (domain, sync_type, status) VALUES ('concepta', v_sync_type, 'running') RETURNING id INTO v_log_id;

    -- ========================================================================
    -- ШАГ 1: GHOST BUSTING (Только раз в час)
    -- ========================================================================
    IF v_is_ghost_bust THEN
        -- Лиды
        CREATE TEMP TABLE _meta_leads ON COMMIT DROP AS SELECT lead_id FROM airbyte_remote.concepta_leads;
        CREATE INDEX ON _meta_leads(lead_id);

        -- SAFETY CHECK
        SELECT COUNT(*) INTO v_prod_leads_count FROM amo_support_schema.concepta_leads;
        IF (SELECT COUNT(*) FROM _meta_leads) = 0 AND v_prod_leads_count > 100 THEN
            RAISE EXCEPTION 'FDW returned 0 leads but PROD has % rows. Aborting to prevent data loss.', v_prod_leads_count;
        END IF;

        WITH deleted AS (
            DELETE FROM amo_support_schema.concepta_leads p
            WHERE p.updated_at < (NOW() - INTERVAL '2 hours')
              AND NOT EXISTS (SELECT 1 FROM _meta_leads r WHERE r.lead_id = p.lead_id)
            RETURNING p.lead_id
        ) SELECT COUNT(*) INTO v_leads_del FROM deleted;

        -- Контакты
        CREATE TEMP TABLE _meta_contacts ON COMMIT DROP AS SELECT contact_id FROM airbyte_remote.concepta_contacts;
        CREATE INDEX ON _meta_contacts(contact_id);

        -- SAFETY CHECK
        SELECT COUNT(*) INTO v_prod_cont_count FROM amo_support_schema.concepta_contacts;
        IF (SELECT COUNT(*) FROM _meta_contacts) = 0 AND v_prod_cont_count > 100 THEN
            RAISE EXCEPTION 'FDW returned 0 contacts but PROD has % rows. Aborting.', v_prod_cont_count;
        END IF;

        WITH deleted AS (
            DELETE FROM amo_support_schema.concepta_contacts p
            WHERE p.updated_at < (NOW() - INTERVAL '2 hours')
              AND NOT EXISTS (SELECT 1 FROM _meta_contacts r WHERE r.contact_id = p.contact_id)
            RETURNING p.contact_id
        ) SELECT COUNT(*) INTO v_cont_del FROM deleted;

        DROP TABLE _meta_leads, _meta_contacts;
    END IF;

    -- ========================================================================
    -- ШАГ 2: ИНКРЕМЕНТ
    -- ========================================================================
    CREATE TEMP TABLE _changed_leads ON COMMIT DROP AS
        SELECT lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json::TEXT AS raw_json
        FROM airbyte_remote.concepta_leads WHERE _synced_at >= v_from_ts;
    CREATE INDEX ON _changed_leads(lead_id);

    CREATE TEMP TABLE _changed_contacts ON COMMIT DROP AS
        SELECT contact_id, name, updated_at, raw_json::TEXT AS raw_json
        FROM airbyte_remote.concepta_contacts WHERE _synced_at >= v_from_ts;
    CREATE INDEX ON _changed_contacts(contact_id);

    -- Upsert Лидов
    WITH upserted AS (
        INSERT INTO amo_support_schema.concepta_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json)
        SELECT lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json::jsonb FROM _changed_leads
        ON CONFLICT (lead_id) DO UPDATE SET
            name = EXCLUDED.name, status_id = EXCLUDED.status_id, pipeline_id = EXCLUDED.pipeline_id, price = EXCLUDED.price,
            created_at = COALESCE(EXCLUDED.created_at, amo_support_schema.concepta_leads.created_at),
            updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json
        WHERE EXCLUDED.updated_at >= amo_support_schema.concepta_leads.updated_at
        RETURNING (xmax = 0) AS is_new
    ) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_leads_ins, v_leads_upd FROM upserted;

    RETURN QUERY SELECT 'concepta_leads'::TEXT, v_leads_ins, v_leads_upd, v_leads_del;

    -- Upsert Контактов
    WITH upserted AS (
        INSERT INTO amo_support_schema.concepta_contacts (contact_id, name, updated_at, raw_json)
        SELECT contact_id, name, updated_at, raw_json::jsonb FROM _changed_contacts
        ON CONFLICT (contact_id) DO UPDATE SET
            name = EXCLUDED.name, updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json
        WHERE EXCLUDED.updated_at >= amo_support_schema.concepta_contacts.updated_at
        RETURNING (xmax = 0) AS is_new
    ) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_cont_ins, v_cont_upd FROM upserted;

    RETURN QUERY SELECT 'concepta_contacts'::TEXT, v_cont_ins, v_cont_upd, v_cont_del;

    -- ========================================================================
    -- ШАГ 3: СВЯЗАННЫЕ ДАННЫЕ
    -- ========================================================================
    IF v_is_ghost_bust AND (v_leads_del > 0 OR v_cont_del > 0) THEN
        DELETE FROM amo_support_schema.concepta_contact_phones p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.concepta_contacts c WHERE c.contact_id = p.contact_id);

        DELETE FROM amo_support_schema.concepta_contact_emails p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.concepta_contacts c WHERE c.contact_id = p.contact_id);

        DELETE FROM amo_support_schema.concepta_lead_contacts p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.concepta_leads l WHERE l.lead_id = p.lead_id)
           OR NOT EXISTS (SELECT 1 FROM amo_support_schema.concepta_contacts c WHERE c.contact_id = p.contact_id);
    END IF;

    SELECT COALESCE(array_agg(lead_id), '{}'::BIGINT[]) INTO v_arr_leads FROM _changed_leads;
    SELECT COALESCE(array_agg(contact_id), '{}'::BIGINT[]) INTO v_arr_contacts FROM _changed_contacts;

    v_arr_leads_len := COALESCE(array_length(v_arr_leads, 1), 0);
    v_arr_cont_len  := COALESCE(array_length(v_arr_contacts, 1), 0);

    -- Телефоны и Emails
    IF v_arr_cont_len > 5000 THEN
        CREATE TEMP TABLE _changed_phones ON COMMIT DROP AS SELECT contact_id, phone FROM airbyte_remote.concepta_contact_phones;
        CREATE TEMP TABLE _changed_emails ON COMMIT DROP AS SELECT contact_id, email FROM airbyte_remote.concepta_contact_emails;
    ELSE
        CREATE TEMP TABLE _changed_phones ON COMMIT DROP AS SELECT contact_id, phone FROM airbyte_remote.concepta_contact_phones WHERE contact_id = ANY(v_arr_contacts);
        CREATE TEMP TABLE _changed_emails ON COMMIT DROP AS SELECT contact_id, email FROM airbyte_remote.concepta_contact_emails WHERE contact_id = ANY(v_arr_contacts);
    END IF;
    CREATE INDEX ON _changed_phones(contact_id, phone);
    CREATE INDEX ON _changed_emails(contact_id, email);

    -- Телефоны
    DELETE FROM amo_support_schema.concepta_contact_phones local
    WHERE local.contact_id = ANY(v_arr_contacts)
      AND NOT EXISTS (SELECT 1 FROM _changed_phones r WHERE r.contact_id = local.contact_id AND r.phone = local.phone);
    INSERT INTO amo_support_schema.concepta_contact_phones (contact_id, phone)
    SELECT contact_id, phone FROM _changed_phones ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_phones_ins = ROW_COUNT;
    RETURN QUERY SELECT 'concepta_contact_phones'::TEXT, v_phones_ins, 0::BIGINT, 0::BIGINT;

    -- Emails
    DELETE FROM amo_support_schema.concepta_contact_emails local
    WHERE local.contact_id = ANY(v_arr_contacts)
      AND NOT EXISTS (SELECT 1 FROM _changed_emails r WHERE r.contact_id = local.contact_id AND r.email = local.email);
    INSERT INTO amo_support_schema.concepta_contact_emails (contact_id, email)
    SELECT contact_id, email FROM _changed_emails ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_emails_ins = ROW_COUNT;
    RETURN QUERY SELECT 'concepta_contact_emails'::TEXT, v_emails_ins, 0::BIGINT, 0::BIGINT;

    -- Связи lead_contacts
    IF v_arr_leads_len > 5000 OR v_arr_cont_len > 5000 THEN
        CREATE TEMP TABLE _changed_links ON COMMIT DROP AS SELECT lead_id, contact_id FROM airbyte_remote.concepta_lead_contacts;
    ELSE
        CREATE TEMP TABLE _changed_links ON COMMIT DROP AS SELECT lead_id, contact_id FROM airbyte_remote.concepta_lead_contacts WHERE lead_id = ANY(v_arr_leads) OR contact_id = ANY(v_arr_contacts);
    END IF;
    CREATE INDEX ON _changed_links(lead_id, contact_id);

    DELETE FROM amo_support_schema.concepta_lead_contacts local
    WHERE (local.lead_id = ANY(v_arr_leads) OR local.contact_id = ANY(v_arr_contacts))
      AND NOT EXISTS (SELECT 1 FROM _changed_links r WHERE r.lead_id = local.lead_id AND r.contact_id = local.contact_id);

    INSERT INTO amo_support_schema.concepta_lead_contacts (lead_id, contact_id)
    SELECT lead_id, contact_id FROM _changed_links ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_links_ins = ROW_COUNT;
    RETURN QUERY SELECT 'concepta_lead_contacts'::TEXT, v_links_ins, 0::BIGINT, 0::BIGINT;

    -- ========================================================================
    -- ФИНАЛИЗАЦИЯ
    -- ========================================================================
    DROP TABLE IF EXISTS _changed_leads, _changed_contacts, _changed_phones, _changed_emails, _changed_links;

    UPDATE amo_support_schema.sync_log SET
        finished_at = NOW(), status = 'success',
        leads_inserted = v_leads_ins, leads_updated = v_leads_upd, leads_deleted = v_leads_del,
        contacts_ins = v_cont_ins, contacts_upd = v_cont_upd, contacts_del = v_cont_del,
        phones_ins = v_phones_ins, emails_ins = v_emails_ins, links_ins = v_links_ins
    WHERE id = v_log_id;

    PERFORM pg_advisory_unlock(hashtext('sync_concepta_smart'));

EXCEPTION WHEN OTHERS THEN
    UPDATE amo_support_schema.sync_log SET finished_at = NOW(), status = 'error', error_message = SQLERRM WHERE id = v_log_id;
    PERFORM pg_advisory_unlock(hashtext('sync_concepta_smart'));
    RETURN QUERY SELECT ('ERROR: ' || SQLERRM)::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
    RETURN;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- sync_entrum_smart()
-- ============================================================================
CREATE OR REPLACE FUNCTION amo_support_schema.sync_entrum_smart()
RETURNS TABLE (entity_name TEXT, inserted BIGINT, updated BIGINT, deleted BIGINT) AS $$
DECLARE
    v_log_id        BIGINT;
    v_from_ts       TIMESTAMPTZ;
    v_last_ghost    TIMESTAMPTZ;
    v_is_ghost_bust BOOLEAN := FALSE;
    v_sync_type     TEXT;

    v_prod_leads_count BIGINT;
    v_prod_cont_count  BIGINT;

    v_arr_leads     BIGINT[];
    v_arr_contacts  BIGINT[];
    v_arr_leads_len INT;
    v_arr_cont_len  INT;

    v_leads_ins BIGINT := 0; v_leads_upd BIGINT := 0; v_leads_del BIGINT := 0;
    v_cont_ins  BIGINT := 0; v_cont_upd  BIGINT := 0; v_cont_del  BIGINT := 0;
    v_phones_ins BIGINT := 0; v_emails_ins BIGINT := 0; v_links_ins BIGINT := 0;
BEGIN
    -- 1. Защита от параллельного запуска
    IF NOT pg_try_advisory_lock(hashtext('sync_entrum_smart')) THEN
        RETURN QUERY SELECT 'WARNING: Sync already running. Skipping.'::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
        RETURN;
    END IF;

    -- 2. Вычисляем время последнего успешного инкремента (с перекрытием 15 мин)
    SELECT COALESCE(MAX(started_at) - INTERVAL '15 minutes', NOW() - INTERVAL '1 day')
    INTO v_from_ts FROM amo_support_schema.sync_log WHERE status = 'success' AND domain = 'entrum';

    -- 3. Вычисляем, пора ли делать Ghost Busting (раз в час)
    SELECT MAX(started_at) INTO v_last_ghost
    FROM amo_support_schema.sync_log WHERE status = 'success' AND sync_type = 'smart_ghost' AND domain = 'entrum';

    IF v_last_ghost IS NULL OR v_last_ghost < NOW() - INTERVAL '1 hour' THEN
        v_is_ghost_bust := TRUE;
        v_sync_type := 'smart_ghost';
    ELSE
        v_sync_type := 'smart_inc';
    END IF;

    -- Логируем старт
    INSERT INTO amo_support_schema.sync_log (domain, sync_type, status) VALUES ('entrum', v_sync_type, 'running') RETURNING id INTO v_log_id;

    -- ========================================================================
    -- ШАГ 1: GHOST BUSTING (Только раз в час)
    -- ========================================================================
    IF v_is_ghost_bust THEN
        -- Лиды
        CREATE TEMP TABLE _meta_leads ON COMMIT DROP AS SELECT lead_id FROM airbyte_remote.entrum_leads;
        CREATE INDEX ON _meta_leads(lead_id);

        -- SAFETY CHECK
        SELECT COUNT(*) INTO v_prod_leads_count FROM amo_support_schema.entrum_leads;
        IF (SELECT COUNT(*) FROM _meta_leads) = 0 AND v_prod_leads_count > 100 THEN
            RAISE EXCEPTION 'FDW returned 0 leads but PROD has % rows. Aborting to prevent data loss.', v_prod_leads_count;
        END IF;

        WITH deleted AS (
            DELETE FROM amo_support_schema.entrum_leads p
            WHERE p.updated_at < (NOW() - INTERVAL '2 hours')
              AND NOT EXISTS (SELECT 1 FROM _meta_leads r WHERE r.lead_id = p.lead_id)
            RETURNING p.lead_id
        ) SELECT COUNT(*) INTO v_leads_del FROM deleted;

        -- Контакты
        CREATE TEMP TABLE _meta_contacts ON COMMIT DROP AS SELECT contact_id FROM airbyte_remote.entrum_contacts;
        CREATE INDEX ON _meta_contacts(contact_id);

        -- SAFETY CHECK
        SELECT COUNT(*) INTO v_prod_cont_count FROM amo_support_schema.entrum_contacts;
        IF (SELECT COUNT(*) FROM _meta_contacts) = 0 AND v_prod_cont_count > 100 THEN
            RAISE EXCEPTION 'FDW returned 0 contacts but PROD has % rows. Aborting.', v_prod_cont_count;
        END IF;

        WITH deleted AS (
            DELETE FROM amo_support_schema.entrum_contacts p
            WHERE p.updated_at < (NOW() - INTERVAL '2 hours')
              AND NOT EXISTS (SELECT 1 FROM _meta_contacts r WHERE r.contact_id = p.contact_id)
            RETURNING p.contact_id
        ) SELECT COUNT(*) INTO v_cont_del FROM deleted;

        DROP TABLE _meta_leads, _meta_contacts;
    END IF;

    -- ========================================================================
    -- ШАГ 2: ИНКРЕМЕНТ
    -- ========================================================================
    CREATE TEMP TABLE _changed_leads ON COMMIT DROP AS
        SELECT lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json::TEXT AS raw_json
        FROM airbyte_remote.entrum_leads WHERE _synced_at >= v_from_ts;
    CREATE INDEX ON _changed_leads(lead_id);

    CREATE TEMP TABLE _changed_contacts ON COMMIT DROP AS
        SELECT contact_id, name, updated_at, raw_json::TEXT AS raw_json
        FROM airbyte_remote.entrum_contacts WHERE _synced_at >= v_from_ts;
    CREATE INDEX ON _changed_contacts(contact_id);

    -- Upsert Лидов
    WITH upserted AS (
        INSERT INTO amo_support_schema.entrum_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json)
        SELECT lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json::jsonb FROM _changed_leads
        ON CONFLICT (lead_id) DO UPDATE SET
            name = EXCLUDED.name, status_id = EXCLUDED.status_id, pipeline_id = EXCLUDED.pipeline_id, price = EXCLUDED.price,
            created_at = COALESCE(EXCLUDED.created_at, amo_support_schema.entrum_leads.created_at),
            updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json
        WHERE EXCLUDED.updated_at >= amo_support_schema.entrum_leads.updated_at
        RETURNING (xmax = 0) AS is_new
    ) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_leads_ins, v_leads_upd FROM upserted;

    RETURN QUERY SELECT 'entrum_leads'::TEXT, v_leads_ins, v_leads_upd, v_leads_del;

    -- Upsert Контактов
    WITH upserted AS (
        INSERT INTO amo_support_schema.entrum_contacts (contact_id, name, updated_at, raw_json)
        SELECT contact_id, name, updated_at, raw_json::jsonb FROM _changed_contacts
        ON CONFLICT (contact_id) DO UPDATE SET
            name = EXCLUDED.name, updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json
        WHERE EXCLUDED.updated_at >= amo_support_schema.entrum_contacts.updated_at
        RETURNING (xmax = 0) AS is_new
    ) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_cont_ins, v_cont_upd FROM upserted;

    RETURN QUERY SELECT 'entrum_contacts'::TEXT, v_cont_ins, v_cont_upd, v_cont_del;

    -- ========================================================================
    -- ШАГ 3: СВЯЗАННЫЕ ДАННЫЕ
    -- ========================================================================
    IF v_is_ghost_bust AND (v_leads_del > 0 OR v_cont_del > 0) THEN
        DELETE FROM amo_support_schema.entrum_contact_phones p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.entrum_contacts c WHERE c.contact_id = p.contact_id);

        DELETE FROM amo_support_schema.entrum_contact_emails p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.entrum_contacts c WHERE c.contact_id = p.contact_id);

        DELETE FROM amo_support_schema.entrum_lead_contacts p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.entrum_leads l WHERE l.lead_id = p.lead_id)
           OR NOT EXISTS (SELECT 1 FROM amo_support_schema.entrum_contacts c WHERE c.contact_id = p.contact_id);
    END IF;

    SELECT COALESCE(array_agg(lead_id), '{}'::BIGINT[]) INTO v_arr_leads FROM _changed_leads;
    SELECT COALESCE(array_agg(contact_id), '{}'::BIGINT[]) INTO v_arr_contacts FROM _changed_contacts;

    v_arr_leads_len := COALESCE(array_length(v_arr_leads, 1), 0);
    v_arr_cont_len  := COALESCE(array_length(v_arr_contacts, 1), 0);

    -- Телефоны и Emails
    IF v_arr_cont_len > 5000 THEN
        CREATE TEMP TABLE _changed_phones ON COMMIT DROP AS SELECT contact_id, phone FROM airbyte_remote.entrum_contact_phones;
        CREATE TEMP TABLE _changed_emails ON COMMIT DROP AS SELECT contact_id, email FROM airbyte_remote.entrum_contact_emails;
    ELSE
        CREATE TEMP TABLE _changed_phones ON COMMIT DROP AS SELECT contact_id, phone FROM airbyte_remote.entrum_contact_phones WHERE contact_id = ANY(v_arr_contacts);
        CREATE TEMP TABLE _changed_emails ON COMMIT DROP AS SELECT contact_id, email FROM airbyte_remote.entrum_contact_emails WHERE contact_id = ANY(v_arr_contacts);
    END IF;
    CREATE INDEX ON _changed_phones(contact_id, phone);
    CREATE INDEX ON _changed_emails(contact_id, email);

    -- Телефоны
    DELETE FROM amo_support_schema.entrum_contact_phones local
    WHERE local.contact_id = ANY(v_arr_contacts)
      AND NOT EXISTS (SELECT 1 FROM _changed_phones r WHERE r.contact_id = local.contact_id AND r.phone = local.phone);
    INSERT INTO amo_support_schema.entrum_contact_phones (contact_id, phone)
    SELECT contact_id, phone FROM _changed_phones ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_phones_ins = ROW_COUNT;
    RETURN QUERY SELECT 'entrum_contact_phones'::TEXT, v_phones_ins, 0::BIGINT, 0::BIGINT;

    -- Emails
    DELETE FROM amo_support_schema.entrum_contact_emails local
    WHERE local.contact_id = ANY(v_arr_contacts)
      AND NOT EXISTS (SELECT 1 FROM _changed_emails r WHERE r.contact_id = local.contact_id AND r.email = local.email);
    INSERT INTO amo_support_schema.entrum_contact_emails (contact_id, email)
    SELECT contact_id, email FROM _changed_emails ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_emails_ins = ROW_COUNT;
    RETURN QUERY SELECT 'entrum_contact_emails'::TEXT, v_emails_ins, 0::BIGINT, 0::BIGINT;

    -- Связи lead_contacts
    IF v_arr_leads_len > 5000 OR v_arr_cont_len > 5000 THEN
        CREATE TEMP TABLE _changed_links ON COMMIT DROP AS SELECT lead_id, contact_id FROM airbyte_remote.entrum_lead_contacts;
    ELSE
        CREATE TEMP TABLE _changed_links ON COMMIT DROP AS SELECT lead_id, contact_id FROM airbyte_remote.entrum_lead_contacts WHERE lead_id = ANY(v_arr_leads) OR contact_id = ANY(v_arr_contacts);
    END IF;
    CREATE INDEX ON _changed_links(lead_id, contact_id);

    DELETE FROM amo_support_schema.entrum_lead_contacts local
    WHERE (local.lead_id = ANY(v_arr_leads) OR local.contact_id = ANY(v_arr_contacts))
      AND NOT EXISTS (SELECT 1 FROM _changed_links r WHERE r.lead_id = local.lead_id AND r.contact_id = local.contact_id);

    INSERT INTO amo_support_schema.entrum_lead_contacts (lead_id, contact_id)
    SELECT lead_id, contact_id FROM _changed_links ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_links_ins = ROW_COUNT;
    RETURN QUERY SELECT 'entrum_lead_contacts'::TEXT, v_links_ins, 0::BIGINT, 0::BIGINT;

    -- ========================================================================
    -- ФИНАЛИЗАЦИЯ
    -- ========================================================================
    DROP TABLE IF EXISTS _changed_leads, _changed_contacts, _changed_phones, _changed_emails, _changed_links;

    UPDATE amo_support_schema.sync_log SET
        finished_at = NOW(), status = 'success',
        leads_inserted = v_leads_ins, leads_updated = v_leads_upd, leads_deleted = v_leads_del,
        contacts_ins = v_cont_ins, contacts_upd = v_cont_upd, contacts_del = v_cont_del,
        phones_ins = v_phones_ins, emails_ins = v_emails_ins, links_ins = v_links_ins
    WHERE id = v_log_id;

    PERFORM pg_advisory_unlock(hashtext('sync_entrum_smart'));

EXCEPTION WHEN OTHERS THEN
    UPDATE amo_support_schema.sync_log SET finished_at = NOW(), status = 'error', error_message = SQLERRM WHERE id = v_log_id;
    PERFORM pg_advisory_unlock(hashtext('sync_entrum_smart'));
    RETURN QUERY SELECT ('ERROR: ' || SQLERRM)::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
    RETURN;
END;
$$ LANGUAGE plpgsql;
