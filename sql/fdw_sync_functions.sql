-- ============================================================================
-- ЕДИНАЯ УМНАЯ СИНХРОНИЗАЦИЯ: Analytics (L2) -> PROD (amo_support_schema)
-- Домен: sigmasz | Режим: Гибридный (Ghost Busting + Инкремент)
-- ============================================================================
--
-- АРХИТЕКТУРА:
--   L2 (prod_sync / airbyte_remote через FDW) → PROD (amo_support_schema)
--
-- ИНВАРИАНТ L2:
--   L2 НИКОГДА не содержит записей с is_deleted=TRUE.
--   Триггеры L1→L2 (unpack_sigmasz_leads_l2, unpack_sigmasz_contacts_l2,
--   propagate_deleted_to_l2) физически УДАЛЯЮТ записи из L2 при удалении.
--   Если запись есть в L2, она гарантированно живая.
--
-- КЛЮЧЕВОЕ ПРАВИЛО:
--   PROD-таблицы обновляются вебхуком AmoCRM в реальном времени.
--   L2 отстаёт на ~10 мин (цикл Airbyte).
--   Обновляем запись в PROD из L2 ТОЛЬКО если L2.updated_at >= PROD.updated_at.
--
-- СТРАТЕГИЯ СИНХРОНИЗАЦИИ:
--   1. Ghost Busting (раз в час): скачиваем только ID из L2, находим
--      записи в PROD которых нет в L2 (удалены триггерами) и чистим.
--      Защита: не трогаем записи моложе 2 часов (могли прийти из вебхука).
--      Safety check: abort если FDW вернул 0 записей а PROD не пуст.
--   2. Инкремент (каждые 5-10 мин): скачиваем полные данные только для
--      записей с _synced_at >= (последний_успешный_запуск - 15 мин).
--      Все данные материализуются в temp-таблицы для минимизации FDW трафика.
--   3. FDW Pushdown: связанные данные (phones, emails, links) скачиваются
--      через = ANY(array). При > 5000 элементов — fallback на full scan.
--
-- ВЫЗОВ ИЗ n8n:
--   SELECT * FROM amo_support_schema.sync_sigmasz_smart();
--   (запускать каждые 5-10 минут, ghost busting сам решает когда нужен)
--
-- ВОЗМОЖНЫЕ РИСКИ И РЕШЕНИЯ:
--   1. Webhook fresher than L2      → updated_at comparison
--   2. Пропуск записей при сбое     → overlap 15 мин
--   3. FDW сбой / пустой ответ      → safety check + abort
--   4. Параллельный запуск           → pg_advisory_lock
--   5. Потеря лога ошибки при RAISE  → RETURN QUERY с маркером ошибки
-- ============================================================================

-- 1. Таблица логов (если ещё нет)
CREATE TABLE IF NOT EXISTS amo_support_schema.sync_log (
    id              BIGSERIAL PRIMARY KEY,
    domain          TEXT NOT NULL DEFAULT 'sigmasz',
    sync_type       TEXT NOT NULL,            -- 'smart_ghost' | 'smart_inc'
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT,                     -- 'running' | 'success' | 'error'
    error_message   TEXT,
    leads_inserted  BIGINT DEFAULT 0, leads_updated   BIGINT DEFAULT 0, leads_deleted   BIGINT DEFAULT 0,
    contacts_ins    BIGINT DEFAULT 0, contacts_upd    BIGINT DEFAULT 0, contacts_del    BIGINT DEFAULT 0,
    phones_ins      BIGINT DEFAULT 0, emails_ins      BIGINT DEFAULT 0, links_ins       BIGINT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sync_log_domain_status ON amo_support_schema.sync_log (domain, status, started_at DESC);

-- 2. ГЛАВНАЯ ФУНКЦИЯ
CREATE OR REPLACE FUNCTION amo_support_schema.sync_sigmasz_smart()
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
    IF NOT pg_try_advisory_lock(hashtext('sync_sigmasz_smart')) THEN
        RETURN QUERY SELECT 'WARNING: Sync already running. Skipping.'::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
        RETURN;
    END IF;

    -- 2. Вычисляем время последнего успешного инкремента (с перекрытием 15 мин)
    SELECT COALESCE(MAX(started_at) - INTERVAL '15 minutes', NOW() - INTERVAL '1 day')
    INTO v_from_ts FROM amo_support_schema.sync_log WHERE status = 'success' AND domain = 'sigmasz';

    -- 3. Вычисляем, пора ли делать Ghost Busting (раз в час)
    SELECT MAX(started_at) INTO v_last_ghost 
    FROM amo_support_schema.sync_log WHERE status = 'success' AND sync_type = 'smart_ghost' AND domain = 'sigmasz';

    IF v_last_ghost IS NULL OR v_last_ghost < NOW() - INTERVAL '1 hour' THEN
        v_is_ghost_bust := TRUE;
        v_sync_type := 'smart_ghost';
    ELSE
        v_sync_type := 'smart_inc';
    END IF;

    -- Логируем старт
    INSERT INTO amo_support_schema.sync_log (sync_type, status) VALUES (v_sync_type, 'running') RETURNING id INTO v_log_id;

    -- ========================================================================
    -- ШАГ 1: GHOST BUSTING (Только раз в час)
    -- ========================================================================
    IF v_is_ghost_bust THEN
        -- Лиды
        CREATE TEMP TABLE _meta_leads ON COMMIT DROP AS SELECT lead_id FROM airbyte_remote.sigmasz_leads;
        CREATE INDEX ON _meta_leads(lead_id);
        
        -- SAFETY CHECK: Защита от пустого ответа FDW
        SELECT COUNT(*) INTO v_prod_leads_count FROM amo_support_schema.sigmasz_leads;
        IF (SELECT COUNT(*) FROM _meta_leads) = 0 AND v_prod_leads_count > 100 THEN
            RAISE EXCEPTION 'FDW returned 0 leads but PROD has % rows. Aborting to prevent data loss.', v_prod_leads_count;
        END IF;

        WITH deleted AS (
            DELETE FROM amo_support_schema.sigmasz_leads p
            WHERE p.updated_at < (NOW() - INTERVAL '2 hours') 
              AND NOT EXISTS (SELECT 1 FROM _meta_leads r WHERE r.lead_id = p.lead_id)
            RETURNING p.lead_id
        ) SELECT COUNT(*) INTO v_leads_del FROM deleted;

        -- Контакты
        CREATE TEMP TABLE _meta_contacts ON COMMIT DROP AS SELECT contact_id FROM airbyte_remote.sigmasz_contacts;
        CREATE INDEX ON _meta_contacts(contact_id);

        -- SAFETY CHECK: Контакты
        SELECT COUNT(*) INTO v_prod_cont_count FROM amo_support_schema.sigmasz_contacts;
        IF (SELECT COUNT(*) FROM _meta_contacts) = 0 AND v_prod_cont_count > 100 THEN
            RAISE EXCEPTION 'FDW returned 0 contacts but PROD has % rows. Aborting.', v_prod_cont_count;
        END IF;

        WITH deleted AS (
            DELETE FROM amo_support_schema.sigmasz_contacts p
            WHERE p.updated_at < (NOW() - INTERVAL '2 hours') 
              AND NOT EXISTS (SELECT 1 FROM _meta_contacts r WHERE r.contact_id = p.contact_id)
            RETURNING p.contact_id
        ) SELECT COUNT(*) INTO v_cont_del FROM deleted;
        
        DROP TABLE _meta_leads, _meta_contacts;
    END IF;

    -- ========================================================================
    -- ШАГ 2: ИНКРЕМЕНТ (Каждые 5-10 минут)
    -- ========================================================================
    
    CREATE TEMP TABLE _changed_leads ON COMMIT DROP AS
        SELECT lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json::TEXT AS raw_json
        FROM airbyte_remote.sigmasz_leads WHERE _synced_at >= v_from_ts;
    CREATE INDEX ON _changed_leads(lead_id);

    CREATE TEMP TABLE _changed_contacts ON COMMIT DROP AS
        SELECT contact_id, name, updated_at, raw_json::TEXT AS raw_json
        FROM airbyte_remote.sigmasz_contacts WHERE _synced_at >= v_from_ts;
    CREATE INDEX ON _changed_contacts(contact_id);

    -- Upsert Лидов
    WITH upserted AS (
        INSERT INTO amo_support_schema.sigmasz_leads (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json)
        SELECT lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json FROM _changed_leads
        ON CONFLICT (lead_id) DO UPDATE SET
            name = EXCLUDED.name, status_id = EXCLUDED.status_id, pipeline_id = EXCLUDED.pipeline_id, price = EXCLUDED.price,
            updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json
        WHERE EXCLUDED.updated_at >= amo_support_schema.sigmasz_leads.updated_at
        RETURNING (xmax = 0) AS is_new
    ) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_leads_ins, v_leads_upd FROM upserted;
    
    RETURN QUERY SELECT 'sigmasz_leads'::TEXT, v_leads_ins, v_leads_upd, v_leads_del;

    -- Upsert Контактов
    WITH upserted AS (
        INSERT INTO amo_support_schema.sigmasz_contacts (contact_id, name, updated_at, raw_json)
        SELECT contact_id, name, updated_at, raw_json FROM _changed_contacts
        ON CONFLICT (contact_id) DO UPDATE SET
            name = EXCLUDED.name, updated_at = EXCLUDED.updated_at, raw_json = EXCLUDED.raw_json
        WHERE EXCLUDED.updated_at >= amo_support_schema.sigmasz_contacts.updated_at
        RETURNING (xmax = 0) AS is_new
    ) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_cont_ins, v_cont_upd FROM upserted;
    
    RETURN QUERY SELECT 'sigmasz_contacts'::TEXT, v_cont_ins, v_cont_upd, v_cont_del;

    -- ========================================================================
    -- ШАГ 3: СВЯЗАННЫЕ ДАННЫЕ (С умным FDW Pushdown)
    -- ========================================================================
    
    -- Чистим сирот (только если ghost busting реально удалил кого-то)
    IF v_is_ghost_bust AND (v_leads_del > 0 OR v_cont_del > 0) THEN
        DELETE FROM amo_support_schema.sigmasz_contact_phones p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.sigmasz_contacts c WHERE c.contact_id = p.contact_id);

        DELETE FROM amo_support_schema.sigmasz_contact_emails p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.sigmasz_contacts c WHERE c.contact_id = p.contact_id);

        DELETE FROM amo_support_schema.sigmasz_lead_contacts p
        WHERE NOT EXISTS (SELECT 1 FROM amo_support_schema.sigmasz_leads l WHERE l.lead_id = p.lead_id)
           OR NOT EXISTS (SELECT 1 FROM amo_support_schema.sigmasz_contacts c WHERE c.contact_id = p.contact_id);
    END IF;

    SELECT COALESCE(array_agg(lead_id), '{}'::BIGINT[]) INTO v_arr_leads FROM _changed_leads;
    SELECT COALESCE(array_agg(contact_id), '{}'::BIGINT[]) INTO v_arr_contacts FROM _changed_contacts;
    
    v_arr_leads_len := COALESCE(array_length(v_arr_leads, 1), 0);
    v_arr_cont_len  := COALESCE(array_length(v_arr_contacts, 1), 0);

    -- ТЕЛЕФОНЫ И EMAILS (с предохранителем для больших массивов)
    IF v_arr_cont_len > 5000 THEN
        CREATE TEMP TABLE _changed_phones ON COMMIT DROP AS SELECT contact_id, phone FROM airbyte_remote.sigmasz_contact_phones;
        CREATE TEMP TABLE _changed_emails ON COMMIT DROP AS SELECT contact_id, email FROM airbyte_remote.sigmasz_contact_emails;
    ELSE
        CREATE TEMP TABLE _changed_phones ON COMMIT DROP AS SELECT contact_id, phone FROM airbyte_remote.sigmasz_contact_phones WHERE contact_id = ANY(v_arr_contacts);
        CREATE TEMP TABLE _changed_emails ON COMMIT DROP AS SELECT contact_id, email FROM airbyte_remote.sigmasz_contact_emails WHERE contact_id = ANY(v_arr_contacts);
    END IF;
    CREATE INDEX ON _changed_phones(contact_id, phone);
    CREATE INDEX ON _changed_emails(contact_id, email);

    -- Телефоны
    DELETE FROM amo_support_schema.sigmasz_contact_phones local
    WHERE local.contact_id = ANY(v_arr_contacts)
      AND NOT EXISTS (SELECT 1 FROM _changed_phones r WHERE r.contact_id = local.contact_id AND r.phone = local.phone);
    INSERT INTO amo_support_schema.sigmasz_contact_phones (contact_id, phone)
    SELECT contact_id, phone FROM _changed_phones ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_phones_ins = ROW_COUNT;
    RETURN QUERY SELECT 'sigmasz_contact_phones'::TEXT, v_phones_ins, 0::BIGINT, 0::BIGINT;

    -- Emails
    DELETE FROM amo_support_schema.sigmasz_contact_emails local
    WHERE local.contact_id = ANY(v_arr_contacts)
      AND NOT EXISTS (SELECT 1 FROM _changed_emails r WHERE r.contact_id = local.contact_id AND r.email = local.email);
    INSERT INTO amo_support_schema.sigmasz_contact_emails (contact_id, email)
    SELECT contact_id, email FROM _changed_emails ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_emails_ins = ROW_COUNT;
    RETURN QUERY SELECT 'sigmasz_contact_emails'::TEXT, v_emails_ins, 0::BIGINT, 0::BIGINT;

    -- СВЯЗИ (с предохранителем для больших массивов)
    IF v_arr_leads_len > 5000 OR v_arr_cont_len > 5000 THEN
        CREATE TEMP TABLE _changed_links ON COMMIT DROP AS SELECT lead_id, contact_id FROM airbyte_remote.sigmasz_lead_contacts;
    ELSE
        CREATE TEMP TABLE _changed_links ON COMMIT DROP AS SELECT lead_id, contact_id FROM airbyte_remote.sigmasz_lead_contacts WHERE lead_id = ANY(v_arr_leads) OR contact_id = ANY(v_arr_contacts);
    END IF;
    CREATE INDEX ON _changed_links(lead_id, contact_id);

    DELETE FROM amo_support_schema.sigmasz_lead_contacts local
    WHERE (local.lead_id = ANY(v_arr_leads) OR local.contact_id = ANY(v_arr_contacts))
      AND NOT EXISTS (SELECT 1 FROM _changed_links r WHERE r.lead_id = local.lead_id AND r.contact_id = local.contact_id);
    
    INSERT INTO amo_support_schema.sigmasz_lead_contacts (lead_id, contact_id)
    SELECT lead_id, contact_id FROM _changed_links ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_links_ins = ROW_COUNT;
    RETURN QUERY SELECT 'sigmasz_lead_contacts'::TEXT, v_links_ins, 0::BIGINT, 0::BIGINT;

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
    
    PERFORM pg_advisory_unlock(hashtext('sync_sigmasz_smart'));

EXCEPTION WHEN OTHERS THEN
    -- Логируем ошибку без RAISE — транзакция коммитнется и лог сохранится
    UPDATE amo_support_schema.sync_log SET finished_at = NOW(), status = 'error', error_message = SQLERRM WHERE id = v_log_id;
    PERFORM pg_advisory_unlock(hashtext('sync_sigmasz_smart'));
    RETURN QUERY SELECT ('ERROR: ' || SQLERRM)::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
    RETURN;
END;
$$ LANGUAGE plpgsql;
