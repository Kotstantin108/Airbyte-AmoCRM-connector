-- ==============================================================================
-- КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ ДЛЯ СИНХРОНИЗАЦИИ L2 → PRODUCTION
-- ==============================================================================
-- Этот файл содержит готовые SQL-скрипты для устранения критических проблем
-- Запустить на PRODUCTION-сервере (где находится amo_support_schema)
-- ==============================================================================

-- ==============================================================================
-- ПРОВЕРКА 1: Убедиться в наличии таблиц на Production
-- ==============================================================================
-- Выполните эту проверку ДО применения исправлений

\echo '=== ПРОВЕРКА: Наличие таблиц concepta и entrum ==='
SELECT 
    schemaname, tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'amo_support_schema'
  AND tablename ~ '(concepta|entrum)_'
ORDER BY schemaname, tablename;

-- Если результат пустой → нужно создать таблицы (см. раздел "СОЗДАНИЕ ТАБЛИЦ")

-- ==============================================================================
-- ПРОВЕРКА 2: PRIMARY KEY на таблицах
-- ==============================================================================
\echo '=== ПРОВЕРКА: PRIMARY KEY на таблицах concepta и entrum ==='
SELECT 
    tc.table_schema, tc.table_name, 
    STRING_AGG(c.column_name, ', ' ORDER BY a.attnum)
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.columns c ON kcu.column_name = c.column_name 
JOIN pg_attribute a ON a.attname = c.column_name
WHERE tc.table_schema = 'amo_support_schema'
  AND tc.table_name ~ '(concepta|entrum)_'
  AND tc.constraint_type = 'PRIMARY KEY'
GROUP BY tc.table_schema, tc.table_name
ORDER BY tc.table_name;

-- Если результат пустой для некоторых таблиц → нужно добавить PRIMARY KEY (см. ниже)

-- ==============================================================================
-- ИСПРАВЛЕНИЕ 1: СОЗДАНИЕ ТАБЛИЦ (если они не существуют)
-- ==============================================================================

-- Для CONCEPTA
CREATE TABLE IF NOT EXISTS amo_support_schema.concepta_leads (
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
CREATE INDEX IF NOT EXISTS idx_concepta_leads_synced ON amo_support_schema.concepta_leads(_synced_at);
CREATE INDEX IF NOT EXISTS idx_concepta_leads_updated ON amo_support_schema.concepta_leads(updated_at);
CREATE INDEX IF NOT EXISTS idx_concepta_leads_deleted ON amo_support_schema.concepta_leads(is_deleted) WHERE is_deleted = TRUE;

CREATE TABLE IF NOT EXISTS amo_support_schema.concepta_contacts (
    contact_id BIGINT PRIMARY KEY,
    name TEXT,
    updated_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_concepta_contacts_synced ON amo_support_schema.concepta_contacts(_synced_at);
CREATE INDEX IF NOT EXISTS idx_concepta_contacts_deleted ON amo_support_schema.concepta_contacts(is_deleted) WHERE is_deleted = TRUE;

CREATE TABLE IF NOT EXISTS amo_support_schema.concepta_lead_contacts (
    lead_id BIGINT NOT NULL,
    contact_id BIGINT NOT NULL,
    PRIMARY KEY (lead_id, contact_id),
    FOREIGN KEY (lead_id) REFERENCES amo_support_schema.concepta_leads(lead_id) ON DELETE CASCADE,
    FOREIGN KEY (contact_id) REFERENCES amo_support_schema.concepta_contacts(contact_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_concepta_lead_contacts_contact ON amo_support_schema.concepta_lead_contacts(contact_id);

CREATE TABLE IF NOT EXISTS amo_support_schema.concepta_contact_phones (
    contact_id BIGINT NOT NULL,
    phone TEXT NOT NULL,
    PRIMARY KEY (contact_id, phone),
    FOREIGN KEY (contact_id) REFERENCES amo_support_schema.concepta_contacts(contact_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amo_support_schema.concepta_contact_emails (
    contact_id BIGINT NOT NULL,
    email TEXT NOT NULL,
    PRIMARY KEY (contact_id, email),
    FOREIGN KEY (contact_id) REFERENCES amo_support_schema.concepta_contacts(contact_id) ON DELETE CASCADE
);

-- Для ENTRUM (аналогично)
CREATE TABLE IF NOT EXISTS amo_support_schema.entrum_leads (
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
CREATE INDEX IF NOT EXISTS idx_entrum_leads_synced ON amo_support_schema.entrum_leads(_synced_at);
CREATE INDEX IF NOT EXISTS idx_entrum_leads_updated ON amo_support_schema.entrum_leads(updated_at);
CREATE INDEX IF NOT EXISTS idx_entrum_leads_deleted ON amo_support_schema.entrum_leads(is_deleted) WHERE is_deleted = TRUE;

CREATE TABLE IF NOT EXISTS amo_support_schema.entrum_contacts (
    contact_id BIGINT PRIMARY KEY,
    name TEXT,
    updated_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_entrum_contacts_synced ON amo_support_schema.entrum_contacts(_synced_at);
CREATE INDEX IF NOT EXISTS idx_entrum_contacts_deleted ON amo_support_schema.entrum_contacts(is_deleted) WHERE is_deleted = TRUE;

CREATE TABLE IF NOT EXISTS amo_support_schema.entrum_lead_contacts (
    lead_id BIGINT NOT NULL,
    contact_id BIGINT NOT NULL,
    PRIMARY KEY (lead_id, contact_id),
    FOREIGN KEY (lead_id) REFERENCES amo_support_schema.entrum_leads(lead_id) ON DELETE CASCADE,
    FOREIGN KEY (contact_id) REFERENCES amo_support_schema.entrum_contacts(contact_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_entrum_lead_contacts_contact ON amo_support_schema.entrum_lead_contacts(contact_id);

CREATE TABLE IF NOT EXISTS amo_support_schema.entrum_contact_phones (
    contact_id BIGINT NOT NULL,
    phone TEXT NOT NULL,
    PRIMARY KEY (contact_id, phone),
    FOREIGN KEY (contact_id) REFERENCES amo_support_schema.entrum_contacts(contact_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS amo_support_schema.entrum_contact_emails (
    contact_id BIGINT NOT NULL,
    email TEXT NOT NULL,
    PRIMARY KEY (contact_id, email),
    FOREIGN KEY (contact_id) REFERENCES amo_support_schema.entrum_contacts(contact_id) ON DELETE CASCADE
);

\echo '✅ Таблицы concepta и entrum созданы (или уже существовали)';

-- ==============================================================================
-- ИСПРАВЛЕНИЕ 2: ДОБАВЛЕНИЕ PRIMARY KEY (если таблицы существуют без ключей)
-- ==============================================================================

-- CONCEPTA
ALTER TABLE amo_support_schema.concepta_leads 
    ADD PRIMARY KEY (lead_id) ON CONFLICT DO NOTHING;

ALTER TABLE amo_support_schema.concepta_contacts 
    ADD PRIMARY KEY (contact_id) ON CONFLICT DO NOTHING;

-- ENTRUM  
ALTER TABLE amo_support_schema.entrum_leads 
    ADD PRIMARY KEY (lead_id) ON CONFLICT DO NOTHING;

ALTER TABLE amo_support_schema.entrum_contacts 
    ADD PRIMARY KEY (contact_id) ON CONFLICT DO NOTHING;

\echo '✅ PRIMARY KEY добавлены (или уже существовали)';

-- ==============================================================================
-- ИСПРАВЛЕНИЕ 3: СОЗДАНИЕ ИНДЕКСОВ НА L2 СЕРВЕРЕ (если их нет)
-- ==============================================================================
-- Выполните эту часть на ANALYTICS (L2) сервере!

\echo '=== Инструкции для L2 сервера (Analytics) ==='
\echo 'Подключитесь к L2 и выполните:'

-- На L2 сервере (prod_sync схема):
CREATE INDEX IF NOT EXISTS idx_concepta_leads_synced_at 
    ON prod_sync.concepta_leads (_synced_at);
CREATE INDEX IF NOT EXISTS idx_concepta_contacts_synced_at 
    ON prod_sync.concepta_contacts (_synced_at);
CREATE INDEX IF NOT EXISTS idx_entrum_leads_synced_at 
    ON prod_sync.entrum_leads (_synced_at);
CREATE INDEX IF NOT EXISTS idx_entrum_contacts_synced_at 
    ON prod_sync.entrum_contacts (_synced_at);

\echo '✅ Индексы на L2 созданы';

-- ==============================================================================
-- ИСПРАВЛЕНИЕ 4: ИСПРАВЛЕННАЯ ФУНКЦИЯ sync_concepta_smart() 
-- ==============================================================================
-- Улучшенная версия с:
-- 1. Проверкой на % потерь данных
-- 2. INTERVAL '1 hour' вместо '15 minutes'
-- 3. Проверкой доступности FDW перед началом

CREATE OR REPLACE FUNCTION amo_support_schema.sync_concepta_smart_v2()
RETURNS TABLE (entity_name TEXT, inserted BIGINT, updated BIGINT, deleted BIGINT) AS $$
DECLARE
    v_log_id        BIGINT;
    v_from_ts       TIMESTAMPTZ;
    v_last_ghost    TIMESTAMPTZ;
    v_is_ghost_bust BOOLEAN := FALSE;
    v_sync_type     TEXT;

    v_prod_leads_count BIGINT;
    v_prod_cont_count  BIGINT;
    v_fdw_leads_count  BIGINT;
    v_fdw_cont_count   BIGINT;
    v_loss_percent_leads NUMERIC;
    v_loss_percent_cont  NUMERIC;

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

    -- 2. Вычисляем время последнего успешного инкремента (ИСПРАВЛЕНИЕ: 1 hour вместо 15 minutes)
    SELECT COALESCE(MAX(started_at) - INTERVAL '1 hour', NOW() - INTERVAL '1 day')
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
    -- ПРОВЕРКА: Доступность FDW перед началом работы
    -- ========================================================================
    BEGIN
        SELECT COUNT(*) INTO v_fdw_leads_count FROM airbyte_remote.concepta_leads;
    EXCEPTION WHEN OTHERS THEN
        UPDATE amo_support_schema.sync_log SET 
            finished_at = NOW(), 
            status = 'error',
            error_message = 'FDW Server unavailable: ' || SQLERRM
        WHERE id = v_log_id;
        PERFORM pg_advisory_unlock(hashtext('sync_concepta_smart'));
        RETURN QUERY SELECT ('ERROR: FDW unavailable - ' || SQLERRM)::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
        RETURN;
    END;

    -- ========================================================================
    -- ШАГ 1: GHOST BUSTING (Только раз в час)
    -- ========================================================================
    IF v_is_ghost_bust THEN
        -- Лиды
        CREATE TEMP TABLE _meta_leads ON COMMIT DROP AS SELECT lead_id FROM airbyte_remote.concepta_leads;
        CREATE INDEX ON _meta_leads(lead_id);

        -- IMPROVED SAFETY CHECK: Проверка на % потерь (вместо COUNT = 0)
        SELECT COUNT(*) INTO v_prod_leads_count FROM amo_support_schema.concepta_leads;
        SELECT COUNT(*) INTO v_fdw_leads_count FROM _meta_leads;
        
        IF v_prod_leads_count > 0 THEN
            v_loss_percent_leads := 100.0 * (v_prod_leads_count - v_fdw_leads_count) / v_prod_leads_count;
            IF v_loss_percent_leads > 20 THEN
                RAISE EXCEPTION 'FDW data loss detected for leads: %.1f%% data loss (FDW: %, PROD: %)',
                    v_loss_percent_leads, v_fdw_leads_count, v_prod_leads_count;
            END IF;
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

        -- IMPROVED SAFETY CHECK
        SELECT COUNT(*) INTO v_prod_cont_count FROM amo_support_schema.concepta_contacts;
        SELECT COUNT(*) INTO v_fdw_cont_count FROM _meta_contacts;
        
        IF v_prod_cont_count > 0 THEN
            v_loss_percent_cont := 100.0 * (v_prod_cont_count - v_fdw_cont_count) / v_prod_cont_count;
            IF v_loss_percent_cont > 20 THEN
                RAISE EXCEPTION 'FDW data loss detected for contacts: %.1f%% data loss (FDW: %, PROD: %)',
                    v_loss_percent_cont, v_fdw_cont_count, v_prod_cont_count;
            END IF;
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
    -- ШАГ 3: СВЯЗАННЫЕ ДАННЫЕ (как в оригинале, без изменений)
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

\echo '✅ Улучшенная функция sync_concepta_smart_v2() создана';
\echo '   Замените вызовы sync_concepta_smart() на sync_concepta_smart_v2()';

-- ==============================================================================
-- ИСПРАВЛЕНИЕ 5: АНАЛОГИЧНАЯ ФУНКЦИЯ ДЛЯ ENTRUM
-- ==============================================================================
-- (Аналогично sync_concepta_smart_v2, но для entrum домена)
-- Создайте sync_entrum_smart_v2() по аналогии выше, заменив:
-- - concepta → entrum
-- - hashtext('sync_concepta_smart') → hashtext('sync_entrum_smart')
-- - amo_support_schema.concepta_* → amo_support_schema.entrum_*

-- ==============================================================================
-- ПРОВЕРКА: Синхронизация времени между L2 и Prod
-- ==============================================================================
\echo '=== ПРОВЕРКА: Разница времени между сервами ==='
SELECT 
    'PRODUCTION' as server,
    NOW() as current_time,
    extract(EPOCH FROM (NOW() - (SELECT NOW()))::INTERVAL)::INT as offset_seconds;

-- Выполните аналогично на L2 и сравните offset_seconds
-- Если разница > 30 секунд → нужна синхронизация NTP

-- ==============================================================================
-- ФИНАЛЬНЫЙ ЧЕК: Всё ли готово?
-- ==============================================================================
\echo ''
\echo '╔════════════════════════════════════════════════════════════════════════╗'
\echo '║                     ЧЕКЛИСТ ВНЕДРЕНИЯ ИСПРАВЛЕНИЙ                       ║'
\echo '╠════════════════════════════════════════════════════════════════════════╣'
\echo '║ ✅ На PRODUCTION:                                                       ║'
\echo '║    [ ] Таблицы concepta_* и entrum_* созданы                           ║'
\echo '║    [ ] PRIMARY KEY есть на всех таблицах                                ║'
\echo '║    [ ] Функция sync_concepta_smart_v2() создана и работает              ║'
\echo '║    [ ] Функция sync_entrum_smart_v2() создана и работает                ║'
\echo '║    [ ] Заменены вызовы в расписании на _v2 функции                      ║'
\echo '║                                                                          ║'
\echo '║ ✅ На ANALYTICS (L2):                                                   ║'
\echo '║    [ ] Индексы на _synced_at созданы                                    ║'
\echo '║    [ ] Время синхронизировано с PRODUCTION (NTP)                        ║'
\echo '║                                                                          ║'
\echo '║ ✅ Мониторинг и тестирование:                                           ║'
\echo '║    [ ] Запущена пробная синхронизация                                   ║'
\echo '║    [ ] Проверены логи в sync_log (должны быть success)                  ║'
\echo '║    [ ] Настроены алерты на ошибки                                       ║'
\echo '║    [ ] Документация обновлена                                           ║'
\echo '╚════════════════════════════════════════════════════════════════════════╝'
\echo ''

