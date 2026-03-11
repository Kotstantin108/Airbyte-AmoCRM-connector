-- ============================================
-- НАСТРОЙКА FDW (Foreign Data Wrapper) ДЛЯ СИНХРОНИЗАЦИИ
-- Синхронизация L2 (prod_sync) -> Прод через n8n
-- Домен: sigmasz (заменить на concepta/entrum для других доменов)
-- ============================================

-- ============================================
-- ЧАСТЬ 1: НАСТРОЙКА НА ПРОДЕ (подключение к L2)
-- ============================================

-- 1.1. Установка расширения postgres_fdw (если еще не установлено)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 1.2. Создание внешнего сервера (Foreign Server)
-- ВАЖНО: Замените параметры на реальные!
CREATE SERVER airbyte_analytics_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host '<analytics_server_host>',        -- IP или hostname сервера L2
    port '5432',                           -- Порт PostgreSQL
    dbname '<analytics_db_name>',          -- Имя базы данных на L2
    connect_timeout '10',                   -- Таймаут подключения (секунды)
    fetch_size '10000'                      -- Размер батча при выборке
);

-- 1.3. Создание пользователя для подключения
-- ВАЖНО: Используйте отдельного пользователя с ограниченными правами!
CREATE USER MAPPING FOR CURRENT_USER
SERVER airbyte_analytics_server
OPTIONS (
    user 'fdw_user',                        -- Пользователь на L2 сервере
    password 'strong_password_here'         -- Пароль (в продакшене используйте секреты)
);

-- Альтернатива: использовать системного пользователя
-- CREATE USER MAPPING FOR postgres
-- SERVER airbyte_analytics_server
-- OPTIONS (user 'fdw_user', password 'strong_password_here');

-- 1.4. Импорт схемы prod_sync
-- Импортируем только необходимые таблицы
IMPORT FOREIGN SCHEMA "prod_sync"
LIMIT TO (
    sigmasz_leads,
    sigmasz_contacts,
    sigmasz_lead_contacts,
    sigmasz_contact_phones,
    sigmasz_contact_emails
)
FROM SERVER airbyte_analytics_server
INTO airbyte_remote;

-- Проверка импортированных таблиц
SELECT 
    foreign_table_schema,
    foreign_table_name,
    foreign_server_name
FROM information_schema.foreign_tables
WHERE foreign_server_name = 'airbyte_analytics_server'
ORDER BY foreign_table_name;

-- ============================================
-- ЧАСТЬ 2: СОЗДАНИЕ ЛОКАЛЬНЫХ ТАБЛИЦ НА ПРОДЕ
-- ============================================

-- 2.1. Создание схемы для синхронизированных данных
CREATE SCHEMA IF NOT EXISTS airbyte_sync;

-- 2.2. Таблица Leads
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_leads (
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

CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_synced ON airbyte_sync.sigmasz_leads(_synced_at);
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_updated ON airbyte_sync.sigmasz_leads(updated_at);
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_deleted ON airbyte_sync.sigmasz_leads(is_deleted) WHERE is_deleted = TRUE;

-- 2.3. Таблица Contacts
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_contacts (
    contact_id BIGINT PRIMARY KEY,
    name TEXT,
    updated_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_synced ON airbyte_sync.sigmasz_contacts(_synced_at);
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_deleted ON airbyte_sync.sigmasz_contacts(is_deleted) WHERE is_deleted = TRUE;

-- 2.4. Таблица связей Lead-Contact
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_lead_contacts (
    lead_id BIGINT NOT NULL,
    contact_id BIGINT NOT NULL,
    PRIMARY KEY (lead_id, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_sigmasz_lead_contacts_contact ON airbyte_sync.sigmasz_lead_contacts(contact_id);

-- 2.5. Таблица телефонов
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_contact_phones (
    contact_id BIGINT NOT NULL,
    phone TEXT NOT NULL,
    PRIMARY KEY (contact_id, phone)
);

-- 2.6. Таблица email
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_contact_emails (
    contact_id BIGINT NOT NULL,
    email TEXT NOT NULL,
    PRIMARY KEY (contact_id, email)
);

-- ============================================
-- ЧАСТЬ 3: ФУНКЦИИ СИНХРОНИЗАЦИИ
-- ============================================

-- 3.1. Функция синхронизации Leads
CREATE OR REPLACE FUNCTION airbyte_sync.sync_sigmasz_leads(
    p_sync_interval_minutes INT DEFAULT 20
)
RETURNS TABLE (
    inserted_count BIGINT,
    updated_count BIGINT,
    deleted_count BIGINT,
    error_message TEXT
) AS $$
DECLARE
    v_inserted BIGINT := 0;
    v_updated BIGINT := 0;
    v_deleted BIGINT := 0;
    v_error TEXT;
    v_sync_from TIMESTAMPTZ;
BEGIN
    -- Определяем время последней синхронизации
    v_sync_from := NOW() - (p_sync_interval_minutes || ' minutes')::INTERVAL;
    
    BEGIN
        -- Синхронизация новых и обновленных записей
        WITH synced AS (
            INSERT INTO airbyte_sync.sigmasz_leads (
                lead_id, name, status_id, pipeline_id, price,
                created_at, updated_at, raw_json, is_deleted, _synced_at
            )
            SELECT 
                lead_id, name, status_id, pipeline_id, price,
                created_at, updated_at, raw_json, is_deleted, NOW()
            FROM airbyte_remote.sigmasz_leads
            WHERE _synced_at > v_sync_from
            ON CONFLICT (lead_id) DO UPDATE SET
                name = EXCLUDED.name,
                status_id = EXCLUDED.status_id,
                pipeline_id = EXCLUDED.pipeline_id,
                price = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at,
                raw_json = EXCLUDED.raw_json,
                is_deleted = EXCLUDED.is_deleted,
                _synced_at = NOW()
            RETURNING (xmax = 0) AS inserted
        )
        SELECT 
            COUNT(*) FILTER (WHERE inserted) INTO v_inserted,
            COUNT(*) FILTER (WHERE NOT inserted) INTO v_updated
        FROM synced;
        
        -- Синхронизация удалений (is_deleted = TRUE)
        WITH deleted AS (
            UPDATE airbyte_sync.sigmasz_leads
            SET is_deleted = TRUE, _synced_at = NOW()
            FROM airbyte_remote.sigmasz_leads
            WHERE airbyte_sync.sigmasz_leads.lead_id = airbyte_remote.sigmasz_leads.lead_id
              AND airbyte_remote.sigmasz_leads.is_deleted = TRUE
              AND airbyte_sync.sigmasz_leads.is_deleted = FALSE
              AND airbyte_remote.sigmasz_leads._synced_at > v_sync_from
            RETURNING airbyte_sync.sigmasz_leads.lead_id
        )
        SELECT COUNT(*) INTO v_deleted FROM deleted;
        
        RETURN QUERY SELECT v_inserted, v_updated, v_deleted, NULL::TEXT;
        
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
            RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::BIGINT, v_error;
    END;
END;
$$ LANGUAGE plpgsql;

-- 3.2. Функция синхронизации Contacts
CREATE OR REPLACE FUNCTION airbyte_sync.sync_sigmasz_contacts(
    p_sync_interval_minutes INT DEFAULT 20
)
RETURNS TABLE (
    inserted_count BIGINT,
    updated_count BIGINT,
    deleted_count BIGINT,
    error_message TEXT
) AS $$
DECLARE
    v_inserted BIGINT := 0;
    v_updated BIGINT := 0;
    v_deleted BIGINT := 0;
    v_error TEXT;
    v_sync_from TIMESTAMPTZ;
BEGIN
    v_sync_from := NOW() - (p_sync_interval_minutes || ' minutes')::INTERVAL;
    
    BEGIN
        -- Синхронизация новых и обновленных записей
        WITH synced AS (
            INSERT INTO airbyte_sync.sigmasz_contacts (
                contact_id, name, updated_at, raw_json, is_deleted, _synced_at
            )
            SELECT 
                contact_id, name, updated_at, raw_json, is_deleted, NOW()
            FROM airbyte_remote.sigmasz_contacts
            WHERE _synced_at > v_sync_from
            ON CONFLICT (contact_id) DO UPDATE SET
                name = EXCLUDED.name,
                updated_at = EXCLUDED.updated_at,
                raw_json = EXCLUDED.raw_json,
                is_deleted = EXCLUDED.is_deleted,
                _synced_at = NOW()
            RETURNING (xmax = 0) AS inserted
        )
        SELECT 
            COUNT(*) FILTER (WHERE inserted) INTO v_inserted,
            COUNT(*) FILTER (WHERE NOT inserted) INTO v_updated
        FROM synced;
        
        -- Синхронизация удалений
        WITH deleted AS (
            UPDATE airbyte_sync.sigmasz_contacts
            SET is_deleted = TRUE, _synced_at = NOW()
            FROM airbyte_remote.sigmasz_contacts
            WHERE airbyte_sync.sigmasz_contacts.contact_id = airbyte_remote.sigmasz_contacts.contact_id
              AND airbyte_remote.sigmasz_contacts.is_deleted = TRUE
              AND airbyte_sync.sigmasz_contacts.is_deleted = FALSE
              AND airbyte_remote.sigmasz_contacts._synced_at > v_sync_from
            RETURNING airbyte_sync.sigmasz_contacts.contact_id
        )
        SELECT COUNT(*) INTO v_deleted FROM deleted;
        
        RETURN QUERY SELECT v_inserted, v_updated, v_deleted, NULL::TEXT;
        
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
            RETURN QUERY SELECT 0::BIGINT, 0::BIGINT, 0::BIGINT, v_error;
    END;
END;
$$ LANGUAGE plpgsql;

-- 3.3. Функция синхронизации связей Lead-Contact
CREATE OR REPLACE FUNCTION airbyte_sync.sync_sigmasz_lead_contacts(
    p_sync_interval_minutes INT DEFAULT 20
)
RETURNS TABLE (
    inserted_count BIGINT,
    error_message TEXT
) AS $$
DECLARE
    v_inserted BIGINT := 0;
    v_error TEXT;
    v_sync_from TIMESTAMPTZ;
BEGIN
    v_sync_from := NOW() - (p_sync_interval_minutes || ' minutes')::INTERVAL;
    
    BEGIN
        -- Синхронизация связей для обновленных leads
        WITH synced AS (
            INSERT INTO airbyte_sync.sigmasz_lead_contacts (lead_id, contact_id)
            SELECT DISTINCT lc.lead_id, lc.contact_id
            FROM airbyte_remote.sigmasz_lead_contacts lc
            INNER JOIN airbyte_remote.sigmasz_leads l ON l.lead_id = lc.lead_id
            WHERE l._synced_at > v_sync_from
            ON CONFLICT DO NOTHING
            RETURNING lead_id
        )
        SELECT COUNT(*) INTO v_inserted FROM synced;
        
        RETURN QUERY SELECT v_inserted, NULL::TEXT;
        
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
            RETURN QUERY SELECT 0::BIGINT, v_error;
    END;
END;
$$ LANGUAGE plpgsql;

-- 3.4. Функция синхронизации телефонов
CREATE OR REPLACE FUNCTION airbyte_sync.sync_sigmasz_contact_phones(
    p_sync_interval_minutes INT DEFAULT 20
)
RETURNS TABLE (
    inserted_count BIGINT,
    error_message TEXT
) AS $$
DECLARE
    v_inserted BIGINT := 0;
    v_error TEXT;
    v_sync_from TIMESTAMPTZ;
BEGIN
    v_sync_from := NOW() - (p_sync_interval_minutes || ' minutes')::INTERVAL;
    
    BEGIN
        -- Синхронизация телефонов для обновленных контактов
        WITH synced AS (
            INSERT INTO airbyte_sync.sigmasz_contact_phones (contact_id, phone)
            SELECT DISTINCT cp.contact_id, cp.phone
            FROM airbyte_remote.sigmasz_contact_phones cp
            INNER JOIN airbyte_remote.sigmasz_contacts c ON c.contact_id = cp.contact_id
            WHERE c._synced_at > v_sync_from
            ON CONFLICT DO NOTHING
            RETURNING contact_id
        )
        SELECT COUNT(*) INTO v_inserted FROM synced;
        
        RETURN QUERY SELECT v_inserted, NULL::TEXT;
        
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
            RETURN QUERY SELECT 0::BIGINT, v_error;
    END;
END;
$$ LANGUAGE plpgsql;

-- 3.5. Функция синхронизации email
CREATE OR REPLACE FUNCTION airbyte_sync.sync_sigmasz_contact_emails(
    p_sync_interval_minutes INT DEFAULT 20
)
RETURNS TABLE (
    inserted_count BIGINT,
    error_message TEXT
) AS $$
DECLARE
    v_inserted BIGINT := 0;
    v_error TEXT;
    v_sync_from TIMESTAMPTZ;
BEGIN
    v_sync_from := NOW() - (p_sync_interval_minutes || ' minutes')::INTERVAL;
    
    BEGIN
        -- Синхронизация email для обновленных контактов
        WITH synced AS (
            INSERT INTO airbyte_sync.sigmasz_contact_emails (contact_id, email)
            SELECT DISTINCT ce.contact_id, ce.email
            FROM airbyte_remote.sigmasz_contact_emails ce
            INNER JOIN airbyte_remote.sigmasz_contacts c ON c.contact_id = ce.contact_id
            WHERE c._synced_at > v_sync_from
            ON CONFLICT DO NOTHING
            RETURNING contact_id
        )
        SELECT COUNT(*) INTO v_inserted FROM synced;
        
        RETURN QUERY SELECT v_inserted, NULL::TEXT;
        
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
            RETURN QUERY SELECT 0::BIGINT, v_error;
    END;
END;
$$ LANGUAGE plpgsql;

-- 3.6. Главная функция синхронизации всего домена
CREATE OR REPLACE FUNCTION airbyte_sync.sync_sigmasz_all(
    p_sync_interval_minutes INT DEFAULT 20
)
RETURNS TABLE (
    table_name TEXT,
    inserted BIGINT,
    updated BIGINT,
    deleted BIGINT,
    error_message TEXT
) AS $$
DECLARE
    v_result RECORD;
BEGIN
    -- Синхронизация Leads
    SELECT * INTO v_result FROM airbyte_sync.sync_sigmasz_leads(p_sync_interval_minutes);
    RETURN QUERY SELECT 
        'sigmasz_leads'::TEXT,
        v_result.inserted_count,
        v_result.updated_count,
        v_result.deleted_count,
        v_result.error_message;
    
    -- Синхронизация Contacts
    SELECT * INTO v_result FROM airbyte_sync.sync_sigmasz_contacts(p_sync_interval_minutes);
    RETURN QUERY SELECT 
        'sigmasz_contacts'::TEXT,
        v_result.inserted_count,
        v_result.updated_count,
        v_result.deleted_count,
        v_result.error_message;
    
    -- Синхронизация связей
    SELECT * INTO v_result FROM airbyte_sync.sync_sigmasz_lead_contacts(p_sync_interval_minutes);
    RETURN QUERY SELECT 
        'sigmasz_lead_contacts'::TEXT,
        v_result.inserted_count,
        0::BIGINT,
        0::BIGINT,
        v_result.error_message;
    
    -- Синхронизация телефонов
    SELECT * INTO v_result FROM airbyte_sync.sync_sigmasz_contact_phones(p_sync_interval_minutes);
    RETURN QUERY SELECT 
        'sigmasz_contact_phones'::TEXT,
        v_result.inserted_count,
        0::BIGINT,
        0::BIGINT,
        v_result.error_message;
    
    -- Синхронизация email
    SELECT * INTO v_result FROM airbyte_sync.sync_sigmasz_contact_emails(p_sync_interval_minutes);
    RETURN QUERY SELECT 
        'sigmasz_contact_emails'::TEXT,
        v_result.inserted_count,
        0::BIGINT,
        0::BIGINT,
        v_result.error_message;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ЧАСТЬ 4: НАСТРОЙКА НА L2 СЕРВЕРЕ (для FDW)
-- ============================================

-- 4.1. Создание пользователя для FDW (на L2 сервере)
/*
CREATE USER fdw_user WITH PASSWORD 'strong_password_here';
GRANT USAGE ON SCHEMA prod_sync TO fdw_user;
GRANT SELECT ON ALL TABLES IN SCHEMA prod_sync TO fdw_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod_sync GRANT SELECT ON TABLES TO fdw_user;
*/

-- 4.2. Настройка pg_hba.conf (на L2 сервере)
/*
# FDW connections from Prod server
host    all    fdw_user    <prod_server_ip>/32    md5
*/

-- После изменения pg_hba.conf:
-- SELECT pg_reload_conf();

-- ============================================
-- ЧАСТЬ 5: ИСПОЛЬЗОВАНИЕ В N8N
-- ============================================

-- Пример SQL для n8n workflow (Postgres node):
/*
-- Вариант 1: Использование главной функции
SELECT * FROM airbyte_sync.sync_sigmasz_all(20);

-- Вариант 2: Синхронизация отдельных таблиц
SELECT * FROM airbyte_sync.sync_sigmasz_leads(20);
SELECT * FROM airbyte_sync.sync_sigmasz_contacts(20);
SELECT * FROM airbyte_sync.sync_sigmasz_lead_contacts(20);
SELECT * FROM airbyte_sync.sync_sigmasz_contact_phones(20);
SELECT * FROM airbyte_sync.sync_sigmasz_contact_emails(20);
*/

-- ============================================
-- ЧАСТЬ 6: МОНИТОРИНГ И ПРОВЕРКА
-- ============================================

-- 6.1. Проверка подключения к внешнему серверу
SELECT * FROM airbyte_remote.sigmasz_leads LIMIT 1;

-- 6.2. Проверка количества записей
SELECT 
    'L2 (remote)' AS source,
    COUNT(*) AS leads_count
FROM airbyte_remote.sigmasz_leads
UNION ALL
SELECT 
    'Прод (local)' AS source,
    COUNT(*) AS leads_count
FROM airbyte_sync.sigmasz_leads;

-- 6.3. Проверка последней синхронизации
SELECT 
    'leads' AS table_name,
    MAX(_synced_at) AS last_sync
FROM airbyte_sync.sigmasz_leads
UNION ALL
SELECT 
    'contacts' AS table_name,
    MAX(_synced_at) AS last_sync
FROM airbyte_sync.sigmasz_contacts;

-- 6.4. Проверка расхождений
SELECT 
    l2.lead_id,
    l2.name AS l2_name,
    l2.updated_at AS l2_updated,
    prod.name AS prod_name,
    prod.updated_at AS prod_updated
FROM airbyte_remote.sigmasz_leads l2
FULL OUTER JOIN airbyte_sync.sigmasz_leads prod ON l2.lead_id = prod.lead_id
WHERE l2.lead_id IS NULL OR prod.lead_id IS NULL
   OR l2.updated_at != prod.updated_at
LIMIT 100;

-- ============================================
-- ЧАСТЬ 7: ДЛЯ ДРУГИХ ДОМЕНОВ
-- ============================================

-- 7.1. Импорт схемы для других доменов
/*
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
*/

-- 7.2. Создание функций синхронизации для других доменов
-- Замените sigmasz на concepta/entrum в функциях выше

-- ============================================
-- ПРИМЕЧАНИЯ
-- ============================================

/*
ВАЖНО:
1. Замените <analytics_server_host> и <analytics_db_name> на реальные значения
2. Замените 'strong_password_here' на сильный пароль
3. Настройте pg_hba.conf на L2 сервере для разрешения подключений
4. Используйте отдельного пользователя для FDW с ограниченными правами
5. Настройте n8n workflow для периодического вызова функций синхронизации

БЕЗОПАСНОСТЬ:
- Используйте отдельного пользователя для FDW
- Ограничьте права только SELECT на необходимые таблицы
- Используйте SSL соединения (рекомендуется)
- Регулярно меняйте пароли

ПРОИЗВОДИТЕЛЬНОСТЬ:
- Настройте fetch_size для оптимизации выборки
- Используйте индексы на _synced_at для быстрой фильтрации
- Мониторьте время выполнения синхронизации
- При необходимости увеличьте интервал синхронизации

ОБРАБОТКА ОШИБОК:
- Функции возвращают error_message при ошибках
- Настройте алерты в n8n на ошибки синхронизации
- Регулярно проверяйте логи синхронизации
*/
