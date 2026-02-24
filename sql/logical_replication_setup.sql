-- ============================================
-- НАСТРОЙКА POSTGRESQL LOGICAL REPLICATION
-- Синхронизация L2 (prod_sync) -> Прод
-- Домен: sigmasz (заменить на concepta/entrum для других доменов)
-- ============================================

-- ============================================
-- ЧАСТЬ 1: НАСТРОЙКА ИЗДАТЕЛЯ (L2 сервер - Analytics)
-- ============================================

-- 1.1. Проверка текущих настроек
-- Выполните на сервере L2 (Analytics):
SELECT name, setting, unit, context 
FROM pg_settings 
WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders')
ORDER BY name;

-- 1.2. Настройка postgresql.conf (требуется перезапуск PostgreSQL)
-- Добавьте или измените следующие параметры в postgresql.conf:
/*
wal_level = logical                    # Включить logical replication
max_replication_slots = 10             # Минимум: количество подписок + резерв
max_wal_senders = 10                   # Минимум: max_replication_slots + физические реплики
max_logical_replication_workers = 4   # Количество воркеров для logical replication
max_sync_workers_per_subscription = 2 # Воркеры для синхронизации таблиц
*/

-- 1.3. Настройка pg_hba.conf (требуется перезагрузка конфигурации)
-- Добавьте строку для разрешения репликации с сервера Прод:
/*
# Logical Replication from Prod server
host    replication    replication_user    <prod_server_ip>/32    md5
host    all            replication_user    <prod_server_ip>/32    md5
*/

-- После изменения конфигурации:
-- SELECT pg_reload_conf(); -- Для pg_hba.conf
-- Или перезапустите PostgreSQL для postgresql.conf

-- 1.4. Создание пользователя для репликации (на L2 сервере)
CREATE USER replication_user WITH REPLICATION PASSWORD 'strong_password_here';

-- Предоставление прав на чтение схемы prod_sync
GRANT USAGE ON SCHEMA prod_sync TO replication_user;
GRANT SELECT ON ALL TABLES IN SCHEMA prod_sync TO replication_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod_sync GRANT SELECT ON TABLES TO replication_user;

-- 1.5. Создание публикации для домена sigmasz
CREATE PUBLICATION airbyte_prod_sync_sigmasz FOR TABLE 
    prod_sync.sigmasz_leads,
    prod_sync.sigmasz_contacts,
    prod_sync.sigmasz_lead_contacts,
    prod_sync.sigmasz_contact_phones,
    prod_sync.sigmasz_contact_emails
WITH (publish = 'insert,update,delete');

-- Проверка публикации
SELECT * FROM pg_publication WHERE pubname = 'airbyte_prod_sync_sigmasz';
SELECT * FROM pg_publication_tables WHERE pubname = 'airbyte_prod_sync_sigmasz';

-- ============================================
-- ЧАСТЬ 2: НАСТРОЙКА ПОДПИСЧИКА (Прод сервер)
-- ============================================

-- 2.1. Проверка настроек на Проде
SELECT name, setting, unit, context 
FROM pg_settings 
WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders')
ORDER BY name;

-- Примечание: На подписчике wal_level может быть 'replica' или 'logical'
-- max_replication_slots должен быть >= 1

-- 2.2. Создание таблиц на Проде (если еще не созданы)
-- Таблицы должны иметь такую же структуру, как в L2

CREATE SCHEMA IF NOT EXISTS airbyte_sync;

-- Таблица Leads
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

-- Таблица Contacts
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_contacts (
    contact_id BIGINT PRIMARY KEY,
    name TEXT,
    updated_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);

-- Таблица связей Lead-Contact
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_lead_contacts (
    lead_id BIGINT NOT NULL,
    contact_id BIGINT NOT NULL,
    PRIMARY KEY (lead_id, contact_id)
);

-- Таблица телефонов
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_contact_phones (
    contact_id BIGINT NOT NULL,
    phone TEXT NOT NULL,
    PRIMARY KEY (contact_id, phone)
);

-- Таблица email
CREATE TABLE IF NOT EXISTS airbyte_sync.sigmasz_contact_emails (
    contact_id BIGINT NOT NULL,
    email TEXT NOT NULL,
    PRIMARY KEY (contact_id, email)
);

-- Создание индексов
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_synced ON airbyte_sync.sigmasz_leads(_synced_at);
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_synced ON airbyte_sync.sigmasz_contacts(_synced_at);
CREATE INDEX IF NOT EXISTS idx_sigmasz_lead_contacts_contact ON airbyte_sync.sigmasz_lead_contacts(contact_id);

-- 2.3. Создание подписки
-- ВАЖНО: Замените параметры подключения на реальные!
CREATE SUBSCRIPTION airbyte_subscription_sigmasz
CONNECTION 'host=<analytics_server_host> port=5432 dbname=<analytics_db_name> user=replication_user password=strong_password_here'
PUBLICATION airbyte_prod_sync_sigmasz
WITH (
    copy_data = true,              -- Копировать существующие данные
    create_slot = true,            -- Создать replication slot автоматически
    enabled = true,                -- Включить подписку сразу
    slot_name = 'airbyte_slot_sigmasz',  -- Имя слота
    synchronous_commit = 'off'     -- Для производительности (можно 'local')
);

-- Проверка подписки
SELECT * FROM pg_subscription WHERE subname = 'airbyte_subscription_sigmasz';
SELECT * FROM pg_subscription_rel WHERE srsubid = (SELECT oid FROM pg_subscription WHERE subname = 'airbyte_subscription_sigmasz');

-- Проверка статуса репликации
SELECT 
    subname,
    subenabled,
    subslotname,
    subpublications
FROM pg_subscription;

-- Проверка lag репликации
SELECT 
    application_name,
    client_addr,
    state,
    sync_state,
    sync_priority,
    sync_standby_priority
FROM pg_stat_replication
WHERE application_name LIKE 'airbyte%';

-- ============================================
-- ЧАСТЬ 3: ОБРАБОТКА КОНФЛИКТОВ
-- ============================================

-- 3.1. Функция для разрешения конфликтов при UPDATE
-- L2 является источником истины, поэтому данные из L2 имеют приоритет
-- Но нужно учесть, что n8n может писать в прод через webhook

-- Создаем функцию для обработки конфликтов в leads
CREATE OR REPLACE FUNCTION airbyte_sync.resolve_lead_conflict()
RETURNS TRIGGER AS $$
BEGIN
    -- Если данные из L2 новее (по updated_at), используем их
    -- Иначе сохраняем локальные данные (от n8n webhook)
    IF NEW.updated_at >= OLD.updated_at THEN
        -- Данные из L2 новее или равны - используем их
        RETURN NEW;
    ELSE
        -- Локальные данные новее - сохраняем их, но обновляем _synced_at
        NEW._synced_at := OLD._synced_at;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Создаем функцию для обработки конфликтов в contacts
CREATE OR REPLACE FUNCTION airbyte_sync.resolve_contact_conflict()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.updated_at >= OLD.updated_at THEN
        RETURN NEW;
    ELSE
        NEW._synced_at := OLD._synced_at;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 3.2. Создание триггеров для обработки конфликтов
-- Эти триггеры будут срабатывать при конфликтах во время репликации

-- Для leads
CREATE TRIGGER trg_resolve_lead_conflict
BEFORE UPDATE ON airbyte_sync.sigmasz_leads
FOR EACH ROW
WHEN (OLD.updated_at IS DISTINCT FROM NEW.updated_at)
EXECUTE FUNCTION airbyte_sync.resolve_lead_conflict();

-- Для contacts
CREATE TRIGGER trg_resolve_contact_conflict
BEFORE UPDATE ON airbyte_sync.sigmasz_contacts
FOR EACH ROW
WHEN (OLD.updated_at IS DISTINCT FROM NEW.updated_at)
EXECUTE FUNCTION airbyte_sync.resolve_contact_conflict();

-- ============================================
-- ЧАСТЬ 4: МОНИТОРИНГ И УПРАВЛЕНИЕ
-- ============================================

-- 4.1. Проверка статуса репликации
CREATE OR REPLACE VIEW airbyte_sync.replication_status AS
SELECT 
    s.subname AS subscription_name,
    s.subenabled AS enabled,
    s.subslotname AS slot_name,
    sr.srrelid::regclass AS table_name,
    sr.srsubstate AS state,
    CASE sr.srsubstate
        WHEN 'i' THEN 'Initializing'
        WHEN 'd' THEN 'Data copy'
        WHEN 's' THEN 'Synchronized'
        WHEN 'r' THEN 'Ready'
        ELSE 'Unknown'
    END AS state_description,
    pg_size_pretty(pg_total_relation_size(sr.srrelid)) AS table_size
FROM pg_subscription s
JOIN pg_subscription_rel sr ON s.oid = sr.srsubid
WHERE s.subname = 'airbyte_subscription_sigmasz'
ORDER BY sr.srrelid::regclass::text;

-- Использование:
-- SELECT * FROM airbyte_sync.replication_status;

-- 4.2. Проверка lag репликации
CREATE OR REPLACE FUNCTION airbyte_sync.get_replication_lag()
RETURNS TABLE (
    subscription_name TEXT,
    slot_name TEXT,
    lag_bytes BIGINT,
    lag_pretty TEXT,
    lag_seconds NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.subname::TEXT,
        s.subslotname::TEXT,
        (pg_wal_lsn_diff(
            pg_current_wal_lsn(),
            (SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = s.subslotname)
        ))::BIGINT AS lag_bytes,
        pg_size_pretty((
            pg_wal_lsn_diff(
                pg_current_wal_lsn(),
                (SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = s.subslotname)
            )
        )::BIGINT) AS lag_pretty,
        ROUND(
            EXTRACT(EPOCH FROM (
                NOW() - (SELECT pg_stat_file('pg_wal/' || 
                    lpad(to_hex((SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = s.subslotname)::bigint), 16, '0'), 
                    'pg_wal'))::timestamp
                ))
            ), 2
        ) AS lag_seconds
    FROM pg_subscription s
    WHERE s.subname = 'airbyte_subscription_sigmasz';
END;
$$ LANGUAGE plpgsql;

-- Использование:
-- SELECT * FROM airbyte_sync.get_replication_lag();

-- 4.3. Управление подпиской

-- Временно отключить подписку
-- ALTER SUBSCRIPTION airbyte_subscription_sigmasz DISABLE;

-- Включить подписку
-- ALTER SUBSCRIPTION airbyte_subscription_sigmasz ENABLE;

-- Обновить список публикаций (если добавили новые таблицы)
-- ALTER SUBSCRIPTION airbyte_subscription_sigmasz REFRESH PUBLICATION;

-- Изменить параметры подключения
-- ALTER SUBSCRIPTION airbyte_subscription_sigmasz 
-- CONNECTION 'host=new_host port=5432 dbname=new_db user=replication_user password=new_password';

-- Пропустить транзакцию при ошибке (осторожно!)
-- ALTER SUBSCRIPTION airbyte_subscription_sigmasz SKIP (lsn = '0/1234567');

-- 4.4. Управление публикацией

-- Добавить таблицу в публикацию (на L2 сервере)
-- ALTER PUBLICATION airbyte_prod_sync_sigmasz ADD TABLE prod_sync.sigmasz_new_table;

-- Удалить таблицу из публикации
-- ALTER PUBLICATION airbyte_prod_sync_sigmasz DROP TABLE prod_sync.sigmasz_old_table;

-- После изменения публикации нужно обновить подписку:
-- ALTER SUBSCRIPTION airbyte_subscription_sigmasz REFRESH PUBLICATION;

-- ============================================
-- ЧАСТЬ 5: ОБРАБОТКА ОШИБОК И ВОССТАНОВЛЕНИЕ
-- ============================================

-- 5.1. Проверка ошибок репликации
CREATE OR REPLACE VIEW airbyte_sync.replication_errors AS
SELECT 
    s.subname AS subscription_name,
    s.subenabled AS enabled,
    s.subslotname AS slot_name,
    rs.slot_name AS replication_slot,
    rs.active AS slot_active,
    rs.restart_lsn,
    rs.confirmed_flush_lsn,
    CASE 
        WHEN rs.active = false THEN 'Slot inactive - check for errors'
        WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), rs.confirmed_flush_lsn) > 1073741824 THEN 'High lag detected (>1GB)'
        ELSE 'OK'
    END AS status
FROM pg_subscription s
LEFT JOIN pg_replication_slots rs ON s.subslotname = rs.slot_name
WHERE s.subname = 'airbyte_subscription_sigmasz';

-- Использование:
-- SELECT * FROM airbyte_sync.replication_errors;

-- 5.2. Функция для перезапуска подписки при ошибках
CREATE OR REPLACE FUNCTION airbyte_sync.restart_subscription()
RETURNS VOID AS $$
BEGIN
    -- Отключаем подписку
    ALTER SUBSCRIPTION airbyte_subscription_sigmasz DISABLE;
    
    -- Небольшая задержка
    PERFORM pg_sleep(2);
    
    -- Включаем подписку
    ALTER SUBSCRIPTION airbyte_subscription_sigmasz ENABLE;
    
    RAISE NOTICE 'Subscription airbyte_subscription_sigmasz restarted';
END;
$$ LANGUAGE plpgsql;

-- Использование:
-- SELECT airbyte_sync.restart_subscription();

-- 5.3. Проверка целостности данных
CREATE OR REPLACE FUNCTION airbyte_sync.check_data_integrity()
RETURNS TABLE (
    table_name TEXT,
    l2_count BIGINT,
    prod_count BIGINT,
    difference BIGINT,
    status TEXT
) AS $$
BEGIN
    -- Эта функция должна выполняться с доступом к обоим серверам
    -- Для упрощения показываем только структуру
    RETURN QUERY
    SELECT 
        'sigmasz_leads'::TEXT,
        (SELECT COUNT(*) FROM prod_sync.sigmasz_leads)::BIGINT,
        (SELECT COUNT(*) FROM airbyte_sync.sigmasz_leads)::BIGINT,
        ((SELECT COUNT(*) FROM prod_sync.sigmasz_leads) - 
         (SELECT COUNT(*) FROM airbyte_sync.sigmasz_leads))::BIGINT,
        CASE 
            WHEN (SELECT COUNT(*) FROM prod_sync.sigmasz_leads) = 
                 (SELECT COUNT(*) FROM airbyte_sync.sigmasz_leads) 
            THEN 'OK' 
            ELSE 'MISMATCH' 
        END;
    -- Добавьте аналогичные запросы для других таблиц
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- ЧАСТЬ 6: ДЛЯ ДРУГИХ ДОМЕНОВ (concepta, entrum)
-- ============================================

-- 6.1. Создание публикаций для других доменов (на L2 сервере)
/*
CREATE PUBLICATION airbyte_prod_sync_concepta FOR TABLE 
    prod_sync.concepta_leads,
    prod_sync.concepta_contacts,
    prod_sync.concepta_lead_contacts,
    prod_sync.concepta_contact_phones,
    prod_sync.concepta_contact_emails
WITH (publish = 'insert,update,delete');

CREATE PUBLICATION airbyte_prod_sync_entrum FOR TABLE 
    prod_sync.entrum_leads,
    prod_sync.entrum_contacts,
    prod_sync.entrum_lead_contacts,
    prod_sync.entrum_contact_phones,
    prod_sync.entrum_contact_emails
WITH (publish = 'insert,update,delete');
*/

-- 6.2. Создание подписок для других доменов (на Проде)
/*
CREATE SUBSCRIPTION airbyte_subscription_concepta
CONNECTION 'host=<analytics_server_host> port=5432 dbname=<analytics_db_name> user=replication_user password=strong_password_here'
PUBLICATION airbyte_prod_sync_concepta
WITH (
    copy_data = true,
    create_slot = true,
    enabled = true,
    slot_name = 'airbyte_slot_concepta'
);

CREATE SUBSCRIPTION airbyte_subscription_entrum
CONNECTION 'host=<analytics_server_host> port=5432 dbname=<analytics_db_name> user=replication_user password=strong_password_here'
PUBLICATION airbyte_prod_sync_entrum
WITH (
    copy_data = true,
    create_slot = true,
    enabled = true,
    slot_name = 'airbyte_slot_entrum'
);
*/

-- ============================================
-- ПРИМЕЧАНИЯ
-- ============================================

/*
ВАЖНО:
1. Замените <analytics_server_host>, <analytics_db_name> на реальные значения
2. Замените 'strong_password_here' на сильный пароль
3. Настройте pg_hba.conf для разрешения подключений
4. Перезапустите PostgreSQL после изменения postgresql.conf
5. Проверьте firewall правила между серверами
6. Мониторьте lag репликации регулярно
7. Настройте алерты на ошибки репликации
8. Регулярно проверяйте целостность данных

БЕЗОПАСНОСТЬ:
- Используйте отдельного пользователя для репликации
- Ограничьте права доступа только необходимыми таблицами
- Используйте SSL соединения для репликации (рекомендуется)
- Регулярно меняйте пароли

ПРОИЗВОДИТЕЛЬНОСТЬ:
- Настройте max_replication_slots и max_wal_senders правильно
- Мониторьте размер WAL файлов
- Настройте автоматическую очистку старых WAL файлов
- Используйте индексы на ключевых полях
*/
