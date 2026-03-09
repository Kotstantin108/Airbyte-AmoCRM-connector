-- ============================================================================
-- Пользователь data_lens_user: READ-ONLY доступ к данным Analytics-сервера
-- Схемы: L1 (airbyte_raw), L2 (prod_sync), L3 (analytics)
-- Исключение: таблица public.amo_tokens (содержит OAuth-секреты)
-- ============================================================================
--
-- Выполняется на ANALYTICS-сервере от суперпользователя (postgres).
-- Подставьте:
--   'ЗАМЕНИТЕ_НА_НАДЁЖНЫЙ_ПАРОЛЬ' — пароль для DataLens
--   my_database — имя базы данных на analytics-сервере
--
-- Новые домены (таблицы/views) подхватываются автоматически
-- через ALTER DEFAULT PRIVILEGES.
-- ============================================================================

-- 1. Создаём пользователя
CREATE USER data_lens_user WITH PASSWORD 'ЗАМЕНИТЕ_НА_НАДЁЖНЫЙ_ПАРОЛЬ' LOGIN;

-- 2. Разрешаем подключение к базе
GRANT CONNECT ON DATABASE my_database TO data_lens_user;

-- ============================================================================
-- 3. Доступ к схемам L1 / L2 / L3
-- ============================================================================

-- L1: airbyte_raw (сырые данные Airbyte)
GRANT USAGE ON SCHEMA airbyte_raw TO data_lens_user;
GRANT SELECT ON ALL TABLES IN SCHEMA airbyte_raw TO data_lens_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA airbyte_raw GRANT SELECT ON TABLES TO data_lens_user;

-- L2: prod_sync (нормализованные данные)
GRANT USAGE ON SCHEMA prod_sync TO data_lens_user;
GRANT SELECT ON ALL TABLES IN SCHEMA prod_sync TO data_lens_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod_sync GRANT SELECT ON TABLES TO data_lens_user;

-- L3: analytics (аналитические таблицы и представления)
GRANT USAGE ON SCHEMA analytics TO data_lens_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO data_lens_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO data_lens_user;

-- ============================================================================
-- 4. Схема public: всё кроме amo_tokens
-- ============================================================================
GRANT USAGE ON SCHEMA public TO data_lens_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO data_lens_user;
REVOKE ALL ON TABLE public.amo_tokens FROM data_lens_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO data_lens_user;

-- Event trigger: если amo_tokens будет пересоздана — автоматический REVOKE
CREATE OR REPLACE FUNCTION public.revoke_datalens_from_tokens()
RETURNS event_trigger AS $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.object_type = 'table' AND obj.object_identity = 'public.amo_tokens' THEN
            EXECUTE 'REVOKE ALL ON TABLE public.amo_tokens FROM data_lens_user';
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER trg_revoke_datalens_tokens
    ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE FUNCTION public.revoke_datalens_from_tokens();

-- ============================================================================
-- Проверка (запустить от data_lens_user):
--   SELECT schemaname, tablename FROM pg_tables
--   WHERE has_table_privilege('data_lens_user', schemaname||'.'||tablename, 'SELECT')
--   ORDER BY schemaname, tablename;
-- Убедитесь: public.amo_tokens ОТСУТСТВУЕТ в списке.
-- ============================================================================
