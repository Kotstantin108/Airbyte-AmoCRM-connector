-- ============================================
-- Пользователь только для просмотра данных (SELECT)
-- ============================================
-- Замените:
--   my_readonly_user — имя пользователя
--   'your_password_here' — пароль
--   my_database — имя вашей базы (для GRANT CONNECT)

-- 1. Создать пользователя (выполнять от суперпользователя, например postgres)
CREATE USER my_readonly_user WITH PASSWORD 'your_password_here' LOGIN;

-- 2. Разрешить подключение к базе (подставьте имя базы)
GRANT CONNECT ON DATABASE my_database TO my_readonly_user;

-- Дальше выполнять, подключившись к этой базе (psql -d my_database и т.п.)

-- 3. Вариант А: только нужные схемы (airbyte_raw, prod_sync, analytics, public)
GRANT USAGE ON SCHEMA airbyte_raw TO my_readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA airbyte_raw TO my_readonly_user;

GRANT USAGE ON SCHEMA prod_sync TO my_readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA prod_sync TO my_readonly_user;

GRANT USAGE ON SCHEMA analytics TO my_readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO my_readonly_user;

GRANT USAGE ON SCHEMA public TO my_readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO my_readonly_user;

-- Будущие таблицы в этих схемах тоже будут доступны на чтение
ALTER DEFAULT PRIVILEGES IN SCHEMA airbyte_raw GRANT SELECT ON TABLES TO my_readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod_sync GRANT SELECT ON TABLES TO my_readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO my_readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO my_readonly_user;


-- ============================================
-- Вариант Б: дать SELECT по всем схемам в базе (кроме system)
-- ============================================
-- Раскомментируйте и выполните вместо блока 3, если нужен доступ ко всем данным в БД:

/*
DO $$
DECLARE
    sch TEXT;
BEGIN
    FOR sch IN
        SELECT nspname FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'
    LOOP
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO my_readonly_user', sch);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO my_readonly_user', sch);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO my_readonly_user', sch);
    END LOOP;
END;
$$;
*/

-- Подключение: psql -h host -U my_readonly_user -d my_database
