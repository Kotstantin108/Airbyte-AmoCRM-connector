-- ============================================
-- Супер-админ с полными правами (всё видеть и править)
-- ============================================
-- Замените:
--   my_admin — имя пользователя
--   'your_password_here' — пароль
--   my_database — имя базы

-- 1. Создать пользователя с правами суперпользователя (выполнять от postgres)
-- ВАРИАНТ А: Полный суперпользователь (может всё, включая создание БД, пользователей и т.д.)
CREATE USER my_admin WITH PASSWORD 'your_password_here' SUPERUSER CREATEDB CREATEROLE LOGIN;

-- ВАРИАНТ Б: Обычный пользователь с полными правами только на эту базу (без SUPERUSER)
-- CREATE USER my_admin WITH PASSWORD 'your_password_here' LOGIN;
-- GRANT CONNECT ON DATABASE my_database TO my_admin;
-- GRANT ALL PRIVILEGES ON DATABASE my_database TO my_admin;

-- Дальше выполнять, подключившись к базе (psql -d my_database и т.п.)

-- 2. Полные права на все схемы и таблицы (вариант Б — если не SUPERUSER)
-- Если выбрали вариант А (SUPERUSER), этот блок не нужен — суперпользователь уже может всё.

-- Вариант Б1: Только нужные схемы (airbyte_raw, prod_sync, analytics, public)
/*
GRANT ALL PRIVILEGES ON SCHEMA airbyte_raw TO my_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA airbyte_raw TO my_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA airbyte_raw TO my_admin;

GRANT ALL PRIVILEGES ON SCHEMA prod_sync TO my_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA prod_sync TO my_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA prod_sync TO my_admin;

GRANT ALL PRIVILEGES ON SCHEMA analytics TO my_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO my_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics TO my_admin;

GRANT ALL PRIVILEGES ON SCHEMA public TO my_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO my_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO my_admin;

-- Будущие таблицы тоже будут доступны
ALTER DEFAULT PRIVILEGES IN SCHEMA airbyte_raw GRANT ALL PRIVILEGES ON TABLES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA airbyte_raw GRANT ALL PRIVILEGES ON SEQUENCES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod_sync GRANT ALL PRIVILEGES ON TABLES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA prod_sync GRANT ALL PRIVILEGES ON SEQUENCES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL PRIVILEGES ON TABLES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL PRIVILEGES ON SEQUENCES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO my_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO my_admin;
*/

-- Вариант Б2: Все пользовательские схемы в базе (если не SUPERUSER)
/*
DO $$
DECLARE
    sch TEXT;
BEGIN
    FOR sch IN
        SELECT nspname FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%' AND nspname != 'information_schema'
    LOOP
        EXECUTE format('GRANT ALL PRIVILEGES ON SCHEMA %I TO my_admin', sch);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO my_admin', sch);
        EXECUTE format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO my_admin', sch);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL PRIVILEGES ON TABLES TO my_admin', sch);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL PRIVILEGES ON SEQUENCES TO my_admin', sch);
    END LOOP;
END;
$$;
*/

-- ============================================
-- Рекомендация: использовать ВАРИАНТ А (SUPERUSER)
-- ============================================
-- Суперпользователь может:
--   - Читать и изменять любые данные
--   - Создавать/удалять таблицы, схемы, функции
--   - Создавать других пользователей
--   - Делать всё, что нужно для администрирования
--
-- Подключение: psql -h host -U my_admin -d my_database
