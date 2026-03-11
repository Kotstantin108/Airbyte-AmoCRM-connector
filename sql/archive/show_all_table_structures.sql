-- ============================================
-- Вывод структуры всех таблиц (схемы airbyte_raw, prod_sync, analytics)
-- ============================================
-- Выполните запрос 1, сохраните результат в файл (например structure_export.txt)
-- и пришлите его — по нему сверю план (id, name, type, _airbyte_meta, _airbyte_data и т.д.).

-- Запрос 1: полная структура (схема, таблица, №, колонка, тип)
SELECT
    c.table_schema,
    c.table_name,
    c.ordinal_position AS n,
    c.column_name,
    c.data_type,
    c.udt_name
FROM information_schema.columns c
WHERE c.table_schema IN ('airbyte_raw', 'prod_sync', 'analytics')
  AND (c.table_name LIKE 'sigmasz%' OR c.table_name LIKE 'airbyte%')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;

-- Запрос 2 (опционально): сводка — одна строка на таблицу, колонки через запятую
/*
SELECT
    table_schema,
    table_name,
    string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema IN ('airbyte_raw', 'prod_sync', 'analytics')
  AND (table_name LIKE 'sigmasz%' OR table_name LIKE 'airbyte%')
GROUP BY table_schema, table_name
ORDER BY table_schema, table_name;
*/
