-- =============================================================================
-- 1. ДИАГНОСТИКА: Найти контакты с PHONE/EMAIL в raw_json но без записей в таблицах
-- =============================================================================

-- Контакты с телефонами в raw_json но без строк в contact_phones
SELECT
    c.contact_id,
    c.name,
    c.raw_json -> 'custom_fields_values' AS cf_values,
    (SELECT count(*) FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id) AS phones_count,
    (SELECT count(*) FROM prod_sync.sigmasz_contact_emails ce WHERE ce.contact_id = c.contact_id) AS emails_count
FROM prod_sync.sigmasz_contacts c
WHERE COALESCE(c.is_deleted, FALSE) IS FALSE
  AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(c.raw_json -> 'custom_fields_values', '[]'::JSONB)) cf
    WHERE cf ->> 'field_code' = 'PHONE'
  )
  AND NOT EXISTS (
    SELECT 1 FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id
  )
LIMIT 50;

-- Общая статистика
SELECT
    'contacts_total' AS metric,
    count(*) AS cnt
FROM prod_sync.sigmasz_contacts
WHERE COALESCE(is_deleted, FALSE) IS FALSE
UNION ALL
SELECT
    'has_phone_in_json_but_not_in_table',
    count(*)
FROM prod_sync.sigmasz_contacts c
WHERE COALESCE(c.is_deleted, FALSE) IS FALSE
  AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(c.raw_json -> 'custom_fields_values', '[]'::JSONB)) cf
    WHERE cf ->> 'field_code' = 'PHONE'
  )
  AND NOT EXISTS (
    SELECT 1 FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id
  )
UNION ALL
SELECT
    'has_email_in_json_but_not_in_table',
    count(*)
FROM prod_sync.sigmasz_contacts c
WHERE COALESCE(c.is_deleted, FALSE) IS FALSE
  AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(COALESCE(c.raw_json -> 'custom_fields_values', '[]'::JSONB)) cf
    WHERE cf ->> 'field_code' = 'EMAIL'
  )
  AND NOT EXISTS (
    SELECT 1 FROM prod_sync.sigmasz_contact_emails ce WHERE ce.contact_id = c.contact_id
  );

-- =============================================================================
-- 2. БЭКФИЛ: Восстановить телефоны и email из raw_json
-- =============================================================================

-- 2a. Бэкфил телефонов
INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone)
SELECT DISTINCT
    c.contact_id,
    public.normalize_phone(v.value ->> 'value')
FROM prod_sync.sigmasz_contacts c
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.raw_json -> 'custom_fields_values', '[]'::JSONB)) cf
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(cf -> 'values') = 'array' THEN cf -> 'values' ELSE '[]'::JSONB END
) v
WHERE COALESCE(c.is_deleted, FALSE) IS FALSE
  AND cf ->> 'field_code' = 'PHONE'
  AND public.normalize_phone(v.value ->> 'value') IS NOT NULL
  -- Только для контактов, у которых нет записей в таблице телефонов
  AND NOT EXISTS (
    SELECT 1 FROM prod_sync.sigmasz_contact_phones cp WHERE cp.contact_id = c.contact_id
  )
ON CONFLICT DO NOTHING;

-- 2b. Бэкфил email
INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email)
SELECT DISTINCT
    c.contact_id,
    lower(BTRIM(v.value ->> 'value'))
FROM prod_sync.sigmasz_contacts c
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.raw_json -> 'custom_fields_values', '[]'::JSONB)) cf
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(cf -> 'values') = 'array' THEN cf -> 'values' ELSE '[]'::JSONB END
) v
WHERE COALESCE(c.is_deleted, FALSE) IS FALSE
  AND cf ->> 'field_code' = 'EMAIL'
  AND NULLIF(BTRIM(v.value ->> 'value'), '') IS NOT NULL
  -- Только для контактов, у которых нет записей в таблице email
  AND NOT EXISTS (
    SELECT 1 FROM prod_sync.sigmasz_contact_emails ce WHERE ce.contact_id = c.contact_id
  )
ON CONFLICT DO NOTHING;
