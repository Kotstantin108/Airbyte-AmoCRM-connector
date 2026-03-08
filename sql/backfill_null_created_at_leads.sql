-- =============================================================================
-- BACKFILL: Заполнение NULL created_at в таблицах лидов L2
-- =============================================================================
-- Этот скрипт находит все лиды в L2, у которых created_at = NULL,
-- и пытается восстановить дату создания из L1 (airbyte_raw),
-- беря самую раннюю валидную дату (отбрасывая нули и мусор).
--
-- Обрабатывает домены: sigmasz, concepta, entrum
-- =============================================================================

BEGIN;

UPDATE prod_sync.sigmasz_leads p
SET 
    created_at = to_timestamp((p.raw_json->>'created_at')::DOUBLE PRECISION),
    _synced_at = NOW()
WHERE p.created_at IS NULL
  AND p.raw_json->>'created_at' IS NOT NULL;

UPDATE prod_sync.concepta_leads p
SET 
    created_at = to_timestamp((p.raw_json->>'created_at')::DOUBLE PRECISION),
    _synced_at = NOW()
WHERE p.created_at IS NULL
  AND p.raw_json->>'created_at' IS NOT NULL;

UPDATE prod_sync.entrum_leads p
SET 
    created_at = to_timestamp((p.raw_json->>'created_at')::DOUBLE PRECISION),
    _synced_at = NOW()
WHERE p.created_at IS NULL
  AND p.raw_json->>'created_at' IS NOT NULL;

COMMIT;

-- Вывод результатов проверки оставшихся NULL:
-- SELECT 'sigmasz' AS domain, COUNT(*) FROM prod_sync.sigmasz_leads WHERE created_at IS NULL
-- UNION ALL
-- SELECT 'concepta' AS domain, COUNT(*) FROM prod_sync.concepta_leads WHERE created_at IS NULL
-- UNION ALL
-- SELECT 'entrum' AS domain, COUNT(*) FROM prod_sync.entrum_leads WHERE created_at IS NULL;
