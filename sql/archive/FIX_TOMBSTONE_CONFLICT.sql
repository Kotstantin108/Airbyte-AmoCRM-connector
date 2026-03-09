-- =============================================================================
-- FIX TOMBSTONE SHIELD: Resolve register_tombstone() conflict
-- + PHYSICAL DELETION in L2 (prod_sync) for all domains
-- =============================================================================
-- Problem: dwh_sync_l1_l2_l3.sql overwrote register_tombstone() with wrong signature
-- Solution: Re-apply correct 3-param version + implement physical deletion in L2
-- Domains: sigmasz, concepta, entrum
-- =============================================================================

-- =============================================================================
-- STEP 1: Restore correct function signatures
-- =============================================================================

-- Drop old (broken) 2-parameter version if it exists
DROP FUNCTION IF EXISTS prod_sync.register_tombstone(TEXT, BIGINT) CASCADE;
DROP FUNCTION IF EXISTS prod_sync.is_tombstoned(TEXT, BIGINT) CASCADE;

-- Create correct 3-parameter versions
CREATE OR REPLACE FUNCTION prod_sync.register_tombstone(
    p_domain TEXT, 
    p_entity_type TEXT, 
    p_entity_id BIGINT
)
RETURNS VOID 
LANGUAGE plpgsql 
SECURITY DEFINER 
AS $$
BEGIN
    INSERT INTO prod_sync.deleted_entities_log 
        (domain, entity_type, entity_id, deleted_at, _synced_at)
    VALUES 
        (p_domain, p_entity_type, p_entity_id, NOW(), NOW())
    ON CONFLICT (domain, entity_type, entity_id) DO UPDATE 
        SET deleted_at = NOW(), _synced_at = NOW();
END;
$$;

-- Overloaded version with explicit deleted_at timestamp
CREATE OR REPLACE FUNCTION prod_sync.register_tombstone(
    p_domain TEXT, 
    p_entity_type TEXT, 
    p_entity_id BIGINT,
    p_deleted_at TIMESTAMPTZ
)
RETURNS VOID 
LANGUAGE plpgsql 
SECURITY DEFINER 
AS $$
BEGIN
    INSERT INTO prod_sync.deleted_entities_log 
        (domain, entity_type, entity_id, deleted_at, _synced_at)
    VALUES 
        (p_domain, p_entity_type, p_entity_id, p_deleted_at, NOW())
    ON CONFLICT (domain, entity_type, entity_id) DO UPDATE 
        SET deleted_at = GREATEST(EXCLUDED.deleted_at, deleted_entities_log.deleted_at),  -- Keep earliest deletion time
            _synced_at = NOW();
END;
$$;

CREATE OR REPLACE FUNCTION prod_sync.is_tombstoned(
    p_domain TEXT, 
    p_entity_type TEXT, 
    p_entity_id BIGINT
)
RETURNS BOOLEAN 
LANGUAGE sql 
STABLE 
SECURITY DEFINER 
AS $$
    SELECT EXISTS (
        SELECT 1 
        FROM prod_sync.deleted_entities_log 
        WHERE domain = p_domain 
          AND entity_type = p_entity_type 
          AND entity_id = p_entity_id
    );
$$;

-- Functions restored with 3-parameter signatures

-- =============================================================================
-- STEP 2: Clean up deleted_entities_log before re-population
-- =============================================================================
-- Remove any corrupted/incomplete records from previous attempts
TRUNCATE TABLE prod_sync.deleted_entities_log;

-- =============================================================================
-- STEP 3: Recover deleted entities from all domains' events tables
-- Using created_at from event (Unix timestamp) as deleted_at, not NOW()
-- Taking LATEST deletion event per entity_id
-- =============================================================================

-- SIGMASZ domain
INSERT INTO prod_sync.deleted_entities_log 
    (domain, entity_type, entity_id, deleted_at, _synced_at)
SELECT 
    'sigmasz',
    entity_type,
    entity_id,
    to_timestamp(MAX(created_at)::BIGINT)::TIMESTAMPTZ,  -- ✅ Latest deletion timestamp
    NOW()
FROM airbyte_raw.sigmasz_events
WHERE (type = 'lead_deleted' AND entity_type = 'lead')
   OR (type = 'contact_deleted' AND entity_type = 'contact')
GROUP BY entity_type, entity_id
ON CONFLICT (domain, entity_type, entity_id) DO UPDATE
    SET deleted_at = EXCLUDED.deleted_at,  -- ✅ Keep original deletion time
        _synced_at = NOW();

-- CONCEPTA domain
INSERT INTO prod_sync.deleted_entities_log 
    (domain, entity_type, entity_id, deleted_at, _synced_at)
SELECT 
    'concepta',
    entity_type,
    entity_id,
    to_timestamp(MAX(created_at)::BIGINT)::TIMESTAMPTZ,  -- ✅ Latest deletion timestamp
    NOW()
FROM airbyte_raw.concepta_events
WHERE (type = 'lead_deleted' AND entity_type = 'lead')
   OR (type = 'contact_deleted' AND entity_type = 'contact')
GROUP BY entity_type, entity_id
ON CONFLICT (domain, entity_type, entity_id) DO UPDATE
    SET deleted_at = EXCLUDED.deleted_at,  -- ✅ Keep original deletion time
        _synced_at = NOW();

-- ENTRUM domain
INSERT INTO prod_sync.deleted_entities_log 
    (domain, entity_type, entity_id, deleted_at, _synced_at)
SELECT 
    'entrum',
    entity_type,
    entity_id,
    to_timestamp(MAX(created_at)::BIGINT)::TIMESTAMPTZ,  -- ✅ Latest deletion timestamp
    NOW()
FROM airbyte_raw.entrum_events
WHERE (type = 'lead_deleted' AND entity_type = 'lead')
   OR (type = 'contact_deleted' AND entity_type = 'contact')
GROUP BY entity_type, entity_id
ON CONFLICT (domain, entity_type, entity_id) DO UPDATE
    SET deleted_at = EXCLUDED.deleted_at,  -- ✅ Keep original deletion time
        _synced_at = NOW();

-- Verify inserts
SELECT 
    domain,
    entity_type,
    COUNT(*) as deleted_count
FROM prod_sync.deleted_entities_log
GROUP BY domain, entity_type
ORDER BY domain, entity_type;

-- =============================================================================
-- STEP 4: PHYSICAL DELETION in L2 (prod_sync) - CASCADE for all related tables
-- =============================================================================

-- SIGMASZ: Delete lead with all related data
DELETE FROM prod_sync.sigmasz_lead_contacts 
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'lead'
);

DELETE FROM prod_sync.sigmasz_leads 
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'lead'
);

-- SIGMASZ: Delete contact with all related data
DELETE FROM prod_sync.sigmasz_contact_phones 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'contact'
);

DELETE FROM prod_sync.sigmasz_contact_emails 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'contact'
);

DELETE FROM prod_sync.sigmasz_lead_contacts 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'contact'
);

DELETE FROM prod_sync.sigmasz_contacts 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'contact'
);

-- CONCEPTA: Delete lead with all related data
DELETE FROM prod_sync.concepta_lead_contacts 
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'lead'
);

DELETE FROM prod_sync.concepta_leads 
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'lead'
);

-- CONCEPTA: Delete contact with all related data
DELETE FROM prod_sync.concepta_contact_phones 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'contact'
);

DELETE FROM prod_sync.concepta_contact_emails 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'contact'
);

DELETE FROM prod_sync.concepta_lead_contacts 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'contact'
);

DELETE FROM prod_sync.concepta_contacts 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'contact'
);

-- ENTRUM: Delete lead with all related data
DELETE FROM prod_sync.entrum_lead_contacts 
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'lead'
);

DELETE FROM prod_sync.entrum_leads 
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'lead'
);

-- ENTRUM: Delete contact with all related data
DELETE FROM prod_sync.entrum_contact_phones 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'contact'
);

DELETE FROM prod_sync.entrum_contact_emails 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'contact'
);

DELETE FROM prod_sync.entrum_lead_contacts 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'contact'
);

DELETE FROM prod_sync.entrum_contacts 
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'contact'
);

-- L1 soft deletion complete for all domains

-- =============================================================================
-- STEP 5: Update L1 (airbyte_raw) - Mark with is_deleted=TRUE (soft delete only)
-- =============================================================================

-- SIGMASZ
UPDATE airbyte_raw.sigmasz_leads 
SET is_deleted = TRUE 
WHERE id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'lead'
)
AND COALESCE(is_deleted, FALSE) IS FALSE;

UPDATE airbyte_raw.sigmasz_contacts 
SET is_deleted = TRUE 
WHERE id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'contact'
)
AND COALESCE(is_deleted, FALSE) IS FALSE;

-- CONCEPTA
UPDATE airbyte_raw.concepta_leads 
SET is_deleted = TRUE 
WHERE id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'lead'
)
AND COALESCE(is_deleted, FALSE) IS FALSE;

UPDATE airbyte_raw.concepta_contacts 
SET is_deleted = TRUE 
WHERE id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'concepta' AND entity_type = 'contact'
)
AND COALESCE(is_deleted, FALSE) IS FALSE;

-- ENTRUM
UPDATE airbyte_raw.entrum_leads 
SET is_deleted = TRUE 
WHERE id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'lead'
)
AND COALESCE(is_deleted, FALSE) IS FALSE;

UPDATE airbyte_raw.entrum_contacts 
SET is_deleted = TRUE 
WHERE id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'entrum' AND entity_type = 'contact'
)
AND COALESCE(is_deleted, FALSE) IS FALSE;

-- L1 soft deletion complete for all domains

-- =============================================================================
-- POST-FIX VERIFICATION QUERIES
-- =============================================================================
-- Run these manually to verify:

-- L1 soft deletion complete for all domains

-- =============================================================================
-- POST-FIX VERIFICATION QUERIES
-- =============================================================================
-- Run these manually to verify:

-- 1. Check function signatures (should show 3-param versions)
-- SELECT proname, oid::regprocedure FROM pg_proc 
-- WHERE proname IN ('register_tombstone', 'is_tombstoned')
-- ORDER BY proname;

-- 2. Check deleted_entities_log content
-- SELECT domain, entity_type, COUNT(*) as deleted_count
-- FROM prod_sync.deleted_entities_log
-- GROUP BY domain, entity_type
-- ORDER BY domain, entity_type;

-- 3. Verify L2 physical deletion (should be empty or much smaller)
-- SELECT 
--     'sigmasz_leads' as table_name,
--     COUNT(*) as remaining_records
-- FROM prod_sync.sigmasz_leads
-- UNION ALL
-- SELECT 'concepta_leads', COUNT(*) FROM prod_sync.concepta_leads
-- UNION ALL
-- SELECT 'entrum_leads', COUNT(*) FROM prod_sync.entrum_leads
-- UNION ALL
-- SELECT 'sigmasz_contacts', COUNT(*) FROM prod_sync.sigmasz_contacts
-- UNION ALL
-- SELECT 'concepta_contacts', COUNT(*) FROM prod_sync.concepta_contacts
-- UNION ALL
-- SELECT 'entrum_contacts', COUNT(*) FROM prod_sync.entrum_contacts;

-- 4. Verify L1 soft deletion (is_deleted=TRUE)
-- SELECT 
--     'sigmasz_leads' as table_name,
--     COUNT(*) as total,
--     SUM(CASE WHEN is_deleted = TRUE THEN 1 ELSE 0 END) as marked_deleted
-- FROM airbyte_raw.sigmasz_leads
-- UNION ALL
-- SELECT 'concepta_leads', COUNT(*), SUM(CASE WHEN is_deleted = TRUE THEN 1 ELSE 0 END)
-- FROM airbyte_raw.concepta_leads
-- UNION ALL
-- SELECT 'entrum_leads', COUNT(*), SUM(CASE WHEN is_deleted = TRUE THEN 1 ELSE 0 END)
-- FROM airbyte_raw.entrum_leads;

-- =============================================================================
