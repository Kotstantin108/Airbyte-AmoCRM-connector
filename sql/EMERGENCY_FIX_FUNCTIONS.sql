-- =============================================================================
-- EMERGENCY FIX: Ensure correct function signatures exist
-- =============================================================================
-- This script MUST be run BEFORE dwh_sync_l1_l2_l3.sql

-- Step 1: Drop ALL versions of these functions (2-param, 3-param, any other)
DROP FUNCTION IF EXISTS prod_sync.register_tombstone(TEXT, BIGINT) CASCADE;
DROP FUNCTION IF EXISTS prod_sync.register_tombstone(TEXT, TEXT, BIGINT) CASCADE;
DROP FUNCTION IF EXISTS prod_sync.is_tombstoned(TEXT, BIGINT) CASCADE;
DROP FUNCTION IF EXISTS prod_sync.is_tombstoned(TEXT, TEXT, BIGINT) CASCADE;

-- Step 2: Create ONLY the correct 3-parameter versions
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

-- Step 3: Verify
SELECT proname, oid::regprocedure 
FROM pg_proc 
WHERE proname IN ('register_tombstone', 'is_tombstoned')
ORDER BY proname;

-- Expected output:
-- register_tombstone | register_tombstone(text,text,bigint)
-- is_tombstoned      | is_tombstoned(text,text,bigint)
