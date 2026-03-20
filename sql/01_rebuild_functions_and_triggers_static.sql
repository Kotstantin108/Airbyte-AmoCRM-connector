-- =============================================================================
-- L1 → L2 FUNCTIONS AND TRIGGERS (CANONICAL — ALL DOMAINS)
-- Domains: sigmasz, concepta, entrum
-- Layers:  airbyte_raw (L1) → prod_sync (L2)
-- Pattern: Tombstone Shield + Dead Letter Queue + Domain Registry
-- =============================================================================
--
-- EXECUTION ORDER (fresh install):
--   1. sql/00_bootstrap_schemas_and_tables.sql  — schemas + base tables
--   2. sql/01_rebuild_functions_and_triggers_static.sql  ← THIS FILE
--   3. sql/02_prod_fdw_sync.sql                 — on the PROD server
--
-- Safe to re-run (idempotent): all objects use CREATE OR REPLACE / IF NOT EXISTS.
-- Requires: at least one successful Airbyte sync (airbyte_raw schema + tables must exist).
-- =============================================================================

BEGIN;

-- =============================================================================
-- BLOCK 0: INFRASTRUCTURE — Tombstone Shield + Dead Letter Queue
-- =============================================================================

-- Tombstone Shield: single source of truth for deleted entities per domain
CREATE TABLE IF NOT EXISTS prod_sync.deleted_entities_log (
    domain       TEXT         NOT NULL DEFAULT 'sigmasz',
    entity_type  VARCHAR(50)  NOT NULL,
    entity_id    BIGINT       NOT NULL,
    deleted_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    _synced_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (domain, entity_type, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_deleted_entities_log_domain
    ON prod_sync.deleted_entities_log (domain, entity_type);
COMMENT ON TABLE prod_sync.deleted_entities_log IS
    'Tombstone Shield. Единственный источник истины об удалённых сущностях. '
    'Проверяется перед любым UPSERT в L2. Никогда не очищается автоматически.';
COMMENT ON COLUMN prod_sync.deleted_entities_log.domain IS
    'Субдомен AmoCRM-источника. Изолирует Shield между доменами.';

-- Dead Letter Queue: quarantine for bad rows from L1
CREATE TABLE IF NOT EXISTS airbyte_raw.l2_dead_letter_queue (
    id            BIGSERIAL    PRIMARY KEY,
    stream_name   TEXT         NOT NULL,
    entity_id     BIGINT,
    raw_record    JSONB,
    error_message TEXT         NOT NULL,
    sqlstate      TEXT,
    failed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    retry_count   INT          NOT NULL DEFAULT 0,
    resolved      BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at   TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dlq_active_entity
    ON airbyte_raw.l2_dead_letter_queue (stream_name, entity_id)
    WHERE resolved = FALSE;
CREATE INDEX IF NOT EXISTS idx_dlq_unresolved
    ON airbyte_raw.l2_dead_letter_queue (stream_name, failed_at)
    WHERE resolved = FALSE;
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at_unresolved
    ON airbyte_raw.l2_dead_letter_queue (failed_at DESC)
    WHERE resolved = FALSE;
COMMENT ON TABLE airbyte_raw.l2_dead_letter_queue IS
    'Dead Letter Queue для строк L1/L2, которые вызвали ошибку. '
    'resolved=FALSE — требует ручного разбора.';

-- Domain Registry
CREATE TABLE IF NOT EXISTS prod_sync.domain_registry (
    domain         TEXT PRIMARY KEY,
    provisioned_at TIMESTAMPTZ DEFAULT NOW(),
    is_active      BOOLEAN     DEFAULT TRUE
);
INSERT INTO prod_sync.domain_registry (domain) VALUES ('sigmasz')  ON CONFLICT DO NOTHING;
INSERT INTO prod_sync.domain_registry (domain) VALUES ('concepta') ON CONFLICT DO NOTHING;
INSERT INTO prod_sync.domain_registry (domain) VALUES ('entrum')   ON CONFLICT DO NOTHING;


-- =============================================================================
-- BLOCK 1: TOMBSTONE SHIELD — HELPER FUNCTIONS
-- =============================================================================

DROP FUNCTION IF EXISTS prod_sync.register_tombstone(TEXT, BIGINT) CASCADE;
CREATE OR REPLACE FUNCTION prod_sync.register_tombstone(
    p_domain      TEXT,
    p_entity_type TEXT,
    p_entity_id   BIGINT
)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    INSERT INTO prod_sync.deleted_entities_log (domain, entity_type, entity_id, deleted_at, _synced_at)
    VALUES (p_domain, p_entity_type, p_entity_id, NOW(), NOW())
    ON CONFLICT (domain, entity_type, entity_id) DO UPDATE SET
        deleted_at = NOW(),
        _synced_at = NOW();
END;
$$;

DROP FUNCTION IF EXISTS prod_sync.is_tombstoned(TEXT, BIGINT) CASCADE;
CREATE OR REPLACE FUNCTION prod_sync.is_tombstoned(
    p_domain      TEXT,
    p_entity_type TEXT,
    p_entity_id   BIGINT
)
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER AS $$
    SELECT EXISTS (
        SELECT 1 FROM prod_sync.deleted_entities_log
        WHERE domain = p_domain
          AND entity_type = p_entity_type
          AND entity_id = p_entity_id
    );
$$;

CREATE OR REPLACE VIEW prod_sync.v_recent_tombstones AS
SELECT domain, entity_type, entity_id, deleted_at, _synced_at
FROM prod_sync.deleted_entities_log
ORDER BY deleted_at DESC
LIMIT 100;


-- =============================================================================
-- BLOCK 2: UTILITY FUNCTIONS
-- =============================================================================

-- Нормализация телефона: только цифры, 8XXXXXXXXXX → 7XXXXXXXXXX
DROP FUNCTION IF EXISTS public.normalize_phone(text) CASCADE;
CREATE FUNCTION public.normalize_phone(p_raw text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    v_clean text;
BEGIN
    IF p_raw IS NULL THEN RETURN NULL; END IF;
    v_clean := regexp_replace(p_raw, '\D', '', 'g');
    IF v_clean = '' THEN RETURN NULL; END IF;
    IF length(v_clean) = 11 AND left(v_clean, 1) = '8' THEN
        v_clean := '7' || substr(v_clean, 2);
    END IF;
    RETURN v_clean;
END;
$$;

-- Безопасное преобразование Unix timestamp (сек или мс) → TIMESTAMPTZ
CREATE OR REPLACE FUNCTION prod_sync.safe_cf_to_timestamp(val TEXT)
RETURNS TIMESTAMPTZ LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    v_big BIGINT;
    v_sec BIGINT;
BEGIN
    IF val IS NULL THEN RETURN NULL; END IF;
    val := BTRIM(val);
    IF val = '' OR val !~ '^\d+$' THEN RETURN NULL; END IF;
    IF length(val) > 14 THEN RETURN NULL; END IF;
    BEGIN
        v_big := val::BIGINT;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
    IF length(val) = 13 THEN
        v_sec := v_big / 1000;
    ELSIF length(val) BETWEEN 1 AND 12 THEN
        v_sec := v_big;
    ELSE
        RETURN NULL;
    END IF;
    RETURN to_timestamp(v_sec::DOUBLE PRECISION);
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;


-- =============================================================================
-- BLOCK 3: SIGMASZ — L1 → L2 TRIGGER FUNCTIONS
-- =============================================================================

-- 3.1 Tombstone writer for sigmasz events
CREATE OR REPLACE FUNCTION airbyte_raw.propagate_deleted_to_l2()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_sigmasz'));
    IF NEW.type = 'lead_deleted' AND NEW.entity_type = 'lead' THEN
        PERFORM prod_sync.register_tombstone('sigmasz', 'lead', NEW.entity_id);
        UPDATE prod_sync.sigmasz_leads SET is_deleted = TRUE, _synced_at = NOW() WHERE lead_id = NEW.entity_id;
    END IF;
    IF NEW.type = 'contact_deleted' AND NEW.entity_type = 'contact' THEN
        PERFORM prod_sync.register_tombstone('sigmasz', 'contact', NEW.entity_id);
        UPDATE prod_sync.sigmasz_contacts SET is_deleted = TRUE, _synced_at = NOW() WHERE contact_id = NEW.entity_id;
    END IF;
    RETURN NEW;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '[propagate_deleted_to_l2] entity=% id=% error=%', NEW.entity_type, NEW.entity_id, SQLERRM;
    RETURN NEW;
END;
$$;

-- 3.2 Utility: process embedded contacts for sigmasz leads
CREATE OR REPLACE FUNCTION prod_sync.process_embedded_contacts(
    p_contacts_json  JSONB,
    p_lead_id        BIGINT,
    p_explicit_empty BOOLEAN DEFAULT FALSE
)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_c    JSONB;
    v_cid  BIGINT;
    v_ts   TIMESTAMPTZ;
    v_main BOOLEAN;
    v_ids  BIGINT[] := ARRAY[]::BIGINT[];
BEGIN
    IF p_explicit_empty THEN
        DELETE FROM prod_sync.sigmasz_lead_contacts WHERE lead_id = p_lead_id;
        RETURN;
    END IF;
    FOR v_c IN SELECT value FROM jsonb_array_elements(p_contacts_json) LOOP
        IF jsonb_typeof(v_c) <> 'object' THEN CONTINUE; END IF;
        BEGIN
            v_cid := NULLIF(BTRIM(COALESCE(v_c->>'id', '')), '')::BIGINT;
        EXCEPTION WHEN OTHERS THEN CONTINUE; END;
        IF v_cid IS NULL OR prod_sync.is_tombstoned('sigmasz', 'contact', v_cid) THEN CONTINUE; END IF;
        v_ts   := prod_sync.safe_cf_to_timestamp(NULLIF(BTRIM(COALESCE(v_c->>'updated_at', '')), ''));
        v_main := COALESCE((v_c->>'is_main')::BOOLEAN, FALSE);
        INSERT INTO prod_sync.sigmasz_contacts
            (contact_id, name, updated_at, raw_json, is_deleted, _synced_at)
        VALUES (v_cid, v_c->>'name', v_ts, v_c, FALSE, NOW())
        ON CONFLICT (contact_id) DO UPDATE SET
            name       = EXCLUDED.name,
            updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_contacts.updated_at),
                                  EXCLUDED.updated_at, sigmasz_contacts.updated_at),
            raw_json   = sigmasz_contacts.raw_json || EXCLUDED.raw_json,
                _synced_at = NOW()
            WHERE sigmasz_contacts.updated_at IS NULL
               OR COALESCE(EXCLUDED.updated_at, '-infinity'::TIMESTAMPTZ) >= sigmasz_contacts.updated_at;
        INSERT INTO prod_sync.sigmasz_lead_contacts (lead_id, contact_id, is_main)
        VALUES (p_lead_id, v_cid, v_main)
        ON CONFLICT (lead_id, contact_id) DO UPDATE SET is_main = EXCLUDED.is_main;
        v_ids := array_append(v_ids, v_cid);
    END LOOP;
    IF array_length(v_ids, 1) > 0 THEN
        DELETE FROM prod_sync.sigmasz_lead_contacts
        WHERE lead_id = p_lead_id AND contact_id <> ALL(v_ids);
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '[process_embedded_contacts] lead_id=% error=%', p_lead_id, SQLERRM;
END;
$$;

-- 3.3 Leads unpacker for sigmasz
CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path = airbyte_raw, prod_sync, public AS $$
DECLARE
    v_lid BIGINT;
    v_cts TIMESTAMPTZ;
    v_uts TIMESTAMPTZ;
    v_emb JSONB := '{}';
    v_cf  JSONB := '[]';
    v_rj  JSONB;
    v_ec  JSONB;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_sigmasz'));
    v_lid := NEW.id;
    IF v_lid IS NULL OR prod_sync.is_tombstoned('sigmasz', 'lead', v_lid) THEN RETURN NEW; END IF;
    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN
        PERFORM prod_sync.register_tombstone('sigmasz', 'lead', v_lid);
        DELETE FROM prod_sync.sigmasz_lead_contacts WHERE lead_id = v_lid;
        DELETE FROM prod_sync.sigmasz_leads WHERE lead_id = v_lid;
        RETURN NEW;
    END IF;
    BEGIN
        IF pg_typeof(NEW.created_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_cts := prod_sync.safe_cf_to_timestamp(NEW.created_at::TEXT);
        ELSE
            v_cts := NEW.created_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_cts := NULL; END;
    IF v_cts IS NULL THEN
        BEGIN
            SELECT prod_sync.safe_cf_to_timestamp(l.created_at::TEXT) INTO v_cts
            FROM airbyte_raw.sigmasz_leads l
            WHERE l.id = v_lid AND l.created_at IS NOT NULL
            ORDER BY l.created_at ASC LIMIT 1;
        EXCEPTION WHEN OTHERS THEN NULL; END;
    END IF;
    BEGIN
        IF pg_typeof(NEW.updated_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_uts := prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT);
        ELSE
            v_uts := NEW.updated_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_uts := NULL; END;
    BEGIN
        IF NEW._embedded IS NOT NULL AND BTRIM(NEW._embedded::TEXT) NOT IN ('', 'null') THEN
            v_emb := NEW._embedded::JSONB;
            IF jsonb_typeof(v_emb) <> 'object' THEN v_emb := '{}'; END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_emb := '{}'; END;
    BEGIN
        IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN ('', 'null') THEN
            v_cf := NEW.custom_fields_values::JSONB;
            IF jsonb_typeof(v_cf) <> 'array' THEN v_cf := '[]'; END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_cf := '[]'; END;
    v_rj := jsonb_build_object(
        'id', NEW.id, 'name', NEW.name, 'status_id', NEW.status_id,
        'pipeline_id', NEW.pipeline_id, 'price', NEW.price,
        'created_at', NEW.created_at, 'updated_at', NEW.updated_at,
        'is_deleted', NEW.is_deleted, '_embedded', v_emb,
        'custom_fields_values', v_cf
    );
    INSERT INTO prod_sync.sigmasz_leads
        (lead_id, name, status_id, pipeline_id, price, created_at, updated_at, raw_json, is_deleted, _synced_at)
    VALUES
        (v_lid, NEW.name, NEW.status_id, NEW.pipeline_id, NEW.price, v_cts, v_uts, v_rj, FALSE, NOW())
    ON CONFLICT (lead_id) DO UPDATE SET
        name        = EXCLUDED.name,
        status_id   = EXCLUDED.status_id,
        pipeline_id = EXCLUDED.pipeline_id,
        price       = EXCLUDED.price,
        created_at  = COALESCE(GREATEST(EXCLUDED.created_at, sigmasz_leads.created_at),
                               EXCLUDED.created_at, sigmasz_leads.created_at),
        updated_at  = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_leads.updated_at),
                               EXCLUDED.updated_at, sigmasz_leads.updated_at),
        raw_json    = EXCLUDED.raw_json,
        is_deleted  = FALSE,
            _synced_at  = NOW()
        WHERE sigmasz_leads.updated_at IS NULL
           OR COALESCE(EXCLUDED.updated_at, '-infinity'::TIMESTAMPTZ) >= sigmasz_leads.updated_at;
    v_ec := v_emb -> 'contacts';
    IF v_ec IS NOT NULL AND jsonb_typeof(v_ec) = 'array' THEN
        PERFORM prod_sync.process_embedded_contacts(v_ec, v_lid, jsonb_array_length(v_ec) = 0);
    END IF;
    RETURN NEW;
EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range
      OR invalid_parameter_value OR data_exception THEN
        BEGIN
            INSERT INTO airbyte_raw.l2_dead_letter_queue
                (stream_name, entity_id, raw_record, error_message, sqlstate)
            VALUES ('sigmasz_leads', v_lid, to_jsonb(NEW), SQLERRM, SQLSTATE);
        EXCEPTION WHEN OTHERS THEN NULL; END;
        RAISE WARNING '[unpack_sigmasz_leads_l2] BAD DATA lead_id=% → DLQ. error=%', v_lid, SQLERRM;
        RETURN NEW;
    WHEN OTHERS THEN
        RAISE EXCEPTION '[unpack_sigmasz_leads_l2] TRANSIENT ERROR lead_id=%: % (STATE: %)',
            v_lid, SQLERRM, SQLSTATE;
END;
$$;

-- 3.4 Contacts unpacker for sigmasz
CREATE OR REPLACE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path = airbyte_raw, prod_sync, public AS $$
DECLARE
    v_cid BIGINT;
    v_uts TIMESTAMPTZ;
    v_cf  JSONB := '[]';
    v_rj  JSONB;
    v_ph  TEXT;
    v_em  TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_sigmasz'));
    v_cid := NEW.id;
    IF v_cid IS NULL OR prod_sync.is_tombstoned('sigmasz', 'contact', v_cid) THEN RETURN NEW; END IF;
    IF COALESCE(NEW.is_deleted, FALSE) IS TRUE THEN
        PERFORM prod_sync.register_tombstone('sigmasz', 'contact', v_cid);
        DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_cid;
        DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_cid;
        DELETE FROM prod_sync.sigmasz_lead_contacts WHERE contact_id = v_cid;
        DELETE FROM prod_sync.sigmasz_contacts WHERE contact_id = v_cid;
        RETURN NEW;
    END IF;
    BEGIN
        IF pg_typeof(NEW.updated_at) IN ('bigint'::REGTYPE, 'integer'::REGTYPE) THEN
            v_uts := prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT);
        ELSE
            v_uts := NEW.updated_at::TIMESTAMPTZ;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_uts := NULL; END;
    BEGIN
        IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN ('', 'null') THEN
            v_cf := NEW.custom_fields_values::JSONB;
            IF jsonb_typeof(v_cf) <> 'array' THEN v_cf := '[]'; END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN v_cf := '[]'; END;
    v_rj := jsonb_build_object(
        'id', NEW.id, 'name', NEW.name, 'updated_at', NEW.updated_at,
        'is_deleted', NEW.is_deleted, 'custom_fields_values', v_cf
    );
    INSERT INTO prod_sync.sigmasz_contacts
        (contact_id, name, updated_at, raw_json, is_deleted, _synced_at)
    VALUES (v_cid, NEW.name, v_uts, v_rj, FALSE, NOW())
    ON CONFLICT (contact_id) DO UPDATE SET
        name       = EXCLUDED.name,
        updated_at = COALESCE(GREATEST(EXCLUDED.updated_at, sigmasz_contacts.updated_at),
                              EXCLUDED.updated_at, sigmasz_contacts.updated_at),
        raw_json   = EXCLUDED.raw_json,
        is_deleted = FALSE,
            _synced_at = NOW()
        WHERE sigmasz_contacts.updated_at IS NULL
           OR COALESCE(EXCLUDED.updated_at, '-infinity'::TIMESTAMPTZ) >= sigmasz_contacts.updated_at;
    DELETE FROM prod_sync.sigmasz_contact_phones WHERE contact_id = v_cid;
    FOR v_ph IN
        SELECT DISTINCT public.normalize_phone(v.value->>'value')
        FROM jsonb_array_elements(v_cf) cf
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]' END
        ) v
        WHERE cf->>'field_code' = 'PHONE'
          AND public.normalize_phone(v.value->>'value') IS NOT NULL
          AND length(public.normalize_phone(v.value->>'value')) >= 10
    LOOP
        INSERT INTO prod_sync.sigmasz_contact_phones (contact_id, phone)
        VALUES (v_cid, v_ph) ON CONFLICT DO NOTHING;
    END LOOP;
    DELETE FROM prod_sync.sigmasz_contact_emails WHERE contact_id = v_cid;
    FOR v_em IN
        SELECT DISTINCT lower(BTRIM(v.value->>'value'))
        FROM jsonb_array_elements(v_cf) cf
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(cf->'values') = 'array' THEN cf->'values' ELSE '[]' END
        ) v
        WHERE cf->>'field_code' = 'EMAIL'
          AND NULLIF(BTRIM(v.value->>'value'), '') IS NOT NULL
    LOOP
        INSERT INTO prod_sync.sigmasz_contact_emails (contact_id, email)
        VALUES (v_cid, v_em) ON CONFLICT DO NOTHING;
    END LOOP;
    RETURN NEW;
EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range
      OR invalid_parameter_value OR data_exception THEN
        BEGIN
            INSERT INTO airbyte_raw.l2_dead_letter_queue
                (stream_name, entity_id, raw_record, error_message, sqlstate)
            VALUES ('sigmasz_contacts', v_cid, to_jsonb(NEW), SQLERRM, SQLSTATE);
        EXCEPTION WHEN OTHERS THEN NULL; END;
        RAISE WARNING '[unpack_sigmasz_contacts_l2] BAD DATA contact_id=% → DLQ. error=%', v_cid, SQLERRM;
        RETURN NEW;
    WHEN OTHERS THEN
        RAISE EXCEPTION '[unpack_sigmasz_contacts_l2] TRANSIENT ERROR contact_id=%: % (STATE: %)',
            v_cid, SQLERRM, SQLSTATE;
END;
$$;


-- =============================================================================
-- BLOCK 4: MULTI-DOMAIN GENERATOR FUNCTIONS
-- (For concepta and entrum — uses dynamic SQL to generate per-domain functions)
-- =============================================================================

-- 4.1 Create L2 tables for a new domain
CREATE OR REPLACE FUNCTION prod_sync.setup_new_domain(p_domain TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS prod_sync.%1$I_leads (lead_id BIGINT PRIMARY KEY, name TEXT, status_id INT, pipeline_id INT, price NUMERIC, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, raw_json JSONB NOT NULL, is_deleted BOOLEAN DEFAULT FALSE, _synced_at TIMESTAMPTZ DEFAULT NOW())', p_domain);
    EXECUTE format('CREATE TABLE IF NOT EXISTS prod_sync.%1$I_contacts (contact_id BIGINT PRIMARY KEY, name TEXT, updated_at TIMESTAMPTZ, raw_json JSONB NOT NULL, is_deleted BOOLEAN DEFAULT FALSE, _synced_at TIMESTAMPTZ DEFAULT NOW())', p_domain);
    EXECUTE format('CREATE TABLE IF NOT EXISTS prod_sync.%1$I_lead_contacts (lead_id BIGINT NOT NULL, contact_id BIGINT NOT NULL, is_main BOOLEAN DEFAULT FALSE, PRIMARY KEY (lead_id, contact_id), FOREIGN KEY (lead_id) REFERENCES prod_sync.%1$I_leads(lead_id) ON DELETE CASCADE, FOREIGN KEY (contact_id) REFERENCES prod_sync.%1$I_contacts(contact_id) ON DELETE CASCADE)', p_domain);
    EXECUTE format('CREATE TABLE IF NOT EXISTS prod_sync.%1$I_contact_phones (contact_id BIGINT NOT NULL, phone TEXT NOT NULL, PRIMARY KEY (contact_id, phone), FOREIGN KEY (contact_id) REFERENCES prod_sync.%1$I_contacts(contact_id) ON DELETE CASCADE)', p_domain);
    EXECUTE format('CREATE TABLE IF NOT EXISTS prod_sync.%1$I_contact_emails (contact_id BIGINT NOT NULL, email TEXT NOT NULL, PRIMARY KEY (contact_id, email), FOREIGN KEY (contact_id) REFERENCES prod_sync.%1$I_contacts(contact_id) ON DELETE CASCADE)', p_domain);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%1$s_leads_wm ON prod_sync.%1$I_leads (_synced_at ASC, lead_id ASC)', p_domain);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%1$s_contacts_wm ON prod_sync.%1$I_contacts (_synced_at ASC, contact_id ASC)', p_domain);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%1$s_leads_deleted ON prod_sync.%1$I_leads (is_deleted) WHERE is_deleted=TRUE', p_domain);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%1$s_lc_lead ON prod_sync.%1$I_lead_contacts (lead_id)', p_domain);
    RETURN format('Domain "%s": L2 tables created.', p_domain);
END;
$$;

-- 4.2 Generate per-domain trigger functions via dynamic SQL
CREATE OR REPLACE FUNCTION prod_sync.setup_new_domain_functions(p_domain TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    EXECUTE format($func$
    CREATE OR REPLACE FUNCTION airbyte_raw.propagate_deleted_to_l2_%1$s() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $f$
    BEGIN
        PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_%1$s'));
        IF NEW.type='lead_deleted' AND NEW.entity_type='lead' THEN PERFORM prod_sync.register_tombstone(%1$L,'lead',NEW.entity_id); UPDATE prod_sync.%1$I_leads SET is_deleted=TRUE,_synced_at=NOW() WHERE lead_id=NEW.entity_id; END IF;
        IF NEW.type='contact_deleted' AND NEW.entity_type='contact' THEN PERFORM prod_sync.register_tombstone(%1$L,'contact',NEW.entity_id); UPDATE prod_sync.%1$I_contacts SET is_deleted=TRUE,_synced_at=NOW() WHERE contact_id=NEW.entity_id; END IF;
        RETURN NEW;
    END; $f$
    $func$, p_domain);

    EXECUTE format($func$
    CREATE OR REPLACE FUNCTION prod_sync.process_embedded_contacts_%1$s(p_contacts_json JSONB, p_lead_id BIGINT, p_explicit_empty BOOLEAN DEFAULT FALSE) RETURNS VOID LANGUAGE plpgsql AS $f$
    DECLARE v_c JSONB; v_cid BIGINT; v_ts TIMESTAMPTZ; v_main BOOLEAN; v_ids BIGINT[]:=ARRAY[]::BIGINT[];
    BEGIN
        IF p_explicit_empty THEN DELETE FROM prod_sync.%1$I_lead_contacts WHERE lead_id=p_lead_id; RETURN; END IF;
        FOR v_c IN SELECT value FROM jsonb_array_elements(p_contacts_json) LOOP
            IF jsonb_typeof(v_c)<>'object' THEN CONTINUE; END IF;
            BEGIN v_cid:=NULLIF(BTRIM(COALESCE(v_c->>'id','')),'')::BIGINT; EXCEPTION WHEN OTHERS THEN CONTINUE; END;
            IF v_cid IS NULL OR prod_sync.is_tombstoned(%1$L,'contact',v_cid) THEN CONTINUE; END IF;
            v_ts:=prod_sync.safe_cf_to_timestamp(NULLIF(BTRIM(COALESCE(v_c->>'updated_at','')),'')); v_main:=COALESCE((v_c->>'is_main')::BOOLEAN,FALSE);
            INSERT INTO prod_sync.%1$I_contacts (contact_id,name,updated_at,raw_json,is_deleted,_synced_at) VALUES(v_cid,v_c->>'name',v_ts,v_c,FALSE,NOW()) ON CONFLICT(contact_id) DO UPDATE SET name=EXCLUDED.name, updated_at=COALESCE(GREATEST(EXCLUDED.updated_at,%1$I_contacts.updated_at),EXCLUDED.updated_at,%1$I_contacts.updated_at), raw_json=%1$I_contacts.raw_json||EXCLUDED.raw_json, _synced_at=NOW() WHERE %1$I_contacts.updated_at IS NULL OR COALESCE(EXCLUDED.updated_at, '-infinity'::TIMESTAMPTZ) >= %1$I_contacts.updated_at;
            INSERT INTO prod_sync.%1$I_lead_contacts(lead_id,contact_id,is_main) VALUES(p_lead_id,v_cid,v_main) ON CONFLICT(lead_id,contact_id) DO UPDATE SET is_main=EXCLUDED.is_main;
            v_ids:=array_append(v_ids,v_cid);
        END LOOP;
        IF array_length(v_ids,1)>0 THEN DELETE FROM prod_sync.%1$I_lead_contacts WHERE lead_id=p_lead_id AND contact_id<>ALL(v_ids); END IF;
    END; $f$
    $func$, p_domain);

    EXECUTE format($func$
    CREATE OR REPLACE FUNCTION airbyte_raw.unpack_%1$s_leads_l2() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=airbyte_raw,prod_sync,public AS $f$
    DECLARE v_lid BIGINT; v_cts TIMESTAMPTZ; v_uts TIMESTAMPTZ; v_emb JSONB:='{}'; v_cf JSONB:='[]'; v_rj JSONB; v_ec JSONB;
    BEGIN
        PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_%1$s'));
        v_lid:=NEW.id; IF v_lid IS NULL OR prod_sync.is_tombstoned(%1$L,'lead',v_lid) THEN RETURN NEW; END IF;
        IF COALESCE(NEW.is_deleted,FALSE) IS TRUE THEN PERFORM prod_sync.register_tombstone(%1$L,'lead',v_lid); DELETE FROM prod_sync.%1$I_lead_contacts WHERE lead_id=v_lid; DELETE FROM prod_sync.%1$I_leads WHERE lead_id=v_lid; RETURN NEW; END IF;
        BEGIN IF pg_typeof(NEW.created_at) IN('bigint'::REGTYPE,'integer'::REGTYPE) THEN v_cts:=prod_sync.safe_cf_to_timestamp(NEW.created_at::TEXT); ELSE v_cts:=NEW.created_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_cts:=NULL; END;
        IF v_cts IS NULL THEN BEGIN EXECUTE format('SELECT prod_sync.safe_cf_to_timestamp(l.created_at::TEXT) FROM airbyte_raw.%1$I_leads l WHERE l.id = $1 AND l.created_at IS NOT NULL ORDER BY l.created_at ASC LIMIT 1') INTO v_cts USING v_lid; EXCEPTION WHEN OTHERS THEN NULL; END; END IF;
        BEGIN IF pg_typeof(NEW.updated_at) IN('bigint'::REGTYPE,'integer'::REGTYPE) THEN v_uts:=prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT); ELSE v_uts:=NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_uts:=NULL; END;
        BEGIN IF NEW._embedded IS NOT NULL AND BTRIM(NEW._embedded::TEXT) NOT IN('','null') THEN v_emb:=NEW._embedded::JSONB; IF jsonb_typeof(v_emb)<>'object' THEN v_emb:='{}'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_emb:='{}'; END;
        BEGIN IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN('','null') THEN v_cf:=NEW.custom_fields_values::JSONB; IF jsonb_typeof(v_cf)<>'array' THEN v_cf:='[]'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_cf:='[]'; END;
        v_rj:=jsonb_build_object('id',NEW.id,'name',NEW.name,'status_id',NEW.status_id,'pipeline_id',NEW.pipeline_id,'price',NEW.price,'created_at',NEW.created_at,'updated_at',NEW.updated_at,'is_deleted',NEW.is_deleted,'_embedded',v_emb,'custom_fields_values',v_cf);
        INSERT INTO prod_sync.%1$I_leads(lead_id,name,status_id,pipeline_id,price,created_at,updated_at,raw_json,is_deleted,_synced_at) VALUES(v_lid,NEW.name,NEW.status_id,NEW.pipeline_id,NEW.price,v_cts,v_uts,v_rj,FALSE,NOW()) ON CONFLICT(lead_id) DO UPDATE SET name=EXCLUDED.name,status_id=EXCLUDED.status_id,pipeline_id=EXCLUDED.pipeline_id,price=EXCLUDED.price,created_at=COALESCE(GREATEST(EXCLUDED.created_at,%1$I_leads.created_at),EXCLUDED.created_at,%1$I_leads.created_at),updated_at=COALESCE(GREATEST(EXCLUDED.updated_at,%1$I_leads.updated_at),EXCLUDED.updated_at,%1$I_leads.updated_at),raw_json=EXCLUDED.raw_json,is_deleted=FALSE,_synced_at=NOW() WHERE %1$I_leads.updated_at IS NULL OR COALESCE(EXCLUDED.updated_at, '-infinity'::TIMESTAMPTZ) >= %1$I_leads.updated_at;
        v_ec:=v_emb->'contacts'; IF v_ec IS NOT NULL AND jsonb_typeof(v_ec)='array' THEN PERFORM prod_sync.process_embedded_contacts_%1$s(v_ec,v_lid,jsonb_array_length(v_ec)=0); END IF;
        RETURN NEW;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN
        BEGIN INSERT INTO airbyte_raw.l2_dead_letter_queue(stream_name,entity_id,raw_record,error_message,sqlstate) VALUES(%1$L||'_leads',v_lid,to_jsonb(NEW),SQLERRM,SQLSTATE); EXCEPTION WHEN OTHERS THEN NULL; END; RETURN NEW;
    END; $f$
    $func$, p_domain);

    EXECUTE format($func$
    CREATE OR REPLACE FUNCTION airbyte_raw.unpack_%1$s_contacts_l2() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=airbyte_raw,prod_sync,public AS $f$
    DECLARE v_cid BIGINT; v_uts TIMESTAMPTZ; v_cf JSONB:='[]'; v_rj JSONB; v_ph TEXT; v_em TEXT;
    BEGIN
        PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_%1$s'));
        v_cid:=NEW.id; IF v_cid IS NULL OR prod_sync.is_tombstoned(%1$L,'contact',v_cid) THEN RETURN NEW; END IF;
        IF COALESCE(NEW.is_deleted,FALSE) IS TRUE THEN PERFORM prod_sync.register_tombstone(%1$L,'contact',v_cid); DELETE FROM prod_sync.%1$I_contact_phones WHERE contact_id=v_cid; DELETE FROM prod_sync.%1$I_contact_emails WHERE contact_id=v_cid; DELETE FROM prod_sync.%1$I_lead_contacts WHERE contact_id=v_cid; DELETE FROM prod_sync.%1$I_contacts WHERE contact_id=v_cid; RETURN NEW; END IF;
        BEGIN IF pg_typeof(NEW.updated_at) IN('bigint'::REGTYPE,'integer'::REGTYPE) THEN v_uts:=prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT); ELSE v_uts:=NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_uts:=NULL; END;
        BEGIN IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN('','null') THEN v_cf:=NEW.custom_fields_values::JSONB; IF jsonb_typeof(v_cf)<>'array' THEN v_cf:='[]'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_cf:='[]'; END;
        v_rj:=jsonb_build_object('id',NEW.id,'name',NEW.name,'updated_at',NEW.updated_at,'is_deleted',NEW.is_deleted,'custom_fields_values',v_cf);
        INSERT INTO prod_sync.%1$I_contacts(contact_id,name,updated_at,raw_json,is_deleted,_synced_at) VALUES(v_cid,NEW.name,v_uts,v_rj,FALSE,NOW()) ON CONFLICT(contact_id) DO UPDATE SET name=EXCLUDED.name,updated_at=COALESCE(GREATEST(EXCLUDED.updated_at,%1$I_contacts.updated_at),EXCLUDED.updated_at,%1$I_contacts.updated_at),raw_json=EXCLUDED.raw_json,is_deleted=FALSE,_synced_at=NOW() WHERE %1$I_contacts.updated_at IS NULL OR COALESCE(EXCLUDED.updated_at, '-infinity'::TIMESTAMPTZ) >= %1$I_contacts.updated_at;
        DELETE FROM prod_sync.%1$I_contact_phones WHERE contact_id=v_cid;
        FOR v_ph IN SELECT DISTINCT public.normalize_phone(v.value->>'value') FROM jsonb_array_elements(v_cf) cf CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values')='array' THEN cf->'values' ELSE '[]' END) v WHERE cf->>'field_code'='PHONE' AND public.normalize_phone(v.value->>'value') IS NOT NULL AND length(public.normalize_phone(v.value->>'value'))>=10 LOOP INSERT INTO prod_sync.%1$I_contact_phones(contact_id,phone) VALUES(v_cid,v_ph) ON CONFLICT DO NOTHING; END LOOP;
        DELETE FROM prod_sync.%1$I_contact_emails WHERE contact_id=v_cid;
        FOR v_em IN SELECT DISTINCT lower(BTRIM(v.value->>'value')) FROM jsonb_array_elements(v_cf) cf CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(cf->'values')='array' THEN cf->'values' ELSE '[]' END) v WHERE cf->>'field_code'='EMAIL' AND NULLIF(BTRIM(v.value->>'value'),'') IS NOT NULL LOOP INSERT INTO prod_sync.%1$I_contact_emails(contact_id,email) VALUES(v_cid,v_em) ON CONFLICT DO NOTHING; END LOOP;
        RETURN NEW;
    EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range OR invalid_parameter_value OR data_exception THEN
        BEGIN INSERT INTO airbyte_raw.l2_dead_letter_queue(stream_name,entity_id,raw_record,error_message,sqlstate) VALUES(%1$L||'_contacts',v_cid,to_jsonb(NEW),SQLERRM,SQLSTATE); EXCEPTION WHEN OTHERS THEN NULL; END; RETURN NEW;
    END; $f$
    $func$, p_domain);

    RETURN format('Domain "%s": all functions created.', p_domain);
END;
$$;

-- 4.3 Safely attach triggers (checks table existence first)
CREATE OR REPLACE FUNCTION prod_sync.attach_domain_triggers(p_domain TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'airbyte_raw' AND c.relname = p_domain || '_leads') THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_unpack_%1$s_leads ON airbyte_raw.%1$I_leads; CREATE TRIGGER trg_unpack_%1$s_leads AFTER INSERT OR UPDATE ON airbyte_raw.%1$I_leads FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_%1$s_leads_l2()', p_domain);
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'airbyte_raw' AND c.relname = p_domain || '_contacts') THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_unpack_%1$s_contacts ON airbyte_raw.%1$I_contacts; CREATE TRIGGER trg_unpack_%1$s_contacts AFTER INSERT OR UPDATE ON airbyte_raw.%1$I_contacts FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_%1$s_contacts_l2()', p_domain);
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname = 'airbyte_raw' AND c.relname = p_domain || '_events') THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_deleted_%1$s ON airbyte_raw.%1$I_events; CREATE TRIGGER trg_deleted_%1$s AFTER INSERT OR UPDATE ON airbyte_raw.%1$I_events FOR EACH ROW EXECUTE FUNCTION airbyte_raw.propagate_deleted_to_l2_%1$s()', p_domain);
    END IF;
    RETURN format('Domain "%s": triggers attached.', p_domain);
END;
$$;

-- 4.4 Event trigger: auto-provision new domains when Airbyte creates tables
CREATE OR REPLACE FUNCTION prod_sync.auto_provision_domain()
RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    obj RECORD;
    v_tbl TEXT; v_dom TEXT;
    v_domains_to_attach TEXT[] := ARRAY[]::TEXT[];
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF obj.object_type = 'table' AND obj.schema_name = 'airbyte_raw'
           AND obj.object_identity NOT LIKE '%custom_fields%' THEN
            IF    obj.object_identity LIKE 'airbyte_raw.%_leads'    THEN v_tbl := split_part(obj.object_identity,'.', 2); v_dom := replace(v_tbl, '_leads', '');
            ELSIF obj.object_identity LIKE 'airbyte_raw.%_contacts' THEN v_tbl := split_part(obj.object_identity,'.', 2); v_dom := replace(v_tbl, '_contacts', '');
            ELSIF obj.object_identity LIKE 'airbyte_raw.%_events'   THEN v_tbl := split_part(obj.object_identity,'.', 2); v_dom := replace(v_tbl, '_events', '');
            ELSE CONTINUE; END IF;

            IF NOT EXISTS (SELECT 1 FROM prod_sync.domain_registry WHERE domain = v_dom) THEN
                PERFORM prod_sync.setup_new_domain(v_dom);
                PERFORM prod_sync.setup_new_domain_functions(v_dom);
                INSERT INTO prod_sync.domain_registry (domain) VALUES (v_dom);
                RAISE NOTICE '[auto_provision] Domain "%" provisioned.', v_dom;
            END IF;

            IF NOT (v_dom = ANY(v_domains_to_attach)) THEN
                v_domains_to_attach := array_append(v_domains_to_attach, v_dom);
            END IF;
        END IF;
    END LOOP;
    FOREACH v_dom IN ARRAY v_domains_to_attach LOOP
        PERFORM prod_sync.attach_domain_triggers(v_dom);
    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS trg_auto_provision_domain;
CREATE EVENT TRIGGER trg_auto_provision_domain
    ON ddl_command_end WHEN TAG IN ('CREATE TABLE')
    EXECUTE FUNCTION prod_sync.auto_provision_domain();


-- =============================================================================
-- BLOCK 5: APPLY FOR ALL DOMAINS
-- =============================================================================

-- sigmasz: tables already created in 00_bootstrap; functions already defined above
-- Apply triggers if airbyte_raw tables already exist (i.e. post-first-sync)
DROP TRIGGER IF EXISTS trg_unpack_sigmasz_leads_l2 ON airbyte_raw.sigmasz_leads;
CREATE TRIGGER trg_unpack_sigmasz_leads_l2
    AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_leads
    FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2();

DROP TRIGGER IF EXISTS trg_unpack_sigmasz_contacts_l2 ON airbyte_raw.sigmasz_contacts;
CREATE TRIGGER trg_unpack_sigmasz_contacts_l2
    AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_contacts
    FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2();

DROP TRIGGER IF EXISTS trg_propagate_deleted_to_l2 ON airbyte_raw.sigmasz_events;
CREATE TRIGGER trg_propagate_deleted_to_l2
    AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_events
    FOR EACH ROW EXECUTE FUNCTION airbyte_raw.propagate_deleted_to_l2();

-- concepta + entrum: generate tables, functions, and register domains
SELECT prod_sync.setup_new_domain('concepta');
SELECT prod_sync.setup_new_domain_functions('concepta');
INSERT INTO prod_sync.domain_registry (domain) VALUES ('concepta') ON CONFLICT DO NOTHING;

SELECT prod_sync.setup_new_domain('entrum');
SELECT prod_sync.setup_new_domain_functions('entrum');
INSERT INTO prod_sync.domain_registry (domain) VALUES ('entrum') ON CONFLICT DO NOTHING;

-- Attach triggers for concepta + entrum (only after their airbyte_raw tables exist)
-- Run manually if the event trigger did not fire during the first Airbyte sync:
--   SELECT prod_sync.attach_domain_triggers('concepta');
--   SELECT prod_sync.attach_domain_triggers('entrum');


-- =============================================================================
-- BLOCK 6: INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_composite_wm    ON prod_sync.sigmasz_leads    (_synced_at ASC, lead_id ASC);
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_composite_wm ON prod_sync.sigmasz_contacts (_synced_at ASC, contact_id ASC);
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_is_deleted       ON prod_sync.sigmasz_leads    (is_deleted) WHERE is_deleted = TRUE;
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_is_deleted    ON prod_sync.sigmasz_contacts (is_deleted) WHERE is_deleted = TRUE;
CREATE INDEX IF NOT EXISTS idx_sigmasz_lead_contacts_lead_id  ON prod_sync.sigmasz_lead_contacts (lead_id);

COMMIT;
