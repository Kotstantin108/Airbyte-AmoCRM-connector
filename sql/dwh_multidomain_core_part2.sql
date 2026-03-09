-- =============================================================================
-- MULTI-DOMAIN DWH — ЧАСТЬ 2: ГЕНЕРАТОРЫ + АВТО-ОБНАРУЖЕНИЕ
-- =============================================================================
-- Выполняется после dwh_multidomain_core_part1.sql
--
-- Содержит:
--   A. setup_new_domain()          — создаёт L2 таблицы
--   B. setup_new_domain_functions()— создаёт все функции через dynamic SQL
--   C. attach_domain_triggers()    — привязывает триггеры к airbyte_raw
--   D. auto_provision_domain()     — event trigger авто-обнаружение
--   E. Применение для concepta и entrum
-- =============================================================================

BEGIN;

-- =============================================================================
-- A. setup_new_domain (только L2 таблицы)
-- =============================================================================
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

-- =============================================================================
-- B. setup_new_domain_functions (Исправлен format с 4 аргументами)
-- =============================================================================
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
            INSERT INTO prod_sync.%1$I_contacts (contact_id,name,updated_at,raw_json,is_deleted,_synced_at) VALUES(v_cid,v_c->>'name',v_ts,v_c,FALSE,NOW()) ON CONFLICT(contact_id) DO UPDATE SET name=EXCLUDED.name, updated_at=COALESCE(GREATEST(EXCLUDED.updated_at,%1$I_contacts.updated_at),EXCLUDED.updated_at,%1$I_contacts.updated_at), raw_json=%1$I_contacts.raw_json||EXCLUDED.raw_json, _synced_at=NOW();
            INSERT INTO prod_sync.%1$I_lead_contacts(lead_id,contact_id,is_main) VALUES(p_lead_id,v_cid,v_main) ON CONFLICT(lead_id,contact_id) DO UPDATE SET is_main=EXCLUDED.is_main;
            v_ids:=array_append(v_ids,v_cid);
        END LOOP;
        IF array_length(v_ids,1)>0 THEN DELETE FROM prod_sync.%1$I_lead_contacts WHERE lead_id=p_lead_id AND contact_id<>ALL(v_ids); END IF;
    END; $f$
    $func$, p_domain);

    EXECUTE format($func$
    CREATE OR REPLACE FUNCTION prod_sync.get_best_contact_for_lead_%1$s(p_lead_id BIGINT) RETURNS TABLE(c_id BIGINT,c_name TEXT,c_phone TEXT,c_email TEXT) LANGUAGE plpgsql STABLE AS $f$
    BEGIN RETURN QUERY SELECT c.contact_id,c.name::TEXT, (SELECT cp.phone FROM prod_sync.%1$I_contact_phones cp WHERE cp.contact_id=c.contact_id ORDER BY cp.phone LIMIT 1), (SELECT ce.email FROM prod_sync.%1$I_contact_emails ce WHERE ce.contact_id=c.contact_id ORDER BY ce.email LIMIT 1) FROM prod_sync.%1$I_lead_contacts lc JOIN prod_sync.%1$I_contacts c ON c.contact_id=lc.contact_id WHERE lc.lead_id=p_lead_id AND COALESCE(c.is_deleted,FALSE) IS FALSE AND NOT prod_sync.is_tombstoned(%1$L,'contact',c.contact_id) ORDER BY COALESCE(lc.is_main,FALSE) DESC, c.contact_id ASC LIMIT 1; END; $f$
    $func$, p_domain);

    EXECUTE format($func$
    CREATE OR REPLACE FUNCTION airbyte_raw.unpack_%1$s_leads_l2() RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER SET search_path=airbyte_raw,prod_sync,public AS $f$
    DECLARE v_lid BIGINT; v_cts TIMESTAMPTZ; v_uts TIMESTAMPTZ; v_emb JSONB:='{}'; v_cf JSONB:='[]'; v_rj JSONB; v_ec JSONB;
    BEGIN
        PERFORM pg_advisory_xact_lock(hashtext('amo_sync_lock_%1$s'));
        v_lid:=NEW.id; IF v_lid IS NULL OR prod_sync.is_tombstoned(%1$L,'lead',v_lid) THEN RETURN NEW; END IF;
        IF COALESCE(NEW.is_deleted,FALSE) IS TRUE THEN PERFORM prod_sync.register_tombstone(%1$L,'lead',v_lid); UPDATE prod_sync.%1$I_leads SET is_deleted=TRUE,_synced_at=NOW() WHERE lead_id=v_lid; RETURN NEW; END IF;
        BEGIN IF pg_typeof(NEW.created_at) IN('bigint'::REGTYPE,'integer'::REGTYPE) THEN v_cts:=prod_sync.safe_cf_to_timestamp(NEW.created_at::TEXT); ELSE v_cts:=NEW.created_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_cts:=NULL; END;
        IF v_cts IS NULL THEN BEGIN EXECUTE format('SELECT prod_sync.safe_cf_to_timestamp(l.created_at::TEXT) FROM airbyte_raw.%1$I_leads l WHERE l.id = $1 AND l.created_at IS NOT NULL ORDER BY l.created_at ASC LIMIT 1') INTO v_cts USING v_lid; EXCEPTION WHEN OTHERS THEN NULL; END; END IF;
        BEGIN IF pg_typeof(NEW.updated_at) IN('bigint'::REGTYPE,'integer'::REGTYPE) THEN v_uts:=prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT); ELSE v_uts:=NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_uts:=NULL; END;
        BEGIN IF NEW._embedded IS NOT NULL AND BTRIM(NEW._embedded::TEXT) NOT IN('','null') THEN v_emb:=NEW._embedded::JSONB; IF jsonb_typeof(v_emb)<>'object' THEN v_emb:='{}'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_emb:='{}'; END;
        BEGIN IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN('','null') THEN v_cf:=NEW.custom_fields_values::JSONB; IF jsonb_typeof(v_cf)<>'array' THEN v_cf:='[]'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_cf:='[]'; END;
        v_rj:=jsonb_build_object('id',NEW.id,'name',NEW.name,'status_id',NEW.status_id,'pipeline_id',NEW.pipeline_id,'price',NEW.price,'created_at',NEW.created_at,'updated_at',NEW.updated_at,'is_deleted',NEW.is_deleted,'_embedded',v_emb,'custom_fields_values',v_cf);
        INSERT INTO prod_sync.%1$I_leads(lead_id,name,status_id,pipeline_id,price,created_at,updated_at,raw_json,is_deleted,_synced_at) VALUES(v_lid,NEW.name,NEW.status_id,NEW.pipeline_id,NEW.price,v_cts,v_uts,v_rj,FALSE,NOW()) ON CONFLICT(lead_id) DO UPDATE SET name=EXCLUDED.name,status_id=EXCLUDED.status_id, pipeline_id=EXCLUDED.pipeline_id,price=EXCLUDED.price, created_at=COALESCE(GREATEST(EXCLUDED.created_at,%1$I_leads.created_at),EXCLUDED.created_at,%1$I_leads.created_at), updated_at=COALESCE(GREATEST(EXCLUDED.updated_at,%1$I_leads.updated_at),EXCLUDED.updated_at,%1$I_leads.updated_at), raw_json=EXCLUDED.raw_json,is_deleted=FALSE,_synced_at=NOW();
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
        IF COALESCE(NEW.is_deleted,FALSE) IS TRUE THEN PERFORM prod_sync.register_tombstone(%1$L,'contact',v_cid); UPDATE prod_sync.%1$I_contacts SET is_deleted=TRUE,_synced_at=NOW() WHERE contact_id=v_cid; RETURN NEW; END IF;
        BEGIN IF pg_typeof(NEW.updated_at) IN('bigint'::REGTYPE,'integer'::REGTYPE) THEN v_uts:=prod_sync.safe_cf_to_timestamp(NEW.updated_at::TEXT); ELSE v_uts:=NEW.updated_at::TIMESTAMPTZ; END IF; EXCEPTION WHEN OTHERS THEN v_uts:=NULL; END;
        BEGIN IF NEW.custom_fields_values IS NOT NULL AND BTRIM(NEW.custom_fields_values::TEXT) NOT IN('','null') THEN v_cf:=NEW.custom_fields_values::JSONB; IF jsonb_typeof(v_cf)<>'array' THEN v_cf:='[]'; END IF; END IF; EXCEPTION WHEN OTHERS THEN v_cf:='[]'; END;
        v_rj:=jsonb_build_object('id',NEW.id,'name',NEW.name,'updated_at',NEW.updated_at,'is_deleted',NEW.is_deleted,'custom_fields_values',v_cf);
        INSERT INTO prod_sync.%1$I_contacts(contact_id,name,updated_at,raw_json,is_deleted,_synced_at) VALUES(v_cid,NEW.name,v_uts,v_rj,FALSE,NOW()) ON CONFLICT(contact_id) DO UPDATE SET name=EXCLUDED.name, updated_at=COALESCE(GREATEST(EXCLUDED.updated_at,%1$I_contacts.updated_at),EXCLUDED.updated_at,%1$I_contacts.updated_at), raw_json=EXCLUDED.raw_json,is_deleted=FALSE,_synced_at=NOW();
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

-- =============================================================================
-- C. Безопасная привязка триггеров (проверка существования таблиц)
-- =============================================================================
CREATE OR REPLACE FUNCTION prod_sync.attach_domain_triggers(p_domain TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    -- Привязываем триггер leads, ТОЛЬКО ЕСЛИ ТАБЛИЦА СУЩЕСТВУЕТ
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'airbyte_raw' AND c.relname = p_domain || '_leads') THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_unpack_%1$s_leads ON airbyte_raw.%1$I_leads; CREATE TRIGGER trg_unpack_%1$s_leads AFTER INSERT OR UPDATE ON airbyte_raw.%1$I_leads FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_%1$s_leads_l2()', p_domain);
    END IF;

    -- Аналогично для contacts
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'airbyte_raw' AND c.relname = p_domain || '_contacts') THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_unpack_%1$s_contacts ON airbyte_raw.%1$I_contacts; CREATE TRIGGER trg_unpack_%1$s_contacts AFTER INSERT OR UPDATE ON airbyte_raw.%1$I_contacts FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_%1$s_contacts_l2()', p_domain);
    END IF;

    -- Аналогично для events
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'airbyte_raw' AND c.relname = p_domain || '_events') THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_deleted_%1$s ON airbyte_raw.%1$I_events; CREATE TRIGGER trg_deleted_%1$s AFTER INSERT OR UPDATE ON airbyte_raw.%1$I_events FOR EACH ROW EXECUTE FUNCTION airbyte_raw.propagate_deleted_to_l2_%1$s()', p_domain);
    END IF;

    -- Триггер обновления связки лид-контакт (таблица в prod_sync, существует всегда)
    EXECUTE format($trg$ CREATE OR REPLACE FUNCTION prod_sync.trg_lead_contact_change_%1$s() RETURNS TRIGGER LANGUAGE plpgsql AS $f$ BEGIN UPDATE prod_sync.%1$I_leads SET _synced_at=NOW() WHERE lead_id=NEW.lead_id; RETURN NEW; END; $f$ $trg$, p_domain);
    EXECUTE format('DROP TRIGGER IF EXISTS trg_lc_change_%1$s ON prod_sync.%1$I_lead_contacts; CREATE TRIGGER trg_lc_change_%1$s AFTER INSERT OR UPDATE ON prod_sync.%1$I_lead_contacts FOR EACH ROW EXECUTE FUNCTION prod_sync.trg_lead_contact_change_%1$s()', p_domain);

    RETURN format('Domain "%s": triggers successfully attached to existing tables.', p_domain);
END;
$$;

-- =============================================================================
-- D. Реестр доменов + Event Trigger (batch DDL safe)
-- =============================================================================
CREATE TABLE IF NOT EXISTS prod_sync.domain_registry (
    domain TEXT PRIMARY KEY,
    provisioned_at TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);
INSERT INTO prod_sync.domain_registry (domain) VALUES ('sigmasz') ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION prod_sync.auto_provision_domain() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
    obj RECORD; v_tbl TEXT; v_dom TEXT;
    v_domains_to_attach TEXT[] := ARRAY[]::TEXT[];
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF obj.object_type = 'table' AND obj.schema_name = 'airbyte_raw' AND obj.object_identity NOT LIKE '%custom_fields%' THEN
            IF obj.object_identity LIKE 'airbyte_raw.%_leads' THEN v_tbl := split_part(obj.object_identity, '.', 2); v_dom := replace(v_tbl, '_leads', '');
            ELSIF obj.object_identity LIKE 'airbyte_raw.%_contacts' THEN v_tbl := split_part(obj.object_identity, '.', 2); v_dom := replace(v_tbl, '_contacts', '');
            ELSIF obj.object_identity LIKE 'airbyte_raw.%_events' THEN v_tbl := split_part(obj.object_identity, '.', 2); v_dom := replace(v_tbl, '_events', '');
            ELSE CONTINUE; END IF;

            -- 1. Регистрируем и разворачиваем структуру, если домен новый
            IF NOT EXISTS (SELECT 1 FROM prod_sync.domain_registry WHERE domain = v_dom) THEN
                PERFORM prod_sync.setup_new_domain(v_dom);
                PERFORM prod_sync.setup_new_domain_functions(v_dom);
                INSERT INTO prod_sync.domain_registry (domain) VALUES (v_dom);
                RAISE NOTICE '[auto_provision] Domain "%" provisioned (tables + functions).', v_dom;
            END IF;

            -- 2. Добавляем в массив для привязки триггеров в конце функции
            IF NOT (v_dom = ANY(v_domains_to_attach)) THEN
                v_domains_to_attach := array_append(v_domains_to_attach, v_dom);
            END IF;
        END IF;
    END LOOP;

    -- Вызываем безопасную привязку триггеров один раз для каждого затронутого домена
    FOREACH v_dom IN ARRAY v_domains_to_attach LOOP
        PERFORM prod_sync.attach_domain_triggers(v_dom);
    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS trg_auto_provision_domain;
CREATE EVENT TRIGGER trg_auto_provision_domain ON ddl_command_end WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION prod_sync.auto_provision_domain();

-- =============================================================================
-- E. ПРИМЕНЕНИЕ для concepta и entrum
-- =============================================================================
SELECT prod_sync.setup_new_domain('concepta');
SELECT prod_sync.setup_new_domain_functions('concepta');
INSERT INTO prod_sync.domain_registry (domain) VALUES ('concepta') ON CONFLICT DO NOTHING;

SELECT prod_sync.setup_new_domain('entrum');
SELECT prod_sync.setup_new_domain_functions('entrum');
INSERT INTO prod_sync.domain_registry (domain) VALUES ('entrum') ON CONFLICT DO NOTHING;

COMMIT;

-- =============================================================================
-- ПОСЛЕ ПЕРВОЙ СИНХРОНИЗАЦИИ AIRBYTE (если event trigger не сработал):
-- SELECT prod_sync.attach_domain_triggers('concepta');
-- SELECT prod_sync.attach_domain_triggers('entrum');
-- =============================================================================
