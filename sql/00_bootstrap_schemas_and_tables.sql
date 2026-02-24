-- ============================================
-- Базовые объекты ДО триггеров (после сноса БД)
-- ============================================
-- Запустите первым. Схему airbyte_raw и таблицы в ней создаёт Airbyte при синхронизации.
-- Этот скрипт создаёт: prod_sync, analytics, таблицы L2/L3, normalize_phone(), process_embedded_contacts().
-- После него: sql/setup_from_scratch_normalized.sql → sql/backfill_initial_normalized.sql

-- =============================================================================
-- 1. Схемы
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS prod_sync;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- 2. L2: таблицы prod_sync
-- =============================================================================
CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_leads (
    lead_id BIGINT PRIMARY KEY,
    name TEXT,
    status_id INT,
    pipeline_id INT,
    price NUMERIC,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_l2_time ON prod_sync.sigmasz_leads(_synced_at);
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_l2_updated ON prod_sync.sigmasz_leads(updated_at);

CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_contacts (
    contact_id BIGINT PRIMARY KEY,
    name TEXT,
    updated_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sigmasz_contacts_l2_time ON prod_sync.sigmasz_contacts(_synced_at);

CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_lead_contacts (
    lead_id BIGINT NOT NULL,
    contact_id BIGINT NOT NULL,
    PRIMARY KEY (lead_id, contact_id),
    FOREIGN KEY (lead_id) REFERENCES prod_sync.sigmasz_leads(lead_id) ON DELETE CASCADE,
    FOREIGN KEY (contact_id) REFERENCES prod_sync.sigmasz_contacts(contact_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_sigmasz_lead_contacts_contact ON prod_sync.sigmasz_lead_contacts(contact_id);

CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_contact_phones (
    contact_id BIGINT NOT NULL,
    phone TEXT NOT NULL,
    PRIMARY KEY (contact_id, phone),
    FOREIGN KEY (contact_id) REFERENCES prod_sync.sigmasz_contacts(contact_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_contact_emails (
    contact_id BIGINT NOT NULL,
    email TEXT NOT NULL,
    PRIMARY KEY (contact_id, email),
    FOREIGN KEY (contact_id) REFERENCES prod_sync.sigmasz_contacts(contact_id) ON DELETE CASCADE
);

-- =============================================================================
-- 3. Вспомогательные функции (нужны триггерам)
-- =============================================================================
CREATE OR REPLACE FUNCTION normalize_phone(phone_text TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN regexp_replace(phone_text, '[^0-9+]', '', 'g');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION prod_sync.process_embedded_contacts(
    p_contacts_json JSONB,
    p_lead_id BIGINT
)
RETURNS VOID AS $$
DECLARE
    v_contact JSONB;
    v_contact_id BIGINT;
BEGIN
    FOR v_contact IN SELECT * FROM jsonb_array_elements(p_contacts_json)
    LOOP
        v_contact_id := (v_contact ->> 'id')::BIGINT;
        INSERT INTO prod_sync.sigmasz_contacts (contact_id, name, updated_at, raw_json, is_deleted, _synced_at)
        VALUES (
            v_contact_id,
            v_contact ->> 'name',
            to_timestamp(NULLIF(v_contact ->> 'updated_at', '')::BIGINT),
            v_contact,
            FALSE,
            NOW()
        )
        ON CONFLICT (contact_id) DO UPDATE SET
            name = EXCLUDED.name,
            updated_at = EXCLUDED.updated_at,
            _synced_at = NOW();
        INSERT INTO prod_sync.sigmasz_lead_contacts (lead_id, contact_id)
        VALUES (p_lead_id, v_contact_id)
        ON CONFLICT DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 4. L3: таблицы analytics (базовые колонки; f_* добавит sync_schema)
-- =============================================================================
CREATE TABLE IF NOT EXISTS analytics.sigmasz_leads (
    lead_id BIGINT PRIMARY KEY,
    name TEXT,
    status_id INT,
    pipeline_id INT,
    price NUMERIC,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    is_deleted BOOLEAN DEFAULT FALSE,
    _synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sigmasz_leads_l3_time ON analytics.sigmasz_leads(_synced_at);

