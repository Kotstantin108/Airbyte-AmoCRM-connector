-- ============================================
-- СКРИПТ: ВКЛЮЧЕНИЕ ТРИГГЕРОВ (Запускать после Backfill)
-- ============================================
-- Этот скрипт создает все необходимые триггеры для автоматической
-- синхронизации новых данных.
-- ============================================

-- 1. Убеждаемся, что функция propagate_sigmasz_leads_l3 актуальна
CREATE OR REPLACE FUNCTION prod_sync.propagate_sigmasz_leads_l3()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM prod_sync.propagate_one_lead_to_l3(NEW.lead_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Функция обновления сделки при изменении её контактов
CREATE OR REPLACE FUNCTION prod_sync.trg_update_lead_on_contact_change()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE prod_sync.sigmasz_leads SET _synced_at = NOW() WHERE lead_id = NEW.lead_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Устанавливаем триггеры L1 -> L2 (Распаковка из airbyte_raw в prod_sync)
DROP TRIGGER IF EXISTS trg_sigmasz_leads_l2 ON airbyte_raw.sigmasz_leads;
CREATE TRIGGER trg_sigmasz_leads_l2 
AFTER INSERT ON airbyte_raw.sigmasz_leads 
FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_leads_l2();

DROP TRIGGER IF EXISTS trg_sigmasz_contacts_l2 ON airbyte_raw.sigmasz_contacts;
CREATE TRIGGER trg_sigmasz_contacts_l2 
AFTER INSERT ON airbyte_raw.sigmasz_contacts 
FOR EACH ROW EXECUTE FUNCTION airbyte_raw.unpack_sigmasz_contacts_l2();

-- Удаления через events
DROP TRIGGER IF EXISTS trg_propagate_deleted_l2 ON airbyte_raw.sigmasz_events;
CREATE TRIGGER trg_propagate_deleted_l2 
AFTER INSERT ON airbyte_raw.sigmasz_events 
FOR EACH ROW 
WHEN (NEW.type IN ('lead_deleted', 'contact_deleted')) 
EXECUTE FUNCTION airbyte_raw.propagate_deleted_to_l2();

-- 4. Триггеры L2 -> L3 (Наполнение чистовой аналитики)
DROP TRIGGER IF EXISTS trg_l2_l3_leads_sigmasz ON prod_sync.sigmasz_leads;
CREATE TRIGGER trg_l2_l3_leads_sigmasz 
AFTER INSERT OR UPDATE ON prod_sync.sigmasz_leads 
FOR EACH ROW EXECUTE FUNCTION prod_sync.propagate_sigmasz_leads_l3();

-- Контакты в отдельную L3 таблицу больше не пишем (она удалена)
DROP TRIGGER IF EXISTS trg_l2_l3_contacts_sigmasz ON prod_sync.sigmasz_contacts;

-- Обновление сделки при изменении её контактов
DROP TRIGGER IF EXISTS trg_lead_contacts_changed ON prod_sync.sigmasz_lead_contacts;
CREATE TRIGGER trg_lead_contacts_changed 
AFTER INSERT OR UPDATE ON prod_sync.sigmasz_lead_contacts 
FOR EACH ROW EXECUTE FUNCTION prod_sync.trg_update_lead_on_contact_change();

-- 5. Триггер Schema Sync (Авто-добавление колонок при появлении новых кастомных полей)
DROP TRIGGER IF EXISTS trg_schema_sigmasz_leads ON airbyte_raw.sigmasz_custom_fields_leads;
CREATE TRIGGER trg_schema_sigmasz_leads 
AFTER INSERT OR UPDATE ON airbyte_raw.sigmasz_custom_fields_leads 
FOR EACH ROW EXECUTE FUNCTION analytics.sync_schema_sigmasz_leads();

-- Контакты (удаляем, так как таблицы контактов в L3 больше нет)
DROP TRIGGER IF EXISTS trg_schema_sigmasz_contacts ON airbyte_raw.sigmasz_custom_fields_contacts;

-- 6. Финальная пересборка View (чтобы отражала новые колонки contact_id, contact_name и т.д.)
SELECT analytics.rebuild_view_sigmasz_leads();

DO $$ BEGIN RAISE NOTICE 'All triggers enabled successfully.'; END $$;

