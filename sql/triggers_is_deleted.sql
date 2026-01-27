-- ============================================
-- Триггер и функции для обработки is_deleted
-- на основе events из AmoCRM
-- База: analytics, Схема: airbyte_raw
-- ============================================

-- Устанавливаем схему airbyte_raw
SET search_path TO airbyte_raw;

-- ============================================
-- 1. Функция для обновления is_deleted по одному event
-- ============================================
CREATE OR REPLACE FUNCTION airbyte_raw.process_deleted_event()
RETURNS TRIGGER AS $$
BEGIN
    -- Обрабатываем удаление лида
    IF NEW.type = 'lead_deleted' AND NEW.entity_type = 'lead' THEN
        UPDATE airbyte_raw.sigmasz_leads 
        SET is_deleted = true
        WHERE id = NEW.entity_id;
        
        RAISE NOTICE 'Marked lead % as deleted', NEW.entity_id;
    END IF;
    
    -- Обрабатываем удаление контакта
    IF NEW.type = 'contact_deleted' AND NEW.entity_type = 'contact' THEN
        UPDATE airbyte_raw.sigmasz_contacts 
        SET is_deleted = true
        WHERE id = NEW.entity_id;
        
        RAISE NOTICE 'Marked contact % as deleted', NEW.entity_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================
-- 2. Триггер на таблицу events
-- ============================================
DROP TRIGGER IF EXISTS trg_process_deleted_event ON airbyte_raw.sigmasz_events;

CREATE TRIGGER trg_process_deleted_event
    AFTER INSERT ON airbyte_raw.sigmasz_events
    FOR EACH ROW
    EXECUTE FUNCTION airbyte_raw.process_deleted_event();


-- ============================================
-- 3. Функция для массовой обработки ВСЕХ events
--    (вызывать после перезагрузки базы)
-- ============================================
CREATE OR REPLACE FUNCTION airbyte_raw.process_all_deleted_events()
RETURNS TABLE (
    processed_leads INT,
    processed_contacts INT
) AS $$
DECLARE
    v_leads_count INT := 0;
    v_contacts_count INT := 0;
BEGIN
    -- Проставляем is_deleted = true для лидов из events
    -- (только тем, у кого ещё не стоит true)
    WITH deleted_leads AS (
        UPDATE airbyte_raw.sigmasz_leads l
        SET is_deleted = true
        FROM airbyte_raw.sigmasz_events e
        WHERE e.type = 'lead_deleted' 
          AND e.entity_type = 'lead'
          AND e.entity_id = l.id
          AND (l.is_deleted IS NULL OR l.is_deleted = false)
        RETURNING l.id
    )
    SELECT COUNT(*) INTO v_leads_count FROM deleted_leads;
    
    -- Проставляем is_deleted = true для контактов из events
    WITH deleted_contacts AS (
        UPDATE airbyte_raw.sigmasz_contacts c
        SET is_deleted = true
        FROM airbyte_raw.sigmasz_events e
        WHERE e.type = 'contact_deleted' 
          AND e.entity_type = 'contact'
          AND e.entity_id = c.id
          AND (c.is_deleted IS NULL OR c.is_deleted = false)
        RETURNING c.id
    )
    SELECT COUNT(*) INTO v_contacts_count FROM deleted_contacts;
    
    RAISE NOTICE 'Marked as deleted: % leads, % contacts', v_leads_count, v_contacts_count;
    
    RETURN QUERY SELECT v_leads_count, v_contacts_count;
END;
$$ LANGUAGE plpgsql;


-- ============================================
-- 4. Пример использования
-- ============================================

-- После перезагрузки базы вызовите:
-- SELECT * FROM airbyte_raw.process_all_deleted_events();

-- Проверить количество удалённых:
-- SELECT COUNT(*) FROM airbyte_raw.sigmasz_leads WHERE is_deleted = true;
-- SELECT COUNT(*) FROM airbyte_raw.sigmasz_contacts WHERE is_deleted = true;

-- Проверить работу триггера:
-- SELECT * FROM airbyte_raw.sigmasz_events WHERE type IN ('lead_deleted', 'contact_deleted') LIMIT 10;


-- ============================================
-- 5. VIEW для активных (не удалённых) записей
-- ============================================

-- Создать view для активных лидов
CREATE OR REPLACE VIEW airbyte_raw.active_leads AS
SELECT l.*
FROM airbyte_raw.sigmasz_leads l
WHERE l.is_deleted IS NOT TRUE
  AND l.id NOT IN (
      SELECT entity_id FROM airbyte_raw.sigmasz_events 
      WHERE type = 'lead_deleted' AND entity_type = 'lead'
  );

-- Создать view для активных контактов  
CREATE OR REPLACE VIEW airbyte_raw.active_contacts AS
SELECT c.*
FROM airbyte_raw.sigmasz_contacts c
WHERE c.is_deleted IS NOT TRUE
  AND c.id NOT IN (
      SELECT entity_id FROM airbyte_raw.sigmasz_events 
      WHERE type = 'contact_deleted' AND entity_type = 'contact'
  );
