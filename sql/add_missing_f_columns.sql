-- ============================================
-- Добавление отсутствующих колонок f_* в analytics
-- ============================================
-- Если таблица airbyte_raw.sigmasz_custom_fields_* была заполнена до установки
-- триггеров sync_schema, в analytics.sigmasz_leads/contacts не создались колонки f_*.
-- Этот скрипт по данным из raw добавляет все недостающие колонки, пересобирает
-- view и заново прокидывает данные L2→L3, чтобы новые колонки заполнились.
--
-- Запуск: после 00_bootstrap, setup_from_scratch_normalized и первого backfill.
-- При большом объёме данных шаг «Перезаполнение L3» можно выполнить отдельно.

-- =============================================================================
-- 1. Добавить недостающие f_* в analytics.sigmasz_leads
-- =============================================================================
DO $$
DECLARE
  r RECORD;
  v_col_name TEXT;
  v_col_type TEXT;
  v_field_type TEXT;
BEGIN
  FOR r IN SELECT id, type FROM airbyte_raw.sigmasz_custom_fields_leads
  LOOP
    v_col_name := 'f_' || r.id::TEXT;
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'analytics' AND table_name = 'sigmasz_leads' AND column_name = v_col_name
    ) THEN
      v_field_type := COALESCE(r.type::TEXT, 'text');
      v_col_type := CASE
        WHEN v_field_type = 'numeric' THEN 'NUMERIC'
        WHEN v_field_type IN ('date', 'date_time', 'birthday') THEN 'TIMESTAMPTZ'
        WHEN v_field_type = 'checkbox' THEN 'BOOLEAN'
        ELSE 'TEXT'
      END;
      EXECUTE format('ALTER TABLE analytics.%I ADD COLUMN %I %s', 'sigmasz_leads', v_col_name, v_col_type);
      RAISE NOTICE 'Added column analytics.sigmasz_leads.%', v_col_name;
    END IF;
  END LOOP;
END $$;

SELECT analytics.rebuild_view_sigmasz_leads();

-- =============================================================================
-- 2. Перезаполнение L3 (чтобы новые колонки f_* получили значения)
-- =============================================================================
-- При большом объёме можно закомментировать и запустить отдельно:
--   SELECT prod_sync.propagate_one_lead_to_l3(lead_id) FROM prod_sync.sigmasz_leads;
SELECT prod_sync.propagate_one_lead_to_l3(lead_id) FROM prod_sync.sigmasz_leads;
