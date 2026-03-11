-- ==============================================================================
-- МОНИТОРИНГ И АЛЕРТЫ ДЛЯ СИНХРОНИЗАЦИИ L2 → PRODUCTION
-- ==============================================================================
-- Этот файл содержит SQL-запросы и рекомендации для настройки мониторинга
-- Запустить на PRODUCTION-сервере
-- ==============================================================================

-- ==============================================================================
-- 1. ОБЗОРНЫЕ ДАШБОРДЫ
-- ==============================================================================

-- 1.1 Последний статус синхронизации по доменам
\echo '=== ДАШБОРД 1: Последний статус по доменам ==='
SELECT 
    domain,
    sync_type,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at))::INT as duration_seconds,
    leads_inserted, leads_updated, leads_deleted,
    contacts_ins, contacts_upd, contacts_del,
    CASE 
        WHEN status = 'success' THEN '✅'
        WHEN status = 'error' THEN '❌'
        WHEN status = 'running' THEN '⏳'
    END as status_icon,
    error_message
FROM amo_support_schema.sync_log
WHERE (domain, started_at) IN (
    SELECT domain, MAX(started_at) FROM amo_support_schema.sync_log GROUP BY domain
)
ORDER BY domain;

-- 1.2 История ошибок за последние 24 часа
\echo '=== ДАШБОРД 2: Ошибки синхронизации за 24 часа ==='
SELECT 
    domain,
    COUNT(*) as error_count,
    MIN(started_at) as first_error,
    MAX(started_at) as last_error,
    STRING_AGG(DISTINCT error_message, '; ' ORDER BY error_message) as error_messages
FROM amo_support_schema.sync_log
WHERE status = 'error' AND started_at > NOW() - INTERVAL '24 hours'
GROUP BY domain
HAVING COUNT(*) > 0
ORDER BY COUNT(*) DESC;

-- 1.3 Статистика синхронизированных данных
\echo '=== ДАШБОРД 3: Объём данных в Production ==='
SELECT 
    'concepta' as domain,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_leads) as leads_count,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_contacts) as contacts_count,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_contact_phones) as phones_count,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_contact_emails) as emails_count,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_leads WHERE is_deleted = TRUE) as deleted_leads
UNION ALL
SELECT 
    'entrum' as domain,
    (SELECT COUNT(*) FROM amo_support_schema.entrum_leads) as leads_count,
    (SELECT COUNT(*) FROM amo_support_schema.entrum_contacts) as contacts_count,
    (SELECT COUNT(*) FROM amo_support_schema.entrum_contact_phones) as phones_count,
    (SELECT COUNT(*) FROM amo_support_schema.entrum_contact_emails) as emails_count,
    (SELECT COUNT(*) FROM amo_support_schema.entrum_leads WHERE is_deleted = TRUE) as deleted_leads;

-- ==============================================================================
-- 2. КРИТИЧНЫЕ ПРОВЕРКИ (требуют срочного внимания)
-- ==============================================================================

-- 2.1 Проверка: Отсутствие успешной синхронизации более 3 часов
\echo '=== ПРОВЕРКА: Синхронизация более 3 часов не происходила ==='
SELECT 
    domain,
    MAX(started_at) as last_successful_sync,
    EXTRACT(HOURS FROM (NOW() - MAX(started_at)))::INT as hours_ago,
    CASE 
        WHEN EXTRACT(HOURS FROM (NOW() - MAX(started_at))) > 3 THEN '🔴 КРИТИЧНОЕ'
        WHEN EXTRACT(HOURS FROM (NOW() - MAX(started_at))) > 2 THEN '🟠 ВНИМАНИЕ'
        ELSE '✅ В норме'
    END as alert_level
FROM amo_support_schema.sync_log
WHERE status = 'success'
GROUP BY domain
ORDER BY hours_ago DESC;

-- 2.2 Проверка: Процесс stuck в статусе 'running'
\echo '=== ПРОВЕРКА: Зависший процесс синхронизации ==='
SELECT 
    domain, sync_type, status, started_at,
    EXTRACT(HOURS FROM (NOW() - started_at))::INT as hours_running,
    '🔴 КРИТИЧНОЕ - процесс завис!' as alert
FROM amo_support_schema.sync_log
WHERE status = 'running' AND started_at < NOW() - INTERVAL '2 hours'
ORDER BY started_at ASC;

-- 2.3 Проверка: Аномальное увеличение удалений
\echo '=== ПРОВЕРКА: Аномальное количество удалений (признак проблемы FDW) ==='
SELECT 
    domain,
    sync_type,
    started_at,
    leads_deleted + contacts_del as total_deleted,
    leads_inserted + contacts_ins + phones_ins + emails_ins + links_ins as total_inserted,
    CASE 
        WHEN leads_deleted > 100 OR contacts_del > 100 THEN '🔴 КРИТИЧНОЕ'
        WHEN leads_deleted > 10 OR contacts_del > 10 THEN '🟠 ВНИМАНИЕ'
        ELSE '✅ В норме'
    END as alert
FROM amo_support_schema.sync_log
WHERE started_at > NOW() - INTERVAL '7 days'
  AND (leads_deleted > 10 OR contacts_del > 10)
ORDER BY started_at DESC
LIMIT 20;

-- 2.4 Проверка: Потеря данных (увеличение deleted_entities_log)
\echo '=== ПРОВЕРКА: Растёт Dead Letter Queue (ошибки в триггерах) ==='
SELECT 
    COUNT(*) as dlq_rows,
    COUNT(*) FILTER (WHERE resolved = FALSE) as unresolved,
    STRING_AGG(DISTINCT stream_name, ', ' ORDER BY stream_name) as affected_streams,
    CASE 
        WHEN COUNT(*) > 1000 THEN '🔴 КРИТИЧНОЕ'
        WHEN COUNT(*) > 100 THEN '🟠 ВНИМАНИЕ'
        ELSE '✅ В норме'
    END as alert
FROM airbyte_raw.l2_dead_letter_queue
WHERE failed_at > NOW() - INTERVAL '24 hours';

-- ==============================================================================
-- 3. ПРЕДУПРЕЖДАЮЩИЕ ПРОВЕРКИ (требуют внимания в ближайшее время)
-- ==============================================================================

-- 3.1 Интервал между успешными синхронизациями растет
\echo '=== ПРОВЕРКА: Время между синхронизациями ==='
WITH sync_intervals AS (
    SELECT 
        domain,
        started_at,
        LAG(started_at) OVER (PARTITION BY domain ORDER BY started_at DESC) as prev_started,
        EXTRACT(MINUTES FROM (started_at - LAG(started_at) OVER (PARTITION BY domain ORDER BY started_at DESC)))::INT as minutes_since_last
    FROM amo_support_schema.sync_log
    WHERE status = 'success'
)
SELECT 
    domain,
    started_at,
    minutes_since_last,
    CASE 
        WHEN minutes_since_last > 120 THEN '🟠 СЛИШКОМ ДОЛГО'
        WHEN minutes_since_last > 70 THEN '🟡 ОЖИДАЕТСЯ'
        ELSE '✅ В норме'
    END as status
FROM sync_intervals
WHERE minutes_since_last IS NOT NULL
ORDER BY started_at DESC
LIMIT 10;

-- 3.2 Доля ошибок растет
\echo '=== ПРОВЕРКА: Процент ошибок за последние 7 дней ==='
SELECT 
    domain,
    COUNT(*) as total_syncs,
    COUNT(*) FILTER (WHERE status = 'success') as successful,
    COUNT(*) FILTER (WHERE status = 'error') as failed,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'error') / COUNT(*), 2) as error_rate_percent,
    CASE 
        WHEN 100.0 * COUNT(*) FILTER (WHERE status = 'error') / COUNT(*) > 10 THEN '🔴 КРИТИЧНОЕ'
        WHEN 100.0 * COUNT(*) FILTER (WHERE status = 'error') / COUNT(*) > 5 THEN '🟠 ВНИМАНИЕ'
        ELSE '✅ В норме'
    END as alert
FROM amo_support_schema.sync_log
WHERE started_at > NOW() - INTERVAL '7 days'
GROUP BY domain;

-- 3.3 Проверка производительности (длительность синхронизации)
\echo '=== ПРОВЕРКА: Время выполнения синхронизации ==='
SELECT 
    domain,
    sync_type,
    started_at,
    EXTRACT(SECONDS FROM (finished_at - started_at))::INT as duration_seconds,
    CASE 
        WHEN EXTRACT(SECONDS FROM (finished_at - started_at)) > 300 THEN '🟠 МЕДЛЕННО'
        ELSE '✅ Нормально'
    END as performance
FROM amo_support_schema.sync_log
WHERE status = 'success' AND started_at > NOW() - INTERVAL '7 days'
ORDER BY started_at DESC
LIMIT 20;

-- ==============================================================================
-- 4. ДЕТАЛЬНАЯ ДИАГНОСТИКА
-- ==============================================================================

-- 4.1 Последний лог синхронизации с полной информацией
\echo '=== ДЕТАЛЬ: Последний лог синхронизации ==='
SELECT 
    id, domain, sync_type, status, started_at, finished_at,
    EXTRACT(SECONDS FROM (finished_at - started_at))::INT as duration_sec,
    leads_inserted, leads_updated, leads_deleted,
    contacts_ins, contacts_upd, contacts_del,
    phones_ins, emails_ins, links_ins,
    error_message
FROM amo_support_schema.sync_log
WHERE (domain, started_at) IN (
    SELECT domain, MAX(started_at) FROM amo_support_schema.sync_log GROUP BY domain
)
ORDER BY domain;

-- 4.2 История ошибок с контекстом
\echo '=== ДЕТАЛЬ: Последние 10 ошибок ==='
SELECT 
    id, domain, sync_type, started_at, 
    error_message,
    CASE 
        WHEN error_message LIKE '%FDW%' THEN 'FDW Connection'
        WHEN error_message LIKE '%unique%' THEN 'Primary Key Conflict'
        WHEN error_message LIKE '%foreign key%' THEN 'Foreign Key Constraint'
        WHEN error_message LIKE '%data loss%' THEN 'Data Loss Detection'
        ELSE 'Other'
    END as error_category
FROM amo_support_schema.sync_log
WHERE status = 'error'
ORDER BY started_at DESC
LIMIT 10;

-- ==============================================================================
-- 5. РЕКОМЕНДУЕМЫЕ VIEW ДЛЯ МОНИТОРИНГА
-- ==============================================================================

-- Создаём view для упрощенного мониторинга
CREATE OR REPLACE VIEW v_sync_health AS
SELECT 
    domain,
    (SELECT started_at FROM amo_support_schema.sync_log 
     WHERE domain = sl.domain ORDER BY started_at DESC LIMIT 1) as last_sync,
    (SELECT status FROM amo_support_schema.sync_log 
     WHERE domain = sl.domain ORDER BY started_at DESC LIMIT 1) as last_status,
    (SELECT COUNT(*) FROM amo_support_schema.sync_log 
     WHERE domain = sl.domain AND status = 'error' AND started_at > NOW() - INTERVAL '24 hours') as errors_24h,
    CASE 
        WHEN (SELECT COUNT(*) FROM amo_support_schema.sync_log 
              WHERE domain = sl.domain AND status = 'error' AND started_at > NOW() - INTERVAL '24 hours') > 3 THEN 'RED'
        WHEN (SELECT COUNT(*) FROM amo_support_schema.sync_log 
              WHERE domain = sl.domain AND status = 'error' AND started_at > NOW() - INTERVAL '24 hours') > 0 THEN 'YELLOW'
        ELSE 'GREEN'
    END as health_status
FROM amo_support_schema.sync_log sl
GROUP BY domain;

-- 5.2 View для отслеживания потерь данных
CREATE OR REPLACE VIEW v_data_loss_indicators AS
SELECT 
    domain,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_leads WHERE domain = 'concepta') as total_leads,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_leads WHERE domain = 'concepta' AND is_deleted = TRUE) as deleted_leads,
    (SELECT COUNT(*) FROM amo_support_schema.concepta_contacts WHERE domain = 'concepta') as total_contacts,
    (SELECT COUNT(*) FROM airbyte_raw.l2_dead_letter_queue WHERE stream_name LIKE domain||'%' AND resolved = FALSE) as unresolved_dlq
FROM (SELECT DISTINCT domain FROM amo_support_schema.sync_log) t;

-- ==============================================================================
-- 6. СКРИПТ ДЛЯ АВТОМАТИЧЕСКОГО МОНИТОРИНГА (через cron/scheduler)
-- ==============================================================================

CREATE OR REPLACE FUNCTION amo_support_schema.check_sync_health()
RETURNS TABLE (check_name TEXT, status TEXT, severity TEXT, message TEXT) AS $$
BEGIN
    -- Проверка 1: Последняя синхронизация
    IF (SELECT MAX(started_at) FROM amo_support_schema.sync_log 
        WHERE status = 'success' AND domain = 'concepta') < NOW() - INTERVAL '3 hours' THEN
        RETURN QUERY SELECT 'Last sync (concepta)'::TEXT, 'FAILED'::TEXT, 'CRITICAL'::TEXT, 
            'No successful sync for 3+ hours'::TEXT;
    END IF;

    -- Проверка 2: Ошибки за последние 24 часа
    IF (SELECT COUNT(*) FROM amo_support_schema.sync_log 
        WHERE status = 'error' AND started_at > NOW() - INTERVAL '24 hours' AND domain = 'concepta') > 3 THEN
        RETURN QUERY SELECT 'Error rate (concepta)'::TEXT, 'WARNING'::TEXT, 'HIGH'::TEXT, 
            'More than 3 errors in 24 hours'::TEXT;
    END IF;

    -- Проверка 3: Зависший процесс
    IF EXISTS (SELECT 1 FROM amo_support_schema.sync_log 
               WHERE status = 'running' AND started_at < NOW() - INTERVAL '2 hours') THEN
        RETURN QUERY SELECT 'Stuck sync process'::TEXT, 'FAILED'::TEXT, 'CRITICAL'::TEXT, 
            'Process running for 2+ hours'::TEXT;
    END IF;

    -- Проверка 4: FDW доступность
    BEGIN
        PERFORM 1 FROM airbyte_remote.concepta_leads LIMIT 1;
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT 'FDW connectivity'::TEXT, 'FAILED'::TEXT, 'CRITICAL'::TEXT, 
            'Cannot connect to FDW server'::TEXT;
        RETURN;
    END;

    -- Если все проверки пройдены
    RETURN QUERY SELECT 'Overall health'::TEXT, 'OK'::TEXT, 'INFO'::TEXT, 'All systems operational'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- Использование:
-- SELECT * FROM amo_support_schema.check_sync_health();

-- ==============================================================================
-- 7. КОМАНДЫ ДЛЯ ИНТЕГРАЦИИ С СИСТЕМАМИ МОНИТОРИНГА
-- ==============================================================================

\echo ''
\echo '╔════════════════════════════════════════════════════════════════════════╗'
\echo '║          РЕКОМЕНДАЦИИ ДЛЯ ИНТЕГРАЦИИ С СИСТЕМАМИ МОНИТОРИНГА            ║'
\echo '╠════════════════════════════════════════════════════════════════════════╣'
\echo '║                                                                          ║'
\echo '║ 1. Grafana/Prometheus:                                                  ║'
\echo '║    - Создать метрики для каждого домена:                               ║'
\echo '║      * sync_duration_seconds                                            ║'
\echo '║      * sync_success_total                                               ║'
\echo '║      * sync_error_total                                                 ║'
\echo '║      * last_sync_timestamp                                              ║'
\echo '║                                                                          ║'
\echo '║ 2. Datadog / New Relic:                                                 ║'
\echo '║    - Использовать view v_sync_health для статусов                       ║'
\echo '║    - Alerting rule: health_status = RED → CRITICAL                      ║'
\echo '║                                                                          ║'
\echo '║ 3. PostgreSQL pg_cron:                                                  ║'
\echo '║    SELECT cron.schedule(''check-sync-health'',                           ║'
\echo '║        ''* * * * *'',  -- Каждую минуту                                  ║'
\echo '║        ''SELECT * FROM amo_support_schema.check_sync_health()'');        ║'
\echo '║                                                                          ║'
\echo '║ 4. Запрос для Elastic Alert:                                            ║'
\echo '║    SELECT * FROM amo_support_schema.sync_log                            ║'
\echo '║    WHERE status = ''error'' AND started_at > NOW() - INTERVAL ''1 hour''  ║'
\echo '║                                                                          ║'
\echo '╚════════════════════════════════════════════════════════════════════════╝'
\echo ''

-- ==============================================================================
-- 8. БЫСТРЫЕ КОМАНДЫ ДИАГНОСТИКИ
-- ==============================================================================

\echo ''
\echo '=== БЫСТРАЯ ДИАГНОСТИКА ==='
\echo 'Используйте эти команды для быстрой проверки:'
\echo ''
\echo '1. Состояние здоровья:'
\echo '   SELECT * FROM v_sync_health;'
\echo ''
\echo '2. Последние ошибки:'
\echo '   SELECT * FROM amo_support_schema.sync_log WHERE status = ''error'' ORDER BY started_at DESC LIMIT 5;'
\echo ''
\echo '3. Dead Letter Queue:'
\echo '   SELECT * FROM airbyte_raw.l2_dead_letter_queue WHERE resolved = FALSE;'
\echo ''
\echo '4. Проверить наличие зависшего процесса:'
\echo '   SELECT * FROM amo_support_schema.sync_log WHERE status = ''running'' AND started_at < NOW() - INTERVAL ''30 minutes'';'
\echo ''
\echo '5. Запустить проверку здоровья:'
\echo '   SELECT * FROM amo_support_schema.check_sync_health();'
\echo ''

