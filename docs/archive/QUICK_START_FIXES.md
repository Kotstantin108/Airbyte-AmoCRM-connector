# ⚡ БЫСТРОЕ РЕЗЮМЕ: Что нужно сделать ПРИ ВЫПОЛНЕНИИ ИСПРАВЛЕНИЙ

## 📊 Найдено проблем

| Критичность | Количество | Статус |
|-------------|-----------|--------|
| 🔴 Критичные | 4 | ⚠️ Требуют срочного решения |
| 🟠 Высокие | 3 | ⚠️ Требуют решения на неделе 1-2 |
| 🟡 Средние | 3 | ℹ️ Решить в месяц |
| 🟢 Низкие | 2 | ℹ️ Nice-to-have |

---

## 🚀 ПЛАН ДЕЙСТВИЙ (Пошаговый)

### ✅ ДЕНЬ 1: Проверка текущего состояния

```bash
# На Production сервере, подключитесь psql
psql -h <prod_host> -U postgres -d <prod_db>

# Проверка 1: Таблицы существуют?
\dt amo_support_schema.concepta_*
\dt amo_support_schema.entrum_*
# Если результат пустой → КРИТИЧНАЯ ПРОБЛЕМА #1

# Проверка 2: Primary Key есть?
SELECT constraint_name, constraint_type 
FROM information_schema.table_constraints
WHERE table_schema = 'amo_support_schema' 
  AND table_name IN ('concepta_leads', 'entrum_leads');
# Если результат пустой → КРИТИЧНАЯ ПРОБЛЕМА #3

# Проверка 3: FDW доступен?
SELECT count(*) FROM airbyte_remote.concepta_leads;
# Если ошибка → FDW недоступен
```

### ✅ ДЕНЬ 1-2: Применить исправления (КРИТИЧНЫЕ)

```bash
# На Production сервере:
psql -h <prod_host> -U postgres -d <prod_db> -f sql/CRITICAL_FIXES.sql

# Скрипт выполнит:
# ✓ Создание таблиц concepta_* и entrum_* (если нет)
# ✓ Добавление PRIMARY KEY (если нет)
# ✓ Создание новых функций _v2 с исправлениями
```

### ✅ ДЕНЬ 2: Обновить L2 (Analytics) сервер

```bash
# На Analytics (L2) сервере:
psql -h <analytics_host> -U postgres -d <analytics_db>

# Создать индексы по _synced_at (из CRITICAL_FIXES.sql):
CREATE INDEX idx_concepta_leads_synced_at ON prod_sync.concepta_leads (_synced_at);
CREATE INDEX idx_concepta_contacts_synced_at ON prod_sync.concepta_contacts (_synced_at);
CREATE INDEX idx_entrum_leads_synced_at ON prod_sync.entrum_leads (_synced_at);
CREATE INDEX idx_entrum_contacts_synced_at ON prod_sync.entrum_contacts (_synced_at);
```

### ✅ ДЕНЬ 3: Обновить расписание запусков

Замените вызовы функций:
```sql
-- СТАРО (в вашем расписании n8n/cron):
SELECT amo_support_schema.sync_concepta_smart();

-- НОВОЕ:
SELECT amo_support_schema.sync_concepta_smart_v2();
```

Аналогично для `sync_entrum_smart()` → `sync_entrum_smart_v2()`

### ✅ ДЕНЬ 4: Настроить мониторинг

```bash
# На Production:
psql -h <prod_host> -U postgres -d <prod_db> -f sql/MONITORING_AND_ALERTS.sql

# Это создаст:
# - View v_sync_health для состояния
# - Функцию check_sync_health() для проверок
# - Дашборды и запросы для мониторинга
```

### ✅ ДЕНЬ 5: Протестировать синхронизацию

```bash
# На Production:
# Запустить синхронизацию вручную
SELECT * FROM amo_support_schema.sync_concepta_smart_v2();

# Проверить результаты
SELECT * FROM amo_support_schema.sync_log 
WHERE domain = 'concepta' 
ORDER BY started_at DESC LIMIT 3;

# Должны быть записи:
# - status = 'success' (зелёно)
# - leads_inserted > 0 (данные попали)
# - No error_message (ошибок нет)
```

---

## 🎯 КРИТИЧНЫЕ ИЗМЕНЕНИЯ В КОДЕ

### Исправление #1: Увеличить интервал с 15 мин на 1 час

**БЫЛО:**
```sql
SELECT COALESCE(MAX(started_at) - INTERVAL '15 minutes', NOW() - INTERVAL '1 day')
```

**СТАЛО:**
```sql
SELECT COALESCE(MAX(started_at) - INTERVAL '1 hour', NOW() - INTERVAL '1 day')
```

**Где:** В функции `sync_concepta_smart_v2()` (строка ~80) и `sync_entrum_smart_v2()`

### Исправление #2: Проверка на % потерь вместо COUNT = 0

**БЫЛО:**
```sql
IF (SELECT COUNT(*) FROM _meta_leads) = 0 AND v_prod_leads_count > 100 THEN
```

**СТАЛО:**
```sql
IF v_prod_leads_count > 0 THEN
    v_loss_percent := 100.0 * (v_prod_leads_count - v_fdw_leads_count) / v_prod_leads_count;
    IF v_loss_percent > 20 THEN
        RAISE EXCEPTION 'FDW data loss detected: %.1f%%', v_loss_percent;
    END IF;
END IF;
```

**Где:** В функции `sync_concepta_smart_v2()` (строка ~105) и `sync_entrum_smart_v2()`

### Исправление #3: Проверка FDW перед началом

**Добавить в начало функции:**
```sql
BEGIN
    SELECT COUNT(*) INTO v_fdw_leads_count FROM airbyte_remote.concepta_leads;
EXCEPTION WHEN OTHERS THEN
    UPDATE amo_support_schema.sync_log SET 
        status = 'error',
        error_message = 'FDW unavailable: ' || SQLERRM
    WHERE id = v_log_id;
    RETURN QUERY SELECT ('ERROR: FDW unavailable')::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
    RETURN;
END;
```

---

## 📊 Что проверить после всех изменений

```sql
-- 1. Проверить здоровье синхронизации
SELECT * FROM amo_support_schema.check_sync_health();
-- Все должны быть 'OK'

-- 2. Проверить последний лог
SELECT domain, status, error_message, started_at 
FROM amo_support_schema.sync_log 
ORDER BY started_at DESC LIMIT 5;
-- Все status должны быть 'success'

-- 3. Проверить объём данных
SELECT COUNT(*) FROM amo_support_schema.concepta_leads;
SELECT COUNT(*) FROM amo_support_schema.entrum_leads;
-- Должны быть большие числа (>100)

-- 4. Проверить Dead Letter Queue
SELECT COUNT(*) FROM airbyte_raw.l2_dead_letter_queue WHERE resolved = FALSE;
-- Должно быть 0 (или минимум)
```

---

## ⚠️ ВАЖНО: Что не нужно делать

❌ **НЕ удаляйте старые функции** `sync_concepta_smart()` и `sync_entrum_smart()` сразу  
❌ **НЕ меняйте интервал без проверки** на FDW доступность  
❌ **НЕ игнорируйте ошибки** в логах — всегда проверяйте  
❌ **НЕ запускайте на продакшене без тестов** на staging

---

## 📚 Документация

Все подробности в файлах:

1. **`SYNCHRONIZATION_ANALYSIS_AND_ISSUES.md`** (20 KB)
   - Полный анализ каждой проблемы
   - Примеры эксплуатации
   - Рекомендации

2. **`CRITICAL_FIXES.sql`** (15 KB)
   - Готовые SQL-скрипты
   - Функции _v2 с исправлениями
   - Проверочные запросы

3. **`MONITORING_AND_ALERTS.sql`** (12 KB)
   - Дашборды и представления
   - Проверка здоровья
   - Интеграция с системами мониторинга

---

## 🆘 Если что-то пошло не так

### Ошибка: "relation does not exist"
```
→ Проблема: Таблицы concepta_* не созданы
→ Решение: Выполнить CRITICAL_FIXES.sql (раздел "СОЗДАНИЕ ТАБЛИЦ")
```

### Ошибка: "there is no unique or exclusion constraint"
```
→ Проблема: Отсутствует PRIMARY KEY на таблице
→ Решение: Выполнить CRITICAL_FIXES.sql (раздел "ДОБАВЛЕНИЕ PRIMARY KEY")
```

### Ошибка: "FDW returned 0 leads but PROD has X rows"
```
→ Проблема: FDW сервер недоступен или сломан
→ Решение: Проверить подключение к L2 (Analytics) серверу
→ Команда: psql -h <analytics_host> -c "SELECT 1"
```

### Синхронизация работает, но данные не попадают на Prod
```
→ Проблема: Может быть пропуск из-за clock skew
→ Решение: Проверить время на обоих серверах
→ Команда: SELECT NOW() на обоих серверах
→ Если разница > 30 сек → настроить NTP
```

---

## ✅ ФИНАЛЬНЫЙ ЧЕКЛИСТ

- [ ] Таблицы concepta_* и entrum_* созданы и видны через `\dt`
- [ ] PRIMARY KEY проверены и есть на всех таблицах
- [ ] Индексы по `_synced_at` созданы на L2
- [ ] Функции _v2 созданы и готовы к использованию
- [ ] Расписание синхронизации обновлено (используются _v2)
- [ ] Мониторинг настроен (таблица sync_log, функция check_sync_health)
- [ ] Первый запуск прошёл успешно (status = 'success')
- [ ] Данные видны в таблицах (COUNT(*) > 0)
- [ ] Нет ошибок в логах (sync_log.status = 'success')
- [ ] Dead Letter Queue пуст (count = 0)

---

## 📞 Поддержка и вопросы

Если остались вопросы по какой-либо проблеме:
1. Прочитайте раздел в `SYNCHRONIZATION_ANALYSIS_AND_ISSUES.md`
2. Выполните проверочные SQL запросы
3. Посмотрите в логах `amo_support_schema.sync_log`

**Статус:** Готов к применению. Сроки внедрения: 5 дней.

