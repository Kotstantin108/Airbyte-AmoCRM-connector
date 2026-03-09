# Углубленный анализ синхронизации L2 → Production
## Проверка требований безопасности и целостности данных

**Дата анализа:** 8 марта 2026  
**Статус:** Критические и потенциальные проблемы найдены

---

## 📋 АНАЛИТИКА ПРОВЕРЕННЫХ КОМПОНЕНТОВ

### Проверены файлы:
- ✅ `fdw_sync_multidomain.sql` (483 строк) — синхронизация L2 → Прод через FDW
- ✅ `dwh_sync_l1_l2_l3.sql` (519 строк) — L1 → L2 триггеры и Tombstone Shield
- ✅ `dwh_multidomain_core_part2.sql` (213 строк) — генераторы таблиц и функций
- ✅ `00_bootstrap_schemas_and_tables.sql` — инициализация схем (sigmasz домен)
- ✅ `fdw_sync_setup.sql` (568 строк) — конфигурация FDW

---

## 🔴 КРИТИЧЕСКИЕ ПРОБЛЕМЫ (ТРЕБУЮТ СРОЧНОГО РЕШЕНИЯ)

### **1. ПРОБЛЕМА: Несоответствие между доменами в таблицах amo_support_schema**

#### Симптом
В файлах обнаружено:
- `fdw_sync_multidomain.sql` использует таблицы: `amo_support_schema.concepta_leads`, `amo_support_schema.entrum_leads`
- `00_bootstrap_schemas_and_tables.sql` создаёт таблицы: `prod_sync.sigmasz_leads` (только sigmasz!)
- `fdw_sync_setup.sql` создаёт: `airbyte_sync.sigmasz_leads` (снова sigmasz)

```sql
-- fdw_sync_multidomain.sql (строка 148)
INSERT INTO amo_support_schema.concepta_leads (lead_id, name, ...)  ❌ ТАБЛИЦА НЕ СУЩЕСТВУЕТ

-- 00_bootstrap_schemas_and_tables.sql (строка 18)
CREATE TABLE IF NOT EXISTS prod_sync.sigmasz_leads (...)  ✓ только sigmasz
```

#### Риск
**🔥 КРИТИЧЕСКИЙ**: Синхронизация concepta и entrum падает с ошибкой "relation does not exist"

#### Действие
**Нужно убедиться**: Созданы ли таблицы `amo_support_schema.concepta_*` и `amo_support_schema.entrum_*` на PRODUCTION-сервере?

```bash
# На PRODUCTION-сервере (psql):
\dt amo_support_schema.concepta_*
\dt amo_support_schema.entrum_*
```

Если таблиц нет → нужно выполнить инициализацию или обновить скрипты bootstrap.

---

### **2. ПРОБЛЕМА: Недостаточная проверка целостности при Ghost Busting**

#### Текущее состояние (строка 107 в fdw_sync_multidomain.sql)
```sql
IF (SELECT COUNT(*) FROM _meta_leads) = 0 AND v_prod_leads_count > 100 THEN
    RAISE EXCEPTION 'FDW returned 0 leads...';
END IF;
```

#### Проблема
- ✅ Защита от полного нуля (0 строк в FDW)
- ❌ **НО**: Если FDW вернёт 5 строк вместо 5000 (потеря 99.9%), проверка НЕ сработает!
- ❌ Произойдет удаление 4995 лидов из продакшена **БЕЗ ОТКАТА**

#### Пример атаки (race condition)
```
Рабочее состояние: amo_support_schema.concepta_leads = 5000 строк
FDW обрывается на середине выборки: _meta_leads = 5 строк
v_prod_leads_count = 5000
Условие: (5 = 0) AND (5000 > 100) → FALSE
Результат: DELETE удалит 4995 лидов! ❌
```

#### Рекомендуемое исправление
```sql
-- Вариант A: Проверка по проценту потерь
DECLARE 
    v_loss_percent NUMERIC;
BEGIN
    v_loss_percent := 100.0 * (v_prod_leads_count - (SELECT COUNT(*) FROM _meta_leads)) / v_prod_leads_count;
    
    IF v_loss_percent > 20 THEN
        RAISE EXCEPTION 'FDW returned only % rows but PROD has %. Loss: %.1f%%. Aborting.',
            (SELECT COUNT(*) FROM _meta_leads), v_prod_leads_count, v_loss_percent;
    END IF;
END;

-- Вариант B: Абсолютный минимум
IF (SELECT COUNT(*) FROM _meta_leads) < GREATEST(100, v_prod_leads_count * 0.8) THEN
    RAISE EXCEPTION 'FDW data loss detected: % rows vs % expected',
        (SELECT COUNT(*) FROM _meta_leads), v_prod_leads_count;
END IF;
```

#### Срочность: 🔥 **КРИТИЧНАЯ** (изменить НЕМЕДЛЕННО)

---

### **3. ПРОБЛЕМА: Синхронизация времени (Clock Skew между L2 и Prod)**

#### Текущее состояние (строка 79, 297)
```sql
SELECT COALESCE(MAX(started_at) - INTERVAL '15 minutes', NOW() - INTERVAL '1 day')
INTO v_from_ts FROM amo_support_schema.sync_log WHERE status = 'success' AND domain = 'concepta';
```

#### Риск: Пропуск данных
Если время на сервере L2 спешит на 5 минут:
```
Prod sync_log.started_at = 10:00:00
L2 _synced_at = 10:04:59 (спешит)
v_from_ts = 10:00:00 - 15 мин = 09:45:00
Records в L2 с _synced_at = 09:50:00 попадают в инкремент ✓
Records в L2 с _synced_at = 10:05:00 будут пропущены в СЛЕДУЮЩЕМ инкременте! ❌
```

#### Рекомендуемое исправление
```sql
-- Увеличить интервал перекрытия с 15 минут до 1 часа
SELECT COALESCE(MAX(started_at) - INTERVAL '1 hour', NOW() - INTERVAL '1 day')
INTO v_from_ts FROM amo_support_schema.sync_log WHERE status = 'success' AND domain = 'concepta';
```

**Безопасность:** UPSERT (ON CONFLICT) защищает от дублей, поэтому перекрытие в 1 час — абсолютно безопасно.

#### Срочность: 🟠 **ВЫСОКАЯ** (изменить в ближайшее время)

---

## 🟡 ВЫСОКИЕ РИСКИ (ТРЕБУЮТ ВНИМАНИЯ)

### **4. ПРОБЛЕМА: Отсутствие проверки PRIMARY KEY на Production**

#### Проверка
В `fdw_sync_multidomain.sql` используется синтаксис:
```sql
INSERT INTO amo_support_schema.concepta_leads (...) VALUES (...)
ON CONFLICT (lead_id) DO UPDATE SET ...
```

Это требует PRIMARY KEY или UNIQUE INDEX по `lead_id`.

#### Как проверить на Production
```sql
-- На PRODUCTION-сервере:
SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_schema = 'amo_support_schema'
  AND table_name = 'concepta_leads'
  AND constraint_type IN ('PRIMARY KEY', 'UNIQUE');
```

#### Если ключа нет → Синхронизация ПАДАЕТ с ошибкой:
```
ERROR: there is no unique or exclusion constraint matching the ON CONFLICT specification
```

#### Создание недостающего ключа
```sql
-- На Production (если таблица существует без PK):
ALTER TABLE amo_support_schema.concepta_leads ADD PRIMARY KEY (lead_id);
ALTER TABLE amo_support_schema.entrum_leads ADD PRIMARY KEY (lead_id);
-- Аналогично для контактов:
ALTER TABLE amo_support_schema.concepta_contacts ADD PRIMARY KEY (contact_id);
ALTER TABLE amo_support_schema.entrum_contacts ADD PRIMARY KEY (contact_id);
```

#### Срочность: 🔥 **КРИТИЧНАЯ**

---

### **5. ПРОБЛЕМА: Отсутствие индекса на `_synced_at` в FDW таблицах**

#### Текущее состояние
В `fdw_sync_multidomain.sql` (строка 144-145):
```sql
SELECT lead_id, name, status_id, ... 
FROM airbyte_remote.concepta_leads 
WHERE _synced_at >= v_from_ts;
```

Если таблица на L2 (`prod_sync.concepta_leads`) не имеет индекса по `_synced_at`, запрос сканирует ВСЕ строки!

#### Проверка на L2-сервере
```sql
-- На ANALYTICS (L2) сервере:
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'concepta_leads' AND indexname LIKE '%_synced%';

-- Должны быть хотя бы индексы вроде:
-- idx_concepta_leads_wm (из dwh_multidomain_core_part2.sql, строка 28)
```

#### Если индекса нет
```sql
-- На L2:
CREATE INDEX idx_concepta_leads_synced_at ON prod_sync.concepta_leads (_synced_at);
CREATE INDEX idx_concepta_contacts_synced_at ON prod_sync.concepta_contacts (_synced_at);
CREATE INDEX idx_entrum_leads_synced_at ON prod_sync.entrum_leads (_synced_at);
CREATE INDEX idx_entrum_contacts_synced_at ON prod_sync.entrum_contacts (_synced_at);
```

#### Срочность: 🟠 **ВЫСОКАЯ** (влияет на производительность)

---

### **6. ПРОБЛЕМА: Отсутствие мониторинга ошибок синхронизации**

#### Текущее состояние
- ✅ Логируется статус в `sync_log` (success/error)
- ✅ Сохраняется `error_message` в синхронизацию
- ❌ **НО**: Нет механизма АЛЕРТА, если많이 ошибок подряд

#### Риск
```
Ночью начались ошибки синхронизации
День1: 1 ошибка (в логе) → данные L2 не попали на Прод
День2: 2 ошибки → ещё 2 дня без обновления
День3: Утром открывают багрепорт "Данные вчера не обновились"
```

#### Рекомендуемый мониторинг
```sql
-- Проверить количество ошибок в последние 24 часа
SELECT domain, COUNT(*) as error_count, MAX(started_at)
FROM amo_support_schema.sync_log
WHERE status = 'error' AND started_at > NOW() - INTERVAL '24 hours'
GROUP BY domain
HAVING COUNT(*) > 2;  -- Если более 2 ошибок подряд → ALERT
```

#### Срочность: 🟠 **ВЫСОКАЯ** (нужна система мониторинга)

---

## 🟡 СРЕДНИЕ РИСКИ (УЛУЧШЕНИЕ АРХИТЕКТУРЫ)

### **7. ПРОБЛЕМА: Нет проверки FDW связности перед Ghost Busting**

#### Текущее состояние
Ghost Busting начинается без предварительной проверки:
```sql
IF v_is_ghost_bust THEN
    -- Сразу создаём временные таблицы из FDW
    CREATE TEMP TABLE _meta_leads AS SELECT lead_id FROM airbyte_remote.concepta_leads;
```

Если FDW сервер недоступен → ошибка в середине функции, но `v_log_id` уже записан как "running"

#### Рекомендуемое исправление
```sql
-- Добавить предварительную проверку
DECLARE
    v_fdw_available BOOLEAN := FALSE;
BEGIN
    -- Проверить доступность FDW перед работой
    BEGIN
        EXECUTE 'SELECT 1 FROM airbyte_remote.concepta_leads LIMIT 1' INTO v_fdw_available;
        v_fdw_available := TRUE;
    EXCEPTION WHEN OTHERS THEN
        UPDATE amo_support_schema.sync_log SET 
            finished_at = NOW(), 
            status = 'error',
            error_message = 'FDW server unavailable: ' || SQLERRM
        WHERE id = v_log_id;
        PERFORM pg_advisory_unlock(hashtext('sync_concepta_smart'));
        RETURN QUERY SELECT ('ERROR: FDW unavailable')::TEXT, 0::BIGINT, 0::BIGINT, 0::BIGINT;
        RETURN;
    END;
```

#### Срочность: 🟡 **СРЕДНЯЯ**

---

### **8. ПРОБЛЕМА: Нет обработки edge case — строки со 100% одинаковыми данными**

#### Текущее состояние
В функции определение "updated" происходит через `xmax`:
```sql
WITH upserted AS (
    INSERT INTO amo_support_schema.concepta_leads (...)
    SELECT ... FROM _changed_leads
    ON CONFLICT (lead_id) DO UPDATE SET ...
    RETURNING (xmax = 0) AS is_new
) SELECT COUNT(*) FILTER (WHERE is_new), COUNT(*) FILTER (WHERE NOT is_new) INTO v_leads_ins, v_leads_upd FROM upserted;
```

#### Проблема
Если данные в L2 **полностью идентичны** данным на Prod:
- `xmax = 0` (новая строка) или `xmax != 0` (обновлена)
- Но это **не отражает реальность** — ничего не изменилось!

#### Рекомендуемое исправление
```sql
-- Отслеживать реальные изменения через ROW_COUNT
WITH upserted AS (
    INSERT INTO amo_support_schema.concepta_leads (...)
    SELECT ... FROM _changed_leads
    ON CONFLICT (lead_id) DO UPDATE SET ...
    WHERE EXCLUDED.updated_at > amo_support_schema.concepta_leads.updated_at  -- ← уже есть!
)
```

**✅ ХОРОШАЯ НОВОСТЬ**: В коде уже есть `WHERE EXCLUDED.updated_at >= amo_support_schema.concepta_leads.updated_at` (строка 156), это правильно!

#### Срочность: 🟢 **НИЗ** (уже реализовано)

---

### **9. ПРОБЛЕМА: Отсутствие обработки очень больших батчей (>200K строк)**

#### Текущее состояние
```sql
SELECT COALESCE(array_agg(lead_id), '{}'::BIGINT[]) INTO v_arr_leads FROM _changed_leads;

IF v_arr_cont_len > 5000 THEN
    -- Используем SELECT из временной таблицы вместо array_agg
    CREATE TEMP TABLE _changed_phones AS SELECT ... FROM airbyte_remote.concepta_contact_phones;
```

#### Риск
- Для телефонов и контактов есть проверка на 5000 строк (строка 199)
- ❌ **Но для лидов нет!** Если в одном инкременте > 100K лидов, `array_agg` может исчерпать память

#### Рекомендуемое исправление
```sql
-- Добавить проверку и для лидов/контактов
IF v_arr_leads_len > 10000 THEN
    -- Использовать SELECT вместо array_agg
    -- Переписать фильтры на основе временной таблицы
    CREATE TEMP TABLE _large_leads AS SELECT lead_id FROM _changed_leads;
    CREATE INDEX ON _large_leads(lead_id);
    -- Остальное как в текущем коде
END IF;
```

#### Срочность: 🟡 **СРЕДНЯЯ** (вероятность низкая, но опасна)

---

### **10. ПРОБЛЕМА: Нет таймаута на долгие транзакции**

#### Текущее состояние
Функция `sync_concepta_smart()` может работать сколь угодно долго без таймаута.

Если синхронизация повис:
- Advisory lock занимается неопределённо долго
- Следующий запуск синхронизации пропускается (из-за lock)
- Данные не обновляются, но логов об ошибке может и не быть

#### Рекомендуемое исправление
```sql
-- В начало функции добавить:
SET statement_timeout = '1 hour';  -- Таймаут на весь запрос

-- Или контролировать время выполнения:
DECLARE
    v_start_time TIMESTAMPTZ := NOW();
    v_max_duration INTERVAL := INTERVAL '50 minutes';
BEGIN
    -- ... основной код ...
    
    IF NOW() - v_start_time > v_max_duration THEN
        RAISE EXCEPTION 'Sync exceeded max duration of %', v_max_duration;
    END IF;
```

#### Срочность: 🟡 **СРЕДНЯЯ**

---

## 🟢 НИЗКИЕ РИСКИ (BEST PRACTICES)

### **11. РЕКОМЕНДАЦИЯ: Добавить версионирование синхронизации**

#### Идея
В таблице `sync_log` добавить номер версии схемы для контроля совместимости.

```sql
ALTER TABLE amo_support_schema.sync_log ADD COLUMN IF NOT EXISTS schema_version INT DEFAULT 1;
```

#### Срочность: 🟢 **НИЗ**

---

### **12. РЕКОМЕНДАЦИЯ: Логировать размер FDW выборки**

#### Текущее состояние
Логируется только количество inserted/updated, но не количество прочитанных из FDW.

```sql
-- Добавить в sync_log
ALTER TABLE amo_support_schema.sync_log ADD COLUMN IF NOT EXISTS fdw_rows_read BIGINT;

-- В функции:
SELECT COUNT(*) INTO v_fdw_rows_leads FROM _changed_leads;
-- ... потом сохранить ...
UPDATE amo_support_schema.sync_log SET fdw_rows_read = v_fdw_rows_leads WHERE id = v_log_id;
```

#### Срочность: 🟢 **НИЗ**

---

## 📊 СВОДНАЯ ТАБЛИЦА ПРОБЛЕМ

| # | Проблема | Критичность | Тип | Действие |
|---|----------|-------------|-----|---------|
| 1 | Несоответствие доменов (concepta vs sigmasz) | 🔥 КРИТИЧНАЯ | Конфигурация | Убедиться в создании таблиц amo_support_schema.* |
| 2 | Недостаточная проверка при Ghost Busting | 🔥 КРИТИЧНАЯ | Логика | Добавить проверку на % потерь данных |
| 3 | Clock Skew между L2 и Prod | 🟠 ВЫСОКАЯ | Конфигурация | Увеличить INTERVAL с 15 мин на 1 час |
| 4 | Отсутствие PRIMARY KEY на Prod | 🔥 КРИТИЧНАЯ | Конфигурация | Проверить и создать ключи |
| 5 | Отсутствие индекса _synced_at на L2 | 🟠 ВЫСОКАЯ | Производительность | Создать индексы |
| 6 | Нет мониторинга ошибок | 🟠 ВЫСОКАЯ | Мониторинг | Настроить система алертов |
| 7 | Нет проверки FDW связности | 🟡 СРЕДНЯЯ | Надёжность | Добавить pre-flight проверку |
| 8 | Edge case обновлений | 🟢 НИЗ | Логика | ✅ Уже реализовано |
| 9 | Риск overflow для больших батчей | 🟡 СРЕДНЯЯ | Производительность | Добавить проверку для лидов |
| 10 | Отсутствие таймаута | 🟡 СРЕДНЯЯ | Надёжность | Добавить statement_timeout |
| 11 | Версионирование схемы | 🟢 НИЗ | Best practice | Добавить поле schema_version |
| 12 | Логирование размера FDW | 🟢 НИЗ | Мониторинг | Добавить fdw_rows_read в лог |

---

## ✅ ЧЕК-ЛИСТ СРОЧНЫХ ДЕЙСТВИЙ

### Неделя 1 (Критичные):
- [ ] **Проверить** наличие таблиц `amo_support_schema.concepta_*` и `amo_support_schema.entrum_*` на Prod
- [ ] **Проверить** PRIMARY KEY на всех таблицах Prod
- [ ] **Исправить** логику Ghost Busting с проверкой на % потерь (вместо COUNT = 0)
- [ ] **Протестировать** FDW связность перед запуском синхронизации

### Неделя 2 (Высокие приоритеты):
- [ ] **Увеличить** INTERVAL с '15 minutes' на '1 hour' в обеих функциях (concepta и entrum)
- [ ] **Создать** индексы по `_synced_at` на L2 таблицах (если нет)
- [ ] **Настроить** monitoring и alerting для sync_log ошибок

### Неделя 3-4 (Средние приоритеты):
- [ ] **Добавить** pre-flight проверку доступности FDW
- [ ] **Добавить** обработку больших батчей (>10K лидов)
- [ ] **Добавить** statement_timeout и контроль длительности

### Месяц 2 (Nice-to-have):
- [ ] **Добавить** versioning в sync_log
- [ ] **Добавить** размер FDW выборки в логирование
- [ ] **Документировать** все changes в CHANGELOG.md

---

## 🔗 СВЯЗАННЫЕ ФАЙЛЫ ДЛЯ ИСПРАВЛЕНИЙ

Когда будете исправлять, обновите:
1. `sql/fdw_sync_multidomain.sql` — основная логика синхронизации
2. `sql/fdw_sync_setup.sql` — проверка наличия PRIMARY KEY
3. `sql/dwh_multidomain_core_part2.sql` — создание индексов
4. `sql/00_bootstrap_schemas_and_tables.sql` — инициализация на Prod (если используется)
5. Новый файл: `sql/monitoring_and_alerts.sql` — для мониторинга

---

## 📌 ВЫВОДЫ

**Позитивное:**
- ✅ Используются advisory locks (защита от параллельного запуска)
- ✅ Есть Dead Letter Queue для ошибок
- ✅ Tombstone Shield реализован правильно
- ✅ UPSERT с проверкой updated_at корректен
- ✅ Проверка на большие батчи (5000+) есть

**Критичное:**
- ❌ Проверка Ghost Busting недостаточна (может удалить 99% данных)
- ❌ Возможен пропуск данных из-за clock skew
- ❌ Может не быть PRIMARY KEY на Prod
- ❌ Нет мониторинга ошибок

**Рекомендация:** Выполните чек-лист срочных действий из раздела "ЧЕК-ЛИСТ" до развёртывания новых доменов в production.

