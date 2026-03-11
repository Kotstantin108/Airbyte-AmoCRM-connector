# Пошаговая инструкция: Исправление Tombstone Shield (таблицы уже в БД)

## Ситуация 📊
- ✅ Таблицы в `prod_sync` и `airbyte_raw` уже созданы
- ✅ Триггеры висят на таблицах
- ❌ `prod_sync.deleted_entities_log` ПУСТА
- ❌ Функции `register_tombstone()` и `is_tombstoned()` имеют конфликт сигнатур
- 🚀 **Нужно физическое удаление из L2 (prod_sync) и мягкое в L1 (airbyte_raw)**

## Решение: 3 простых шага

### Шаг 1️⃣ : Запустить скрипт исправления

```powershell
# Через PowerShell:
psql -U postgres -d your_database_name -f sql/FIX_TOMBSTONE_CONFLICT.sql -v ON_ERROR_STOP=1
```

Или через **pgAdmin**:
1. Откройте Query Tool
2. Скопируйте содержимое файла `FIX_TOMBSTONE_CONFLICT.sql`
3. Нажмите Execute (F5)
4. Проверьте результаты в сообщениях

**Этот скрипт:**
- ✅ Восстанавливает функции с ПРАВИЛЬНОЙ сигнатурой (3 параметра)
- ✅ Заполняет `deleted_entities_log` из таблиц `sigmasz_events`, `concepta_events`, `entrum_events`
- ✅ **ФИЗИЧЕСКИ удаляет записи из L2** (`prod_sync`) - каскадно со всеми связанными таблицами
- ✅ Мягко удаляет (is_deleted=TRUE) в L1 (`airbyte_raw`)
- ✅ Работает для всех трёх доменов: `sigmasz`, `concepta`, `entrum`

---

### Шаг 2️⃣ : Затем переапплицировать триггеры

```powershell
# Переоснаще триггеры с исправленными вызовами функций:
psql -U postgres -d your_database_name -f sql/dwh_sync_l1_l2_l3.sql -v ON_ERROR_STOP=1
```

Это нужно, чтобы триггеры знали о правильной сигнатуре функций.

---

### Шаг 3️⃣ : Проверить что всё работает

```sql
-- В pgAdmin Query Tool или psql выполните эти проверки:

-- 1. Проверить сигнатуры функций
SELECT proname, oid::regprocedure 
FROM pg_proc 
WHERE proname IN ('register_tombstone', 'is_tombstoned')
ORDER BY proname;

-- Должно быть:
-- register_tombstone | register_tombstone(text,text,bigint)
-- is_tombstoned      | is_tombstoned(text,text,bigint)

-- 2. Проверить количество удалённых записей в логе
SELECT 
    domain,
    entity_type,
    COUNT(*) as deleted_count
FROM prod_sync.deleted_entities_log
GROUP BY domain, entity_type
ORDER BY domain, entity_type;

-- 3. Проверить физическое удаление из L2 (должно быть много меньше, чем было)
SELECT 
    'sigmasz_leads' as table_name,
    COUNT(*) as remaining_records
FROM prod_sync.sigmasz_leads
UNION ALL
SELECT 'concepta_leads', COUNT(*) FROM prod_sync.concepta_leads
UNION ALL
SELECT 'entrum_leads', COUNT(*) FROM prod_sync.entrum_leads
UNION ALL
SELECT 'sigmasz_contacts', COUNT(*) FROM prod_sync.sigmasz_contacts
UNION ALL
SELECT 'concepta_contacts', COUNT(*) FROM prod_sync.concepta_contacts
UNION ALL
SELECT 'entrum_contacts', COUNT(*) FROM prod_sync.entrum_contacts;

-- 4. Проверить мягкое удаление в L1 (is_deleted=TRUE)
SELECT 
    'sigmasz_leads' as table_name,
    COUNT(*) as total,
    SUM(CASE WHEN is_deleted = TRUE THEN 1 ELSE 0 END) as marked_deleted
FROM airbyte_raw.sigmasz_leads
UNION ALL
SELECT 'concepta_leads', COUNT(*), SUM(CASE WHEN is_deleted = TRUE THEN 1 ELSE 0 END)
FROM airbyte_raw.concepta_leads
UNION ALL
SELECT 'entrum_leads', COUNT(*), SUM(CASE WHEN is_deleted = TRUE THEN 1 ELSE 0 END)
FROM airbyte_raw.entrum_leads;

-- 5. Проверить что триггер работает (новое удаление должно попасть в лог)
-- Вставьте тестовое событие для sigmasz:
INSERT INTO airbyte_raw.sigmasz_events (type, entity_type, entity_id, raw_json)
VALUES ('lead_deleted', 'lead', 999999999, '{}');

-- Проверьте что оно попало в deleted_entities_log:
SELECT * FROM prod_sync.deleted_entities_log 
WHERE entity_id = 999999999;

-- Проверьте что оно удалено из prod_sync.sigmasz_leads (если там была запись):
SELECT COUNT(*) FROM prod_sync.sigmasz_leads WHERE lead_id = 999999999;
-- Должно быть: 0

-- Если всё верно -> триггер РАБОТАЕТ ✅
-- Удалите тестовое событие:
DELETE FROM airbyte_raw.sigmasz_events WHERE entity_id = 999999999;
DELETE FROM prod_sync.deleted_entities_log WHERE entity_id = 999999999;
```

---

## Что происходит под капотом? 🔍

| Уровень | Операция | Результат |
|---------|----------|-----------|
| **L1 (airbyte_raw)** | Мягкое удаление | `is_deleted = TRUE` → история сохранена |
| **L2 (prod_sync)** | Физическое удаление | `DELETE FROM sigmasz_leads` → чистая витрина |
| **L3 (analytics)** | Синхронизация | Удаляются при следующем ETL |

---

## Что удаляется при физическом удалении L2? 🗑️

Для каждого удаленного **лида**:
- `prod_sync.sigmasz_lead_contacts` ← все контакты этого лида
- `prod_sync.sigmasz_leads` ← сам лид

Для каждого удаленного **контакта**:
- `prod_sync.sigmasz_contact_phones` ← все номера
- `prod_sync.sigmasz_contact_emails` ← все почты
- `prod_sync.sigmasz_lead_contacts` ← все связи с лидами
- `prod_sync.sigmasz_contacts` ← сам контакт

**То же самое для `concepta` и `entrum` доменов!**

---

## Что делать если ошибка?

### Ошибка: "ОШИБКА: функция не существует register_tombstone"
→ Скрипт `FIX_TOMBSTONE_CONFLICT.sql` создаёт функцию, это нормально

### Ошибка: "CONFLICT при вставке в deleted_entities_log"
→ Это нормально! `ON CONFLICT DO NOTHING` обрабатывает дубликаты

### Ошибка при запуске dwh_sync_l1_l2_l3.sql
→ Это ожидаемо, т.к. триггеры переоздаются. Ошибки типа "trigger already exists" - игнорируйте

### Лиды физически удалили из L2, но мне они еще нужны для аудита?
→ Они остаются в `airbyte_raw.sigmasz_leads` с флагом `is_deleted=TRUE` и в `deleted_entities_log` с timestamp

---

## Резюме исправлений

| Файл | Что изменилось |
|------|----------------|
| `FIX_TOMBSTONE_CONFLICT.sql` | ✨ Новый! Восстанавливает функции, заполняет лог, физически удаляет L2, мягко удаляет L1 |
| `dwh_sync_l1_l2_l3.sql` | ✅ Удалены дублирующие функции, обновлены вызовы на 3 параметра |
| `dwh_multidomain_core_part1.sql` | ✅ Источник истины для 3-параметрических функций |

---

## После исправления

✅ Новые удаления в AmoCRM → сразу в `deleted_entities_log`  
✅ **Физическое удаление из L2** (`prod_sync`)  
✅ **Мягкое удаление в L1** (`airbyte_raw`)  
✅ `propagate_deleted_to_l2()` триггер работает корректно для всех доменов  
✅ История аудита сохранена в `airbyte_raw` и `deleted_entities_log`

🎉 **Готово!**
