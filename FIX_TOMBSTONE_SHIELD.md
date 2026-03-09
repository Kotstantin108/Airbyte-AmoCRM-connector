# Исправление Tombstone Shield — deleted_entities_log пустой

## Проблема 🔴

После массового удаления лидов в AmoCRM, таблица `prod_sync.deleted_entities_log` осталась пустой, хотя:
- В `airbyte_raw.sigmasz_events` 32МБ записей про удаления
- Триггер `trg_propagate_deleted_to_l2` висит на таблице
- Функция `propagate_deleted_to_l2()` создана

**Причина:** Конфликт сигнатур функции `register_tombstone()`:
- `dwh_multidomain_core_part1.sql` определяет: `register_tombstone(domain TEXT, entity_type TEXT, entity_id BIGINT)`
- `dwh_sync_l1_l2_l3.sql` переопределял: `register_tombstone(entity_type TEXT, entity_id BIGINT)` ❌
- Последний выигрывает → функция перебита на старую сигнатуру
- Вызовы с 3 параметрами падают молча (Exception WHEN OTHERS)

## Решение ✅

### Шаг 1: Переупроверить порядок выполнения SQL

**ОБЯЗАТЕЛЬНЫЙ порядок:**

```bash
1. sql/00_bootstrap_schemas_and_tables.sql
2. sql/dwh_multidomain_core_part1.sql         ← регистрирует функции с domain параметром
3. sql/dwh_multidomain_core_part2.sql
4. sql/dwh_sync_l1_l2_l3.sql                   ← NOT переопределяет функции, а использует
5. sql/backfill_initial_normalized.sql
```

### Шаг 2: Применить исправления

Все исправления уже внесены в файлы:

✅ **`dwh_sync_l1_l2_l3.sql`:**
- Удалены дублирующие функции `register_tombstone()` и `is_tombstoned()`
- Все вызовы обновлены на 3-параметрические версии:
  - `is_tombstoned('contact', id)` → `is_tombstoned('sigmasz', 'contact', id)`
  - `register_tombstone('lead', id)` → `register_tombstone('sigmasz', 'lead', id)`

✅ **`RUN_ORDER_NORMALIZED.md`:**
- Обновлена документация с правильным порядком выполнения
- Добавлены части 1 и 2 в порядок

### Шаг 3: Переапплицировать скрипты в БД

Если скрипты уже запущены:

```sql
-- 1. Сначала переисполнить dwh_multidomain_core_part1.sql
--    (он уже в порядке, дропает функцию с 2 параметрами)

-- 2. Затем ПОВТОРНО исполнить dwh_sync_l1_l2_l3.sql
--    (теперь функции не будут перебиты)

-- 3. Проверить что функции правильные:
SELECT proname, oid::regprocedure 
FROM pg_proc 
WHERE proname IN ('register_tombstone', 'is_tombstoned');

-- Должно быть:
-- register_tombstone | register_tombstone(text,text,bigint)
-- is_tombstoned      | is_tombstoned(text,text,bigint)
```

### Шаг 4: Очистить стопроцентно старые удаления

Если данные уже в 2026 году, а deleted_entities_log пустой → нужно переобработать события вручную:

```sql
-- Воспроизвести удаления из события (для sigmasz домена)
INSERT INTO prod_sync.deleted_entities_log (domain, entity_type, entity_id, deleted_at, _synced_at)
SELECT 
    'sigmasz',
    NEW.entity_type,
    NEW.entity_id,
    NOW(),
    NOW()
FROM airbyte_raw.sigmasz_events
WHERE (NEW.type = 'lead_deleted' AND NEW.entity_type = 'lead')
   OR (NEW.type = 'contact_deleted' AND NEW.entity_type = 'contact')
ON CONFLICT (domain, entity_type, entity_id) DO NOTHING;

-- И пересинк таблицы L2
UPDATE prod_sync.sigmasz_leads 
SET is_deleted = TRUE, _synced_at = NOW()
WHERE lead_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'lead'
);

UPDATE prod_sync.sigmasz_contacts 
SET is_deleted = TRUE, _synced_at = NOW()
WHERE contact_id IN (
    SELECT entity_id FROM prod_sync.deleted_entities_log 
    WHERE domain = 'sigmasz' AND entity_type = 'contact'
);
```

## Проверка ✅

После применения исправлений:

```sql
-- 1. Функции имеют правильную сигнатуру
SELECT oid::regprocedure FROM pg_proc WHERE proname = 'register_tombstone';
-- Output: register_tombstone(text,text,bigint)

-- 2. В deleted_entities_log появляются записи
SELECT * FROM prod_sync.deleted_entities_log 
ORDER BY deleted_at DESC LIMIT 5;

-- 3. Новые удаления немедленно попадают туда
DELETE FROM airbyte_raw.sigmasz_leads WHERE id = 12345;
-- (или Airbyte закидает событие lead_deleted)
-- → должна появиться строка в deleted_entities_log

-- 4. L2 таблицы синхронизируются
SELECT COUNT(*) FROM prod_sync.sigmasz_leads WHERE is_deleted = TRUE;
```

## Итог

**Баг был в дублировании функций с разными сигнатурами.** Исправления уже применены в файлах. При следующем запуске SQL скриптов триггеры будут работать корректно и регистрировать удаления в `deleted_entities_log`.
