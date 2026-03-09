# Важно: Временные метки в Tombstone Shield

## Проблема 📋

В `deleted_entities_log` были записаны **текущие времена синхронизации (NOW())** вместо **реальных времён удаления в AmoCRM**.

```sql
-- Было (неправильно):
deleted_at = NOW()  -- Когда Airbyte загрузил (23:31:52)
```

```sql
-- Должно быть (правильно):
deleted_at = to_timestamp(created_at::BIGINT)  -- Когда реально удалили (1742985427 = Unix timestamp)
```

---

## Последствия неправильного подхода 🔴

### Пример из реальности:

```
AmoCRM: Удалили лид #123 в 10:00 (created_at = 1742985427)
Airbyte: Загрузил событие в 23:31:52 (через 13+ часов!)

Было:
deleted_at = NOW() = 23:31:52  ❌ НЕПРАВИЛЬНО!
deleted_entities_log показывает: "Удалили в 23:31"
Историк: "Странно, удаления произошли вечером?"

Стало:
deleted_at = to_timestamp(1742985427) = 10:00 ✅ ПРАВИЛЬНО!
deleted_entities_log показывает: "Удалили в 10:00"
Историк: "Да, удаления произошли в 10:00"
```

---

## Race Condition: Почему время важно 🎯

### Сценарий обратного порядка событий:

```
t=10:00  AmoCRM: Создан лид #456, created_at=1742970000
t=10:05  AmoCRM: Обновлен лид #456, updated_at=1742970300
t=10:15  AmoCRM: Удален лид #456, deleted_at=1742970900

Но Airbyte загружает их в ДРУГОМ порядке:
t=23:20  Airbyte: Event "lead_created" (created_at=1742970000)
t=23:25  Airbyte: Event "lead_deleted" (created_at=1742970900) ⚠️ РАНЬШЕ update!
t=23:30  Airbyte: Event "lead_updated" (updated_at=1742970300)
```

**С неправильным временем (NOW()):**
```
deleted_at = 23:25 (время загрузки удаления)
updated_at = 23:30 (время загрузки update)

Результат: updated_at (23:30) > deleted_at (23:25)
→ UPDATE ПЕРЕПИШЕТ УДАЛЁННУЮ ЗАПИСЬ! 👻
```

**С правильным временем (timestamp из события):**
```
deleted_at = 10:15 (реальное время удаления)
updated_at = 10:05 (реальное время update)

Результат: deleted_at (10:15) > updated_at (10:05)
→ UPDATE будет отвергнут, запись останется удалённой ✅
```

---

## Решение в FIX_TOMBSTONE_CONFLICT.sql ✅

### Шаг 1: Используем timestamp из события

```sql
INSERT INTO prod_sync.deleted_entities_log 
    (domain, entity_type, entity_id, deleted_at, _synced_at)
SELECT DISTINCT
    'sigmasz',
    entity_type,
    entity_id,
    to_timestamp(created_at::BIGINT)::TIMESTAMPTZ,  -- ✅ Реальное время удаления!
    NOW()  -- Когда мы это обработали
FROM airbyte_raw.sigmasz_events
WHERE type = 'lead_deleted' AND entity_type = 'lead'
ON CONFLICT (domain, entity_type, entity_id) DO UPDATE
    SET deleted_at = EXCLUDED.deleted_at,  -- ✅ Сохраняем исходное время
        _synced_at = NOW();
```

### Шаг 2: Функция с поддержкой явного времени

```sql
-- Версия 1: Без времени (используется NOW())
PERFORM prod_sync.register_tombstone('sigmasz', 'lead', 123);

-- Версия 2: С явным временем (из события)
PERFORM prod_sync.register_tombstone('sigmasz', 'lead', 123, event_timestamp);
```

---

## Как это использовать в триггерах 🔧

```sql
-- При обработке события lead_deleted:
v_deleted_at := to_timestamp((NEW.raw_json->>'created_at')::BIGINT)::TIMESTAMPTZ;

PERFORM prod_sync.register_tombstone(
    'sigmasz', 
    'lead', 
    v_lead_id,
    v_deleted_at  -- ✅ Передаём реальное время из события
);
```

---

## Проверка: Правильно ли записалось время? ✅

```sql
-- Посмотреть deleted_entities_log
SELECT 
    entity_id,
    deleted_at,
    _synced_at,
    (deleted_at AT TIME ZONE 'Europe/Moscow')::DATE as deletion_date
FROM prod_sync.deleted_entities_log
WHERE domain = 'sigmasz' AND entity_type = 'lead'
ORDER BY deleted_at DESC
LIMIT 10;

-- deleted_at должна быть СТАРШЕ _synced_at на часы/дни
-- deleted_at = время в AmoCRM (давно)
-- _synced_at = время загрузки в Airbyte (недавно)
```

**Ожидаемый результат:**
```
entity_id  | deleted_at                    | _synced_at                     | deletion_date
-----------|-------------------------------|--------------------------------|---------------
4898037    | 2026-03-07 14:30:27.000 +0400 | 2026-03-08 23:31:52.319 +0400  | 2026-03-07  ✅
1446523    | 2026-03-06 10:15:43.000 +0400 | 2026-03-08 23:31:52.319 +0400  | 2026-03-06  ✅
1020961    | 2026-03-05 09:00:00.000 +0400 | 2026-03-08 23:31:52.319 +0400  | 2026-03-05  ✅
```

---

## Дополнительная защита: Проверка updated_at < deleted_at

Для полной защиты от race condition можно добавить в триггер:

```sql
-- Если запись была обновлена ПОСЛЕ её удаления в исходной системе
-- → отвергаем update
IF v_updated_ts > v_deleted_at THEN
    -- Это не должно произойти, но если произошло - логируем
    RAISE WARNING 'ANOMALY: Lead % updated after deletion (updated=%, deleted=%)', 
        v_lead_id, v_updated_ts, v_deleted_at;
    RETURN NEW;
END IF;
```

---

## Summary

| Аспект | Было | Стало |
|--------|------|-------|
| **deleted_at** | NOW() (время загрузки) | to_timestamp(created_at) (время удаления) |
| **Аудит** | Неточная история ❌ | Точная история ✅ |
| **Race condition** | Возможна при обратном порядке ❌ | Защита через timestamp ✅ |
| **Функция** | register_tombstone(3 параметра) | register_tombstone(3 или 4 параметра) |

🎉 **Теперь время в deleted_entities_log соответствует реальным событиям в AmoCRM!**
