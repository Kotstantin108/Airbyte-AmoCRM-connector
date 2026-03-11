# Порядок запуска скриптов (нормализованные таблицы Airbyte)

У вас данные в **колонках** (id, name, _embedded, custom_fields_values в leads/contacts; id, name, type в custom_fields), а не в одной _airbyte_data/_airbyte_meta.

---

## Установка с нуля (после сноса БД)

**Порядок запуска:**

| Шаг | Скрипт | Что делает |
|-----|--------|------------|
| **1** | **`sql/00_bootstrap_schemas_and_tables.sql`** | Создаёт схемы `prod_sync`, `analytics`; таблицы L2 и L3; функции `normalize_phone()`, `process_embedded_contacts()`. |
| **2** | **`sql/dwh_multidomain_core_part1.sql`** | Создаёт многодоменную инфраструктуру: расширяет `deleted_entities_log` с колонкой `domain`, переопределяет функции `register_tombstone(domain, entity_type, entity_id)` и `is_tombstoned(domain, entity_type, entity_id)`. |
| **3** | **`sql/dwh_multidomain_core_part2.sql`** | Генераторы и авто-обнаружение для новых доменов. |
| **4** | **`sql/dwh_sync_l1_l2_l3.sql`** | Создаёт все триггеры L1→L2 и L2→L3, вспомогательные функции. Использует функции из части 1. |
| **5** | **`sql/backfill_initial_normalized.sql`** | Переносит уже существующие данные из L1 в L2 и L3. |

```bash
psql -f sql/00_bootstrap_schemas_and_tables.sql
psql -f sql/dwh_multidomain_core_part1.sql
psql -f sql/dwh_multidomain_core_part2.sql
psql -f sql/dwh_sync_l1_l2_l3.sql
psql -f sql/backfill_initial_normalized.sql
```

**Важно:** Схему `airbyte_raw` и таблицы в ней создаёт **Airbyte** при первой синхронизации. Перед шагом 2 убедитесь, что в БД есть схема `airbyte_raw` и таблицы `sigmasz_leads`, `sigmasz_contacts`, `sigmasz_events`, `sigmasz_custom_fields_leads`, `sigmasz_custom_fields_contacts` (запустите синк Airbyte хотя бы один раз).

---

## Если backfill шёл с ошибками или поля пустые

1. **Прервать** текущий backfill (Ctrl+C или завершить сессию).
2. **Применить исправления:** `psql -f sql/setup_from_scratch_normalized.sql` (подтянет безопасное преобразование дат и пересоздаст функции).
3. **Запустить backfill на ночь:** `psql -f sql/backfill_initial_normalized.sql`.  
   Backfill теперь в две фазы: сначала заполняется prod_sync (без триггеров L2→L3), затем analytics; в конце триггеры включаются обратно.

---

## Если в analytics нет колонок f_*

Если таблицы `airbyte_raw.sigmasz_custom_fields_leads` и `sigmasz_custom_fields_contacts` были заполнены **до** установки триггеров `sync_schema`, в `analytics.sigmasz_leads` и `analytics.sigmasz_contacts` не создались колонки `f_*`. Один раз выполните:

```bash
psql -f sql/add_missing_f_columns.sql
```

Скрипт добавляет все недостающие колонки по данным из raw, пересобирает view и заново прокидывает данные L2→L3. При большом объёме данных последние две строки (перезаполнение L3) можно закомментировать в скрипте и выполнить отдельно.

---

## Пересоздание триггеров (если таблицы уже есть)

Если нужно пересоздать только триггеры и функции (таблицы L2/L3 уже существуют):

```bash
psql -f sql/setup_from_scratch_normalized.sql
```

После этого при необходимости запустите backfill:

```bash
psql -f sql/backfill_initial_normalized.sql
```

---

## Дополнительные скрипты

- **`sql/add_missing_f_columns.sql`** — добавить отсутствующие колонки `f_*` в analytics и перезаполнить L3 (если custom_fields загрузились до установки триггеров).
- **`sql/drop_all_triggers.sql`** — удаление всех триггеров и функций (для полного сброса перед переустановкой).
- **`sql/fdw_sync_setup.sql`** — настройка FDW для синхронизации prod_sync → прод БД (если нужно).
- **`sql/logical_replication_setup.sql`** — настройка логической репликации (альтернатива FDW).
- **`sql/create_readonly_user.sql`**, **`sql/create_superadmin.sql`** — создание пользователей БД.

---

## Проверка после установки

После выполнения всех шагов:

- ✅ Новые строки в `airbyte_raw.sigmasz_leads` / `sigmasz_contacts` автоматически уходят в `prod_sync` (триггеры L1→L2).
- ✅ Данные из `prod_sync` автоматически попадают в `analytics` (триггеры L2→L3).
- ✅ Таблицы `prod_sync.sigmasz_leads`, `prod_sync.sigmasz_contacts` и `analytics.sigmasz_leads`, `analytics.sigmasz_contacts` заполнены (backfill из шага 3).
- ✅ View `analytics.sigmasz_leads_human` и `analytics.sigmasz_contacts_human` открываются без ошибки и показывают колонки f_* с человеческими именами из custom_fields.
- ✅ Удалённые сделки/контакты не хранятся в `prod_sync` и `analytics` (физически удаляются при обнаружении удаления).

---

## Для аналитиков: пустые поля (NULL)

В части кастомных полей типа «дата» в источнике бывают некорректные значения (числа вне диапазона дат или не даты). Для таких значений в L3 пишется **NULL** — поле не теряется, но дата не заполняется. Остальные поля (название, статус, воронка, бюджет, валидные даты и текстовые кастомные поля) заполняются полностью. При необходимости можно доразобрать исходные значения в AmoCRM и поправить маппинг.

---

## Кастомные поля и телефоны/почты по сделке

- **Кастомные поля (f_*)** в `analytics.sigmasz_leads` заполняются из `prod_sync.sigmasz_leads.raw_json -> custom_fields_values`. В коде поддерживаются ключи **`field_id`** и **`id`** (AmoCRM может отдавать любой из них). После правок перезапустите `setup_from_scratch_normalized.sql` и при необходимости backfill или пересчёт: `SELECT prod_sync.propagate_one_lead_to_l3(lead_id) FROM prod_sync.sigmasz_leads;`.
- **Телефоны и почты** хранятся у **контактов**: `prod_sync.sigmasz_contact_phones`, `prod_sync.sigmasz_contact_emails`. По сделке их можно получить через связь сделка–контакт. Пример view (запустить при необходимости):

```sql
CREATE OR REPLACE VIEW analytics.sigmasz_leads_with_phones AS
SELECT l.lead_id, l.name AS lead_name, l.status_id, l.pipeline_id, l.price, l.created_at, l.updated_at,
       c.contact_id, c.name AS contact_name,
       cp.phone, ce.email
FROM analytics.sigmasz_leads l
JOIN prod_sync.sigmasz_lead_contacts lc ON lc.lead_id = l.lead_id
JOIN prod_sync.sigmasz_contacts c ON c.contact_id = lc.contact_id
LEFT JOIN prod_sync.sigmasz_contact_phones cp ON cp.contact_id = c.contact_id
LEFT JOIN prod_sync.sigmasz_contact_emails ce ON ce.contact_id = c.contact_id;
```

---

## Пересоздание view после изменений в custom_fields

Если добавляли новые кастомные поля или правили custom_fields:

```sql
SELECT analytics.rebuild_view_sigmasz_leads();
SELECT analytics.rebuild_view_sigmasz_contacts();
```
