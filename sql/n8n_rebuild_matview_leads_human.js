// =====================================================
// n8n Code Node: Построение MATERIALIZED VIEW для аналитики
// Генерирует SQL для analytics.{subdomain}_leads_human
// напрямую из L2 таблиц (prod_sync)
// =====================================================
// Воспроизводит 1:1 схему текущего VIEW из
// analytics.rebuild_view_leads_human(), но читает из L2
// =====================================================

// =====================================================
// 1. ВХОДНЫЕ ДАННЫЕ
// =====================================================

// custom fields
const response = item.json;
const fields = response._embedded ? response._embedded.custom_fields : [];

// subdomain
const subdomain = $('Get Tokens').item.json.subdomain;

// =====================================================
// 2. ИМЕНА ТАБЛИЦ
// =====================================================

// View живёт в analytics
const viewSchema = 'analytics';
const viewName = `${viewSchema}.${subdomain}_leads_human`;
const cleanViewName = `${subdomain}_leads_human`;

// Источники данных — L2 таблицы в prod_sync
const leadsTable = `prod_sync.${subdomain}_leads`;
const contactsTable = `prod_sync.${subdomain}_contacts`;
const linkTable = `prod_sync.${subdomain}_lead_contacts`;
const phonesTable = `prod_sync.${subdomain}_contact_phones`;
const emailsTable = `prod_sync.${subdomain}_contact_emails`;

// =====================================================
// 3. ЗАРЕЗЕРВИРОВАННЫЕ ИМЕНА (как в rebuild_view_leads_human)
// =====================================================

const reservedNames = new Set([
    "ID", "Название", "Статус", "Воронка", "Бюджет",
    "Дата создания", "Дата обновления", "Удален",
    "ID контакта", "Имя контакта", "Телефон", "Email"
]);

// =====================================================
// 4. CUSTOM FIELDS → PIVOT SQL
// =====================================================

let customFieldsSql = [];
const customFieldNames = [];  // для явного SELECT (без cfp.*)
const usedNames = new Set();

fields.forEach(field => {
    const id = field.id;
    let rawName = field.name.replace(/"/g, '').trim();
    // Нормализуем пробелы как в rebuild_view_leads_human
    let name = rawName.replace(/\s+/g, ' ').trim();

    // Обрезаем до 63 байт (лимит идентификаторов PostgreSQL)
    while (Buffer.byteLength(name, 'utf8') > 63) {
        name = name.substring(0, name.length - 1).trim();
    }

    // При дубликатах или коллизиях со стандартными полями — добавляем _id
    if (reservedNames.has(name) || usedNames.has(name)) {
        const suffix = `_${id}`;
        const maxBytes = 63 - Buffer.byteLength(suffix, 'utf8');
        while (Buffer.byteLength(name, 'utf8') > maxBytes) {
            name = name.substring(0, name.length - 1).trim();
        }
        name = `${name}${suffix}`;
    }
    usedNames.add(name);

    const type = field.type;
    const valExpr = `max(CASE WHEN field_id = '${id}' THEN field_value ELSE NULL::text END)`;

    let isDate = ['date', 'date_time', 'birthday'].includes(type);

    let sqlLine;

    if (isDate) {
        sqlLine = `(CASE
            WHEN ${valExpr} ~ '^\\d+$' THEN to_timestamp(${valExpr}::bigint)
            WHEN ${valExpr} ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN to_timestamp(${valExpr}, 'DD.MM.YYYY')
            WHEN ${valExpr} ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN ${valExpr}::timestamp
            ELSE NULL
        END) AS "${name}"`;
    } else if (type === 'checkbox') {
        sqlLine = `(${valExpr})::boolean AS "${name}"`;
    } else if (type === 'numeric') {
        sqlLine = `(CASE WHEN ${valExpr} ~ '^[0-9]+(\\.[0-9]+)?$'
                          THEN ${valExpr}::numeric ELSE NULL END) AS "${name}"`;
    } else {
        sqlLine = `${valExpr} AS "${name}"`;
    }

    customFieldsSql.push('    ' + sqlLine.replace(/\n/g, ' '));
    customFieldNames.push(name);
});

const customFieldsBlock = customFieldsSql.join(',\n');

// Явный список custom field колонок (без lead_id из CTE)
const cfpSelectCols = customFieldNames.length
    ? customFieldNames.map(n => `cfp."${n}"`).join(',\n    ')
    : '';

// =====================================================
// 5. СТАНДАРТНЫЕ ПОЛЯ (1:1 с rebuild_view_leads_human)
// =====================================================

const standardColumns = `
    l.lead_id AS "ID",
    l.name AS "Название",
    l.status_id AS "Статус",
    l.pipeline_id AS "Воронка",
    l.price AS "Бюджет",
    l.created_at AS "Дата создания",
    l.updated_at AS "Дата обновления",
    l.is_deleted AS "Удален",
    ci.contact_id AS "ID контакта",
    ci.contact_name AS "Имя контакта",
    ci.phone AS "Телефон",
    ci.email AS "Email"
`;

// =====================================================
// 6. ФИНАЛЬНЫЙ SQL (SAFE SWAP PATTERN)
// =====================================================

// Разделитель между стандартными полями и кастомными
const cfpBlock = cfpSelectCols ? `,\n    ${cfpSelectCols}` : '';

const fullQuery = `
-- 1. Подготовка: Удаляем временную view _new, если она осталась от сбоя
DROP MATERIALIZED VIEW IF EXISTS ${viewName}_new CASCADE;

-- 2. Создаем НОВУЮ materialized view (_new)
CREATE MATERIALIZED VIEW ${viewName}_new AS
WITH custom_fields_pivoted AS (
    SELECT
        lead_id,
${customFieldsBlock}
    FROM (
        SELECT
            l1.lead_id,
            cf.value ->> 'field_id' AS field_id,
            ((cf.value -> 'values') -> 0 ->> 'value') AS field_value
        FROM ${leadsTable} l1
        CROSS JOIN LATERAL jsonb_array_elements(
            l1.raw_json -> 'custom_fields_values'
        ) cf(value)
        WHERE jsonb_typeof(l1.raw_json -> 'custom_fields_values') = 'array'
    ) t
    GROUP BY lead_id
),
contact_info AS (
    SELECT DISTINCT ON (lc.lead_id)
        lc.lead_id,
        c.contact_id,
        c.name AS contact_name,
        (SELECT cp.phone
         FROM ${phonesTable} cp
         WHERE cp.contact_id = c.contact_id
         ORDER BY cp.phone LIMIT 1) AS phone,
        (SELECT ce.email
         FROM ${emailsTable} ce
         WHERE ce.contact_id = c.contact_id
         ORDER BY ce.email LIMIT 1) AS email
    FROM ${linkTable} lc
    JOIN ${contactsTable} c
        ON  c.contact_id = lc.contact_id
        AND COALESCE(c.is_deleted, FALSE) IS FALSE
    ORDER BY lc.lead_id, COALESCE(lc.is_main, FALSE) DESC, lc.contact_id ASC
)
SELECT
${standardColumns}${cfpBlock}
FROM ${leadsTable} l
LEFT JOIN custom_fields_pivoted cfp ON l.lead_id = cfp.lead_id
LEFT JOIN contact_info ci ON l.lead_id = ci.lead_id;

-- 3. Создаем индекс на НОВОЙ view
CREATE UNIQUE INDEX ${cleanViewName}_unique_idx_new
ON ${viewName}_new ("ID");

-- 4. Атомарная подмена (Swap)
BEGIN;
    DROP MATERIALIZED VIEW IF EXISTS ${viewName}_old CASCADE;

    -- Удаляем старый объект: может быть обычный VIEW или MATERIALIZED VIEW
    DROP VIEW IF EXISTS ${viewName} CASCADE;
    DROP MATERIALIZED VIEW IF EXISTS ${viewName} CASCADE;

    ALTER MATERIALIZED VIEW ${viewName}_new RENAME TO ${cleanViewName};
    ALTER INDEX ${viewSchema}.${cleanViewName}_unique_idx_new RENAME TO ${cleanViewName}_unique_idx;
COMMIT;
`;

return {
    json: {
        query: fullQuery,
        subdomain
    }
};
