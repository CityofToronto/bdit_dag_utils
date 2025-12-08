--noqa: disable=TMP
WITH records AS (
    SELECT
        '`' || schema_name || '.' || relname || '`' AS obj_name,
        dts,
        table_sizes,
        pg_size_pretty(table_sizes[1] - coalesce(table_sizes[2],0)) AS size_change
    FROM public.object_size_daily
    WHERE
        ABS(table_sizes[1] - coalesce(table_sizes[2],0)) > pg_size_bytes('{{ var.json.db_monitoring.db_table_alert }}')  -->=10GB size increase in a day
        AND dts[1] = CURRENT_DATE
)

SELECT
    COUNT(*) < 1 AS _check,
    'The following database object(s) changed in size by more than '
    || '{{ var.json.db_monitoring.db_table_alert }} today:' AS summ,
    array_agg(
        obj_name || ' '
        || CASE WHEN table_sizes[1] > coalesce(table_sizes[2],0) THEN 'in' ELSE 'de' END || 'creased in size by `'
        || size_change || '` between ' || dts[1] || ' and ' || coalesce(dts[2], dts[1] - 1) || '.'
        ORDER BY obj_name
    ) AS gaps
FROM records;