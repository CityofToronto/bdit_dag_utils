--noqa: disable=TMP
WITH ytd AS (
    SELECT SUM(schema_size) 
    FROM public.schema_size_daily
    WHERE dt = CURRENT_DATE - 1
)

SELECT
    CASE
        --notify if less than 100 days till limit
        WHEN (SUM(schema_size) - ytd.sum) * 100 + SUM(schema_size)
            > pg_size_bytes('{{ var.json.db_monitoring.db_max_size }}') THEN false
        ELSE True
    END AS _check,
    'The database is growing at ' || pg_size_pretty(SUM(schema_size) - ytd.sum)
    || ' per day and will exceed {{ var.json.db_monitoring.db_max_size }} in '
    || floor(
        (pg_size_bytes('{{ var.json.db_monitoring.db_max_size }}') - SUM(schema_size)) --distance to max
        / (SUM(schema_size) - ytd.sum) --daily growth
    ) || ' days at this rate.' AS summ
FROM public.schema_size_daily AS tdy, ytd
WHERE dt = CURRENT_DATE
GROUP BY ytd.sum;