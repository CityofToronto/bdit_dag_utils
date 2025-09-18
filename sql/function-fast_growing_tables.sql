-- FUNCTION: public.fast_growing_tables(text, text)

-- DROP FUNCTION IF EXISTS public.fast_growing_tables(text, text);

CREATE OR REPLACE FUNCTION public.fast_growing_tables(
    db_table_alert text,
    server text
)
RETURNS TABLE(_check boolean, summ text, gaps text[]) 
LANGUAGE SQL
COST 100
VOLATILE PARALLEL UNSAFE
ROWS 1

AS $BODY$
    --noqa: disable=TMP
    WITH records AS (
        SELECT
            '`' || schema_name || '.' || relname || '`' AS obj_name,
            dts,
            table_sizes,
            pg_size_pretty(table_sizes[1] - coalesce(table_sizes[2],0)) AS size_change
        FROM public.object_size_daily
        WHERE
            ABS(table_sizes[1] - coalesce(table_sizes[2],0)) > pg_size_bytes(fast_growing_tables.db_table_alert)  -->=10GB size increase in a day
            AND dts[1] = CURRENT_DATE
    )

    SELECT
        COUNT(*) < 1 AS _check,
        'The following :' || fast_growing_tables.server || ': database object(s) changed in size by more than '
        || fast_growing_tables.db_table_alert || ' today:' AS summ,
        array_agg(
            obj_name || ' '
            || CASE WHEN table_sizes[1] > coalesce(table_sizes[2],0) THEN 'in' ELSE 'de' END || 'creased in size by `'
            || size_change || '` between ' || dts[1] || ' and ' || coalesce(dts[2], dts[1] - 1) || '.'
            ORDER BY obj_name
        ) AS gaps
    FROM records;

$BODY$;

--ptc
ALTER FUNCTION public.fast_growing_tables(text, text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION public.fast_growing_tables(text, text) TO postgres;
GRANT EXECUTE ON FUNCTION public.fast_growing_tables(text, text) TO ptc_humans;

--bigdata
ALTER FUNCTION public.fast_growing_tables(text, text) OWNER TO dbadmin;
GRANT EXECUTE ON FUNCTION public.fast_growing_tables(text, text) TO dbadmin;
GRANT EXECUTE ON FUNCTION public.fast_growing_tables(text, text) TO bdit_humans;

--both
GRANT EXECUTE ON FUNCTION public.fast_growing_tables(text, text) TO ref_bot;
