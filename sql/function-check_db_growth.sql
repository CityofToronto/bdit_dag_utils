-- FUNCTION: public.check_db_growth(text)

-- DROP FUNCTION IF EXISTS public.check_db_growth(text);

CREATE OR REPLACE FUNCTION public.check_db_growth(
    db_max_size text
)
RETURNS TABLE(_check boolean, summ text) 
LANGUAGE SQL
COST 100
VOLATILE PARALLEL UNSAFE
ROWS 1

AS $BODY$
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
                > pg_size_bytes(check_db_growth.db_max_size) THEN false
            ELSE True
        END AS _check,
        'The database is growing at ' || pg_size_pretty(SUM(schema_size) - ytd.sum)
        || ' per day and will exceed ' || check_db_growth.db_max_size || ' in '
        || floor(
            (pg_size_bytes(check_db_growth.db_max_size) - SUM(schema_size)) --distance to max
            / (SUM(schema_size) - ytd.sum) --daily growth
        ) || ' days at this rate.' AS summ
    FROM public.schema_size_daily AS tdy, ytd
    WHERE dt = CURRENT_DATE
    GROUP BY ytd.sum;
$BODY$;

--ptc
ALTER FUNCTION public.check_db_growth(text) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION public.check_db_growth(text) TO postgres;
GRANT EXECUTE ON FUNCTION public.check_db_growth(text) TO ptc_humans;

--bigdata
ALTER FUNCTION public.check_db_growth(text) OWNER TO dbadmin;
GRANT EXECUTE ON FUNCTION public.check_db_growth(text) TO dbadmin;
GRANT EXECUTE ON FUNCTION public.check_db_growth(text) TO bdit_humans;

--both
GRANT EXECUTE ON FUNCTION public.check_db_growth(text) TO ref_bot;
