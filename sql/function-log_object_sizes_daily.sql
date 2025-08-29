-- FUNCTION: public.log_object_sizes_daily()

-- DROP FUNCTION IF EXISTS public.log_object_sizes_daily();

CREATE OR REPLACE FUNCTION public.log_object_sizes_daily(
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

INSERT INTO public.object_size_daily (schema_name, relname, table_sizes, dts)
SELECT
    pg_namespace.nspname AS schema_name,
    pg_class.relname,
    ARRAY[pg_relation_size(pg_class.oid)]::bigint[],
    ARRAY[CURRENT_DATE]::date[]    
FROM pg_catalog.pg_class
JOIN pg_catalog.pg_namespace ON pg_class.relnamespace = pg_namespace.oid
FULL JOIN public.object_size_daily AS existing
    ON existing.schema_name = pg_namespace.nspname
    AND existing.relname = pg_class.relname
WHERE
    pg_namespace.nspname NOT LIKE 'pg_%'
    AND pg_relation_size(pg_class.oid) > 0
ON CONFLICT ON CONSTRAINT object_size_daily_pkey
--append to existing arrays
DO UPDATE SET
    table_sizes = EXCLUDED.table_sizes || object_size_daily.table_sizes,
    dts = EXCLUDED.dts || object_size_daily.dts;

--trim table_sizes ARRAYS
UPDATE public.object_size_daily
SET table_sizes = trim_array(table_sizes, array_length(table_sizes, 1)-10)
WHERE array_length(table_sizes, 1) > 10;

--trim dts ARRAYS
UPDATE public.object_size_daily
SET dts = trim_array(dts, array_length(dts, 1)-10)
WHERE array_length(dts, 1) > 10;

$BODY$;

--ptc
ALTER FUNCTION public.log_object_sizes_daily() OWNER TO postgres;
GRANT EXECUTE ON FUNCTION public.log_object_sizes_daily() TO postgres;
REVOKE ALL ON FUNCTION public.log_object_sizes_daily() FROM ptc_humans;

--bigdata
ALTER FUNCTION public.log_object_sizes_daily() OWNER TO dbadmin;
GRANT EXECUTE ON FUNCTION public.log_object_sizes_daily() TO dbadmin;
REVOKE ALL ON FUNCTION public.log_object_sizes_daily() FROM bdit_humans;

--both
GRANT EXECUTE ON FUNCTION public.log_object_sizes_daily() TO ref_bot;
