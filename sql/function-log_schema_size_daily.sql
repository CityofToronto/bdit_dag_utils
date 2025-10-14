-- FUNCTION: public.log_schema_size_daily()

-- DROP FUNCTION IF EXISTS public.log_schema_size_daily();

CREATE OR REPLACE FUNCTION public.log_schema_size_daily(
)
RETURNS void
LANGUAGE sql
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$

INSERT INTO public.schema_size_daily (
    dt, schema_name, schema_size, schema_size_pretty
)
SELECT
    CURRENT_DATE AS dt,
    pg_namespace.nspname AS schema_name,
    SUM(pg_relation_size(pg_class.oid)) AS schema_size,
    pg_size_pretty(SUM(pg_relation_size(pg_class.oid))) AS schema_size_pretty
FROM pg_catalog.pg_class
JOIN pg_catalog.pg_namespace ON relnamespace = pg_namespace.oid
GROUP BY
    CURRENT_DATE,
    pg_namespace.nspname
ORDER BY schema_size DESC
ON CONFLICT ON CONSTRAINT schema_size_daily_pkey
DO UPDATE
SET schema_size = EXCLUDED.schema_size,
    schema_size_pretty = EXCLUDED.schema_size_pretty;
$BODY$;

ALTER FUNCTION public.log_schema_size_daily()
OWNER TO dbadmin;

GRANT EXECUTE ON FUNCTION public.log_schema_size_daily() TO public;

GRANT EXECUTE ON FUNCTION public.log_schema_size_daily() TO dbadmin;

GRANT EXECUTE ON FUNCTION public.log_schema_size_daily() TO ref_bot;
