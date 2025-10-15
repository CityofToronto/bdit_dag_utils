-- FUNCTION: public.get_sessions(text)

-- DROP FUNCTION IF EXISTS public.get_sessions(text);

CREATE OR REPLACE FUNCTION public.get_sessions(datname text)
RETURNS TABLE(pid integer, usename name, application_name text, backend_start text, state text, wait_event text, blocking_pids text[], query text, state_change text, query_start text, xact_start text, backend_type text, active_since numeric) 
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
ROWS 1000

AS $BODY$

    SELECT
        pid,
        usename,
        application_name,
        pg_catalog.to_char(backend_start, 'YYYY-MM-DD HH24:MI:SS TZ') AS backend_start,
        state,
        wait_event_type || ': ' || wait_event AS wait_event,
        pg_catalog.pg_blocking_pids(pid) AS blocking_pids,
        query,
        pg_catalog.to_char(state_change, 'YYYY-MM-DD HH24:MI:SS TZ') AS state_change,
        pg_catalog.to_char(query_start, 'YYYY-MM-DD HH24:MI:SS TZ') AS query_start,
        pg_catalog.to_char(xact_start, 'YYYY-MM-DD HH24:MI:SS TZ') AS xact_start,
        backend_type,
        CASE WHEN state = 'active' THEN ROUND((extract(epoch from now() - query_start) / 60)::numeric, 2) ELSE 0 END AS active_since
    FROM
        pg_catalog.pg_stat_activity
    WHERE datname = get_sessions.datname
    ORDER BY pid;

$BODY$;

ALTER FUNCTION public.get_sessions(text) OWNER TO dbadmin;

GRANT EXECUTE ON FUNCTION public.get_sessions(text) TO dbadmin;

GRANT EXECUTE ON FUNCTION public.get_sessions(text) TO ref_bot;

REVOKE ALL ON FUNCTION public.get_sessions(text) FROM PUBLIC;
