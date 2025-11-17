--log blocked or long running queries

INSERT INTO public.session_log (
    pid, usename, application_name, backend_start, state, wait_event, blocking_pids,
    query, state_change, query_start, xact_start, backend_type, active_since
)
SELECT
    pid,
    usename,
    application_name,
    backend_start,
    state,
    wait_event,
    blocking_pids,
    query,
    state_change,
    query_start,
    xact_start,
    backend_type,
    active_since
FROM public.get_sessions(%s::text)
WHERE
    backend_type <> 'parallel worker' AND (
        active_since >= 30
        OR pid IN (
            SELECT UNNEST(blocking_pids)::integer FROM public.get_sessions(%s::text)
        )
    )
ON CONFLICT ON CONSTRAINT session_log_pkey
DO UPDATE SET (application_name, state, wait_event, blocking_pids, query, state_change, query_start, xact_start, backend_type, active_since)
    = (
        EXCLUDED.application_name,
        EXCLUDED.state,
        EXCLUDED.wait_event,
        EXCLUDED.blocking_pids,
        EXCLUDED.query,
        EXCLUDED.state_change,
        EXCLUDED.query_start,
        EXCLUDED.xact_start,
        EXCLUDED.backend_type,
        EXCLUDED.active_since
    )