-- Table: public.session_log

-- DROP TABLE IF EXISTS public.session_log;

CREATE TABLE IF NOT EXISTS public.session_log
(
    pid integer NOT NULL,
    usename name COLLATE pg_catalog."C" NOT NULL,
    application_name text COLLATE pg_catalog."default",
    backend_start timestamp without time zone NOT NULL,
    state text COLLATE pg_catalog."default",
    wait_event text COLLATE pg_catalog."default",
    blocking_pids text[] COLLATE pg_catalog."default",
    query text COLLATE pg_catalog."default",
    state_change timestamp without time zone,
    query_start timestamp without time zone,
    xact_start timestamp without time zone,
    backend_type text COLLATE pg_catalog."default",
    active_since numeric,
    ds date,
    CONSTRAINT session_log_pkey PRIMARY KEY (pid, usename, backend_start)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.session_log OWNER TO dbadmin;

REVOKE ALL ON TABLE public.session_log FROM bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE public.session_log TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE public.session_log TO dbadmin;

GRANT ALL ON TABLE public.session_log TO rds_superuser WITH GRANT OPTION;

GRANT INSERT, SELECT, UPDATE ON TABLE public.session_log TO ref_bot;
