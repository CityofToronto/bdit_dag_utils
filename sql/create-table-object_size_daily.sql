-- Table: public.object_size_daily

-- DROP TABLE IF EXISTS public.object_size_daily;

CREATE TABLE IF NOT EXISTS public.object_size_daily
(
    schema_name name COLLATE pg_catalog."C" NOT NULL,
    relname name COLLATE pg_catalog."C" NOT NULL,
    table_sizes bigint [],
    dts date [],
    CONSTRAINT object_size_daily_pkey PRIMARY KEY (schema_name, relname)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.object_size_daily
OWNER TO dbadmin;

REVOKE ALL ON TABLE public.object_size_daily FROM bdit_humans;
REVOKE ALL ON TABLE public.object_size_daily FROM ref_bot;

GRANT SELECT ON TABLE public.object_size_daily TO bdit_humans;

GRANT ALL ON TABLE public.object_size_daily TO dbadmin;

GRANT INSERT, SELECT, UPDATE ON TABLE public.object_size_daily TO ref_bot;
