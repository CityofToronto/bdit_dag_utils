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

--bigdata
ALTER TABLE IF EXISTS public.object_size_daily OWNER TO dbadmin;
GRANT ALL ON TABLE public.object_size_daily TO dbadmin;

REVOKE ALL ON TABLE public.object_size_daily FROM bdit_humans;
GRANT SELECT ON TABLE public.object_size_daily TO bdit_humans;

--ptc
ALTER TABLE IF EXISTS public.object_size_daily OWNER TO postgres;
GRANT ALL ON TABLE public.object_size_daily TO postgres;

REVOKE ALL ON TABLE public.object_size_daily FROM ptc_humans;
GRANT SELECT ON TABLE public.object_size_daily TO ptc_humans;

--both
REVOKE ALL ON TABLE public.object_size_daily FROM ref_bot;
GRANT INSERT, SELECT, UPDATE ON TABLE public.object_size_daily TO ref_bot;
