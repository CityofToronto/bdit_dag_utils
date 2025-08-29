-- Table: public.schema_size_daily

-- DROP TABLE IF EXISTS public.schema_size_daily;

CREATE TABLE IF NOT EXISTS public.schema_size_daily
(
    dt date NOT NULL,
    schema_name name COLLATE pg_catalog."C" NOT NULL,
    schema_size numeric,
    schema_size_pretty text COLLATE pg_catalog."default",
    CONSTRAINT schema_size_daily_pkey PRIMARY KEY (dt, schema_name)
)

TABLESPACE pg_default;

--bigdata
ALTER TABLE IF EXISTS public.schema_size_daily OWNER TO dbadmin;
GRANT ALL ON TABLE public.schema_size_daily TO dbadmin;

REVOKE ALL ON TABLE public.schema_size_daily FROM bdit_humans;
GRANT SELECT ON TABLE public.schema_size_daily TO bdit_humans;

--ptc
ALTER TABLE IF EXISTS public.schema_size_daily OWNER TO postgres;
GRANT ALL ON TABLE public.schema_size_daily TO postgres;

REVOKE ALL ON TABLE public.schema_size_daily FROM ptc_humans;
GRANT SELECT ON TABLE public.schema_size_daily TO ptc_humans;

--both
REVOKE ALL ON TABLE public.schema_size_daily FROM ref_bot;
GRANT INSERT, SELECT, UPDATE ON TABLE public.schema_size_daily TO ref_bot;

-- Index: schema_size_daily_dt_idx

-- DROP INDEX IF EXISTS public.schema_size_daily_dt_idx;

CREATE INDEX IF NOT EXISTS schema_size_daily_dt_idx
ON public.schema_size_daily USING btree
(dt ASC NULLS LAST)
TABLESPACE pg_default;