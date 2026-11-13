-- DROP FUNCTION dbadmin.vacuum_space_saved;

CREATE OR REPLACE FUNCTION dbadmin.vacuum_space_saved(
    mnth date,
    previous_size numeric
)
RETURNS text AS $$

WITH disk_space AS (
    SELECT SUM(pg_total_relation_size(pg_class.oid)) AS disk_space
    FROM pg_namespace AS n
    JOIN pg_catalog.pg_class
        ON pg_class.relnamespace = n.oid
        AND pg_class.relkind IN ('r', 'm', 'p') --tables, views, mat views, foreign table, partitioned table
    WHERE pg_class.relname LIKE '%' || to_char(mnth, 'yyyymm') || '%'
)

SELECT
    'Space saved due to VACUUM: ' || pg_size_pretty(disk_space - vacuum_space_saved.previous_size)
    || ' (Of ' || pg_size_pretty(vacuum_space_saved.previous_size) || ')'
FROM disk_space;

$$ LANGUAGE sql;

--Example
--SELECT dbadmin.vacuum_space_saved('2026-01-01', 127779559424)

ALTER FUNCTION dbadmin.vacuum_space_saved OWNER TO dbadmin;
GRANT EXECUTE ON FUNCTION dbadmin.vacuum_space_saved TO ref_bot;
