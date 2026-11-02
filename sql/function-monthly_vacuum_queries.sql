CREATE OR REPLACE FUNCTION dbadmin.monthly_vacuum_queries(mnth date)
RETURNS TABLE (
    vacuum_query text
) AS $$

WITH disk_space AS (
    SELECT 'VACUUM FULL ONLY ' || n.nspname || '.' || c.relname || ';' AS vacuum_query, c.oid AS disk_space
    FROM pg_namespace AS n
    JOIN pg_catalog.pg_class AS c
        ON c.relnamespace = n.oid
        AND c.relkind IN ('r', 'm', 'p') --tables, mat views, partitioned table
    WHERE c.relname LIKE '%' || to_char(mnth, 'yyyymm') || '%'
)

SELECT vacuum_query
FROM disk_space

UNION

SELECT 'SELECT dbadmin.space_saved(''' || mnth || ''', ' || SUM(pg_total_relation_size(disk_space)) || ');'
FROM disk_space
ORDER BY 1 DESC; --correctly puts select after vacuum

$$ LANGUAGE sql;

--Example
--SELECT dbadmin.monthly_vacuum_queries('2026-01-01')

ALTER FUNCTION dbadmin.monthly_vacuum_queries OWNER TO dbadmin;
GRANT EXECUTE ON FUNCTION dbadmin.monthly_vacuum_queries TO ref_bot;