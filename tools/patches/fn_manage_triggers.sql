CREATE OR REPLACE FUNCTION event_manager.fn_manage_triggers()
RETURNS VOID AS
 $_$
DECLARE
    my_statement    VARCHAR;
BEGIN
    PERFORM pid
       FROM pg_catalog.pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'pg_restore';

    IF FOUND THEN
        RAISE DEBUG 'Database restore in progress, trigger creation disabled.';
        RETURN;
    END IF;

    CREATE TEMP TABLE tt_desired_triggers AS
    (
        WITH tt_op_expansion AS
        (
            SELECT source_event_table,
                   unnest( COALESCE( regexp_split_to_array( source_column_name, ',' ), '{null}'::VARCHAR[] ) ) AS source_column_name,
                   unnest( COALESCE( op, '{null}'::VARCHAR[] ) ) AS op
              FROM event_manager.tb_event_table_work_item
        ),
        tt_aggregate AS
        (
            SELECT n.nspname::VARCHAR AS schema_name,
                   c.relname::VARCHAR AS table_name,
                   array_agg( DISTINCT a.attname::VARCHAR ) AS column_name,
                   a_pk.attname::VARCHAR AS primary_key,
                   array_agg( DISTINCT tt.op ) AS op
              FROM event_manager.tb_event_table et
        INNER JOIN tt_op_expansion tt
                ON tt.source_event_table = et.event_table
        INNER JOIN pg_catalog.pg_class c
                ON c.relname::VARCHAR = et.table_name
        INNER JOIN pg_catalog.pg_namespace n
                ON n.oid = c.relnamespace
               AND n.nspname::VARCHAR = et.schema_name
         LEFT JOIN pg_catalog.pg_attribute a
                ON a.attrelid = c.oid
               AND a.attnum > 0
               AND a.attname::VARCHAR = tt.source_column_name
        INNER JOIN pg_catalog.pg_attribute a_pk
                ON a_pk.attrelid = c.oid
               AND a_pk.attnum > 0
        INNER JOIN pg_catalog.pg_constraint cn
                ON cn.conrelid = c.oid
               AND cn.contype = 'p'
               AND cn.conkey[1] = a_pk.attnum
          GROUP BY n.nspname,
                   c.relname,
                   a_pk.attname
        )
            SELECT schema_name,
                   table_name,
                   CASE WHEN array_position( column_name, NULL ) IS NOT NULL
                        THEN NULL
                        ELSE column_name
                         END AS column_name,
                   primary_key,
                   CASE WHEN 'I' = ANY( op ) THEN TRUE
                        ELSE FALSE
                         END AS i,
                   CASE WHEN 'U' = ANY( op ) THEN TRUE
                        ELSE FALSE
                         END AS u,
                   CASE WHEN 'D' = ANY( op ) THEN TRUE
                        ELSE FALSE
                         END AS d
              FROM tt_aggregate
    );

    CREATE TEMP TABLE tt_existing_triggers AS
    (
        WITH tt_triggers AS
        (
            SELECT nc.nspname::VARCHAR AS schema_name,
                   c.relname::VARCHAR AS table_name,
                   array_agg( DISTINCT a.attname::VARCHAR ) AS column_name,
                   a_pk.attname::VARCHAR AS primary_key,
                   CASE WHEN ( ( t.tgtype::int4::bit(16) ) << 13 )::bit = 1::BIT THEN TRUE
                        ELSE FALSE
                         END AS i,
                   CASE WHEN ( ( t.tgtype::int4::bit(16) ) << 12 )::bit = 1::BIT THEN TRUE
                        ELSE FALSE
                         END AS u,
                   CASE WHEN ( ( t.tgtype::int4::bit(16) ) << 11 )::bit = 1::bit THEN TRUE
                        ELSE FALSE
                         END AS d
              FROM pg_catalog.pg_trigger t
        INNER JOIN pg_catalog.pg_proc p
                ON p.oid = t.tgfoid
               AND p.proname::VARCHAR = 'fn_enqueue_event'
        INNER JOIN pg_catalog.pg_namespace n
                ON n.oid = p.pronamespace
               AND n.nspname::VARCHAR = 'event_manager'
        INNER JOIN pg_catalog.pg_class c
                ON c.oid = t.tgrelid
         LEFT JOIN pg_catalog.pg_attribute a
                ON a.attrelid = c.oid
               AND a.attnum > 0
               AND a.attnum = ANY( t.tgattr )
        INNER JOIN pg_catalog.pg_attribute a_pk
                ON a_pk.attnum > 0
               AND a_pk.attrelid = c.oid
        INNER JOIN pg_catalog.pg_constraint cn
                ON cn.conrelid = c.oid
               AND cn.contype = 'p'
               AND cn.conkey[1] = a_pk.attnum
        INNER JOIN pg_catalog.pg_namespace nc
                ON nc.oid = c.relnamespace
             WHERE t.tgname::VARCHAR = 'tr_event_enqueue'
          GROUP BY nc.nspname,
                   c.relname,
                   a_pk.attname,
                   t.tgtype
        )
            SELECT schema_name,
                   table_name,
                   CASE WHEN array_position( column_name, NULL ) IS NOT NULL
                        THEN NULL
                        ELSE column_name
                         END AS column_name,
                   primary_key,
                   i,
                   u,
                   d
              FROM tt_triggers
    );

    FOR my_statement IN(
                SELECT 'DROP TRIGGER tr_event_enqueue ON ' || COALESCE( tte.schema_name || '.', '' ) || tte.table_name
                  FROM tt_existing_triggers tte
             LEFT JOIN tt_desired_triggers ttd
                    ON ttd.table_name = tte.table_name
                   AND ttd.schema_name = tte.schema_name
                 WHERE ttd.primary_key IS NULL
                    OR (
                            ttd.primary_key IS DISTINCT FROM tte.primary_key
                         OR ttd.column_name IS DISTINCT FROM tte.column_name
                         OR ttd.i IS DISTINCT FROM tte.i
                         OR ttd.u IS DISTINCT FROM tte.u
                         OR ttd.d IS DISTINCT FROM tte.d
                       )
                       ) LOOP
        EXECUTE my_statement;
    END LOOP;

    DELETE FROM tt_existing_triggers tte
          USING tt_desired_triggers ttd
          WHERE tte.schema_name = ttd.schema_name
            AND tte.table_name = ttd.table_name
            AND (
                    tte.primary_key IS DISTINCT FROM ttd.primary_key
                 OR tte.column_name IS DISTINCT FROM ttd.column_name
                 OR tte.i IS DISTINCT FROM ttd.i
                 OR tte.u IS DISTINCT FROM ttd.u
                 OR tte.d IS DISTINCT FROM ttd.d
                );

    FOR my_statement IN(
                SELECT FORMAT(
                           'CREATE TRIGGER tr_event_enqueue AFTER '
                        || array_to_string(
                               ARRAY[
                                   CASE WHEN ttd.i IS TRUE THEN 'INSERT' ELSE NULL END,
                                   CASE WHEN ttd.u IS TRUE THEN
                                                           CASE WHEN array_length( ttd.column_name, 1 ) >= 1
                                                                THEN 'UPDATE OF ' || array_to_string( ttd.column_name, ', ' )
                                                                ELSE 'UPDATE'
                                                                 END
                                                           ELSE NULL
                                                            END,
                                   CASE WHEN ttd.d IS TRUE THEN 'DELETE' ELSE NULL END
                               ]::VARCHAR[],
                               ' OR '
                           )
                        || ' ON ' || COALESCE( ttd.schema_name || '.', '' ) || ttd.table_name
                        || ' FOR EACH ROW EXECUTE PROCEDURE event_manager.fn_enqueue_event( %L )',
                           ttd.primary_key
                        ) AS tgdef
                   FROM tt_desired_triggers ttd
              LEFT JOIN tt_existing_triggers tte
                     ON tte.schema_name = ttd.schema_name
                    AND tte.table_name = ttd.table_name
                  WHERE tte.primary_key IS NULL
                       ) LOOP
        EXECUTE my_statement;
    END LOOP;

    DROP TABLE tt_desired_triggers;
    DROP TABLE tt_existing_triggers;

    RETURN;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;
