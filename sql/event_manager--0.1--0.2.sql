/*-----------------------------------------------------------------------------
 *
 * event_manager--0.1--0.2.sql
 *     Event Manager extension schema
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        event_manager--0.1--0.2.sql
 *
 *-----------------------------------------------------------------------------
 */

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_action', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table_work_item', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_setting', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_action', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table_work_item', '' );

CREATE OR REPLACE FUNCTION @extschema@.fn_new_event_trigger()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_pk_column    VARCHAR;
BEGIN
    -- Special case logic for restoring backups
   PERFORM t.oid
      FROM pg_trigger t
INNER JOIN pg_class c
        ON c.oid = t.tgrelid
       AND c.relname::VARCHAR = NEW.table_name
INNER JOIN pg_namespace n
        ON n.oid = c.relnamespace
       AND n.nspname::VARCHAR = NEW.schema_name
     WHERE t.tgname = 'tr_event_enqueue';

    IF FOUND THEN
        RAISE NOTICE 'event enqueue trigger already exists';
        RETURN NEW;
    END IF;

    SELECT a.attname::VARCHAR
      INTO my_pk_column
      FROM pg_class c
INNER JOIN pg_namespace n
        ON n.oid = c.relnamespace
INNER JOIN pg_attribute a
        ON a.attrelid = c.oid
INNER JOIN pg_constraint cn
        ON cn.conrelid = c.oid
       AND cn.contype = 'p'
       AND cn.conkey[1] = a.attnum
     WHERE c.relname::VARCHAR = NEW.table_name
       AND n.nspname::VARCHAR = NEW.schema_name;

    IF( my_pk_column IS NULL ) THEN
        RAISE EXCEPTION 'Target table, %.% needs to have a surrogate integer primary key!',
            NEW.schema_name,
            NEW.table_name;
    END IF;

    IF( NEW.no_trigger IS TRUE ) THEN
        RETURN NEW;
    END IF;

    EXECUTE format(
                'CREATE TRIGGER tr_event_enqueue '
             || '    AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
             || '    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_enqueue_event( %L );',
                NEW.schema_name,
                NEW.table_name,
                my_pk_column
            );

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: created trigger on %.%', NEW.schema_name, NEW.column_name;
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

ALTER TABLE @extschema@.tb_event_table_work_item DROP CONSTRAINT tb_event_table_work_item_inverse_event_fkey;
