DROP EXTENSION IF EXISTS event_manager CASCADE;
DROP TABLE IF EXISTS eventmanagertest.tb_b CASCADE;
DROP TABLE IF EXISTS eventmanagertest.tb_a CASCADE;
DROP SEQUENCE IF EXISTS eventmanagertest.sq_a;
DROP SEQUENCE IF EXISTS eventmanagertest.sq_b;
DROP SCHEMA IF EXISTS eventmanagertest;

CREATE EXTENSION IF NOT EXISTS event_manager;

DO
 $_$
DECLARE
    my_table_count  INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO my_table_count
      FROM pg_class c
INNER JOIN pg_namespace n
        ON n.oid = c.relnamespace
INNER JOIN pg_extension e
        ON e.extnamespace = n.oid
       AND e.extname::VARCHAR = 'event_manager'
     WHERE c.relname::VARCHAR IN(
               'tb_action',
               'tb_event_table',
               'tb_event_table_work_item',
               'tb_event_table_work_item_instance',
               'tb_setting',
               'tb_work_queue',
               'tb_event_queue'
           )
       AND c.relkind = 'r';
    
    IF my_table_count != 7 THEN
        RAISE EXCEPTION 'FAILED: Extension failed to install';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: Extension build';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;
