INSERT INTO event_manager.tb_event_table_work_item
            (
                source_event_table,
                action,
                work_item_query,
                op
            )
     SELECT et.event_table,
            a.action,
            'SELECT ( ''{"md5":"'' || reverse( md5( ( a.a )::VARCHAR ) ) || ''"}'' )::JSONB AS parameters FROM eventmanagertest.tb_a a JOIN eventmanagertest.tb_b b ON b.a = a.a WHERE a.a = ( ?pk_value?::INTEGER - 1000 )',
            ARRAY['I','U','D']::CHAR(1)[]
       FROM event_manager.tb_event_table et
 INNER JOIN event_manager.tb_action a
         ON a.query IS NOT NULL
      WHERE et.table_name = 'tb_a'
        AND et.schema_name = 'eventmanagertest';

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_table_work_item;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: create event_table_work_item';
        RETURN;
    END IF;

    PERFORM *
       FROM pg_trigger t
 INNER JOIN pg_class c
         ON c.oid = t.tgrelid
        AND c.relkind = 'r'
        AND c.relname::VARCHAR = 'tb_a'
 INNER JOIN pg_namespace n
         ON n.oid = c.relnamespace
        AND n.nspname::VARCHAR = 'eventmanagertest'
      WHERE t.tgname::VARCHAR = 'tr_event_enqueue';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: event table trigger creation';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: create event_table_work_item';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;
