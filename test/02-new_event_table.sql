INSERT INTO event_manager.tb_event_table
            (
                table_name,
                schema_name
            )
     VALUES
            (
                'tb_a',
                'eventmanagertest'
            );

DO
 $_$
BEGIN
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
        RAISE EXCEPTION 'FAILED: new event table';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: new event table';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;
