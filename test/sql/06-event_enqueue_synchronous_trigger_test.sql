ALTER TABLE event_manager.tb_event_queue DISABLE TRIGGER tr_handle_new_event_queue_item;

INSERT INTO eventmanagertest.tb_a
            (
                foo,
                bar
            )
     VALUES
            (
                'synchronous_test',
                'synchronous_test'
            );


DO
 $_$
BEGIN
    PERFORM *
       FROM eventmanagertest.tb_a
      WHERE foo = 'synchronous_test'
        AND bar = 'synchronous_test';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: new event (data missing from event_source)';
        RETURN;
    END IF;

    PERFORM *
       FROM event_manager.tb_event_queue eq
 INNER JOIN event_manager.tb_event_table_work_item etwi
         ON etwi.event_table_work_item = eq.event_table_work_item
 INNER JOIN event_manager.tb_event_table et
         ON etwi.source_event_table = et.event_table
        AND et.table_name = 'tb_a'
        AND et.schema_name = 'eventmanagertest';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: new event (data missing from event queue)';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: new event';
END
 $_$
    LANGUAGE plpgsql;

DELETE FROM event_manager.tb_event_queue;
ALTER TABLE event_manager.tb_event_queue ENABLE TRIGGER tr_handle_new_event_queue_item;
