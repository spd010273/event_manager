UPDATE event_manager.tb_setting
   SET value = 'true'
 WHERE key = 'event_manager.execute_asynchronously';

DO
 $_$
DECLARE
    my_setting BOOLEAN;
BEGIN
    PERFORM *
       FROM event_manager.tb_setting
      WHERE value = 'true'
        AND key = 'event_manager.execute_asynchronously';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: Change setting to async';
        RETURN;
    END IF;

    SELECT current_setting( 'event_manager.execute_asynchronously', TRUE )::BOOLEAN
      INTO my_setting;

    IF my_setting IS FALSE THEN
        RAISE EXCEPTION 'FAILED: Change setting to async (GUC check)';
        RETURN;
    END IF;
    RETURN;
END
 $_$
    LANGUAGE plpgsql;

UPDATE event_manager.tb_event_table_work_item SET execute_asynchronously = TRUE;
ALTER TABLE event_manager.tb_event_queue DISABLE TRIGGER tr_handle_new_event_queue_item;
INSERT INTO eventmanagertest.tb_a
            (
                foo,
                bar
            )
     VALUES
            (
                'asynchronous_test_1',
                'asynchronous_test_1'
            );

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue
      WHERE execute_asynchronously IS TRUE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: async_exec flag did not get carried on to queue item';
        RETURN;
    END IF;

    RETURN;
END
 $_$
    LANGUAGE plpgsql;

ALTER TABLE event_manager.tb_event_queue ENABLE TRIGGER tr_handle_new_event_queue_item;

UPDATE eventmanagertest.tb_a
   SET bar = 'asynchronous_test'
 WHERE foo = 'asynchronous_test_1';

SELECT pg_sleep( 5 );

--Ensure the item wasn't processed async
DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue
      WHERE pk_value = 1002;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: async item was processed synchronously';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: Async / sync mix test';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;

DELETE FROM event_manager.tb_event_queue;
