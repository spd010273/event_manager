-- Previous test created a row in tb_a we're going to use to fake a new enqueue
ALTER TABLE event_manager.tb_work_queue DISABLE TRIGGER tr_handle_new_work_queue_item;
UPDATE eventmanagertest.tb_a
   SET bar = 'updatetest'
 WHERE foo = 'synchronous_test';

SELECT pg_sleep( 5 );

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_work_queue
      WHERE execute_asynchronously IS FALSE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: event_queue_processing';
        RETURN;
    END IF;

    PERFORM *
       FROM event_manager.tb_event_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: event_queue remove post processing';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: event queue processing';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;

ALTER TABLE event_manager.tb_work_queue ENABLE TRIGGER tr_handle_new_work_queue_item;
DELETE FROM event_manager.tb_work_queue;
DELETE FROM event_manager.tb_event_queue;
