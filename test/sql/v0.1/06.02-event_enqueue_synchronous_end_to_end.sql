UPDATE eventmanagertest.tb_a
   SET bar = 'synchronous_test'
 WHERE foo = 'synchronous_test';

SELECT pg_sleep( 5 );

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: event queue processing (item still in queue)';
        RETURN;
    END IF;

    PERFORM *
       FROM event_manager.tb_work_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: work queue processing (item still in queue)';
        RETURN;
    END IF;

    PERFORM *
       FROM eventmanagertest.tb_b b
      WHERE b.a = 1;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: action did not take effect';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: queue processing synchronous';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;
