--require: event_manager

UPDATE eventmanagertest.tb_a
   SET bar = 'asynchronous_test_1'
 WHERE foo = 'asynchronous_test_1';

SELECT pg_sleep(10);

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: event queue not empty';
        RETURN;
    END IF;

    PERFORM *
       FROM event_manager.tb_work_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: work queue not empty';
        RETURN;
    END IF;

    PERFORM *
       FROM eventmanagertest.tb_b b
      WHERE b.a = 2;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: async action did not take effect';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: queue processing async';
    RETURN;
END
 $_$
    LANGUAGE 'plpgsql';
