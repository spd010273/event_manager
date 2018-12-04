--require: event_manager
SET test_guc.value = 'foobar';

BEGIN;
UPDATE event_manager.tb_setting
   SET value = 'test_guc.value'
 WHERE key = 'event_manager.session_gucs';

UPDATE eventmanagertest.tb_a
   SET bar = 'asynchronous_test_1'
 WHERE foo = 'asynchronous_test_1';

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue
      WHERE new IS NOT NULL
        AND old IS NOT NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: psuedorecords not populated';
    END IF;


    PERFORM *
       FROM event_manager.tb_event_queue
      WHERE session_values IS NOT NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: session data not populated';
    END IF;
    RETURN;
END
 $_$
    LANGUAGE plpgsql;
COMMIT;

CREATE OR REPLACE FUNCTION fn_ins_test()
RETURNS TRIGGER AS
 $_$
BEGIN
    IF( NEW.session_values IS NULL ) THEN
        RAISE EXCEPTION 'FAILED: session data not populated (work_queue)';
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql';

CREATE TRIGGER aaa_tr_ins_test
    AFTER INSERT ON event_manager.tb_work_queue
    FOR EACH ROW EXECUTE PROCEDURE fn_ins_test();

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
 DROP TRIGGER aaa_tr_ins_test ON event_manager.tb_work_queue;
 DROP FUNCTION fn_ins_test();
