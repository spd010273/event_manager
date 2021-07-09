--require: event_manager
SET test_guc.value = 'foobar';

UPDATE event_manager.tb_action
   SET query = 'ASDF'
 WHERE uri IS NULL
   AND static_parameters IS NULL;

SELECT * FROM event_manager.tb_action;
BEGIN;
UPDATE event_manager.tb_setting
   SET value = 'test_guc.value'
 WHERE key = 'event_manager.session_gucs';

INSERT INTO eventmanagertest.tb_a
            (
                foo,
                bar
            )
     VALUES
            (
                'asynchronous_test_2',
                'asynchronous_test_2'
            );
DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue
      WHERE new IS NOT NULL
        AND old IS NULL;

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
       FROM event_manager.tb_work_queue
      WHERE failed;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: work queue has no failed event';
        RETURN;
    END IF;

    RETURN;
END
 $_$
    LANGUAGE 'plpgsql';

UPDATE event_manager.tb_action
   SET query = 'DELETE FROM eventmanagertest.tb_b b USING eventmanagertest.tb_a a WHERE a.a = b.a AND b.foo = ?md5?::VARCHAR'
 WHERE uri IS NULL
   AND static_parameters IS NULL;
ALTER TABLE event_manager.tb_event_table_work_item
    DISABLE TRIGGER tr_work_item_query_test;
UPDATE event_manager.tb_event_table_work_item
   SET work_item_query = 'SELECT ( ''{"md5":'' || reverse( md5( ( a.a )::VARCHAR ) || ''}'' )::JSONB AS paramters FROM eventmanagertest.tb_a a JOIN eventmanagertest.tb_b b ON b.a = a.a WHERE a.a = ( ?pk_value?::INTEGER - 1000 )';
ALTER TABLE event_manager.tb_event_table_work_item
    ENABLE TRIGGER tr_work_item_query_test;
UPDATE eventmanagertest.tb_a
   SET bar = 'asynchronous_test_3'
 WHERE foo = 'asynchronous_test_2';

SELECT pg_sleep( 10 );

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_queue
      WHERE failed;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: event queue item not failed';
        RETURN;
    END IF;

    RETURN;
END
 $_$
    LANGUAGE 'plpgsql';

DELETE FROM event_manager.tb_work_queue;
DELETE FROM event_manager.tb_event_queue;
UPDATE event_manager.tb_event_table_work_item
   SET work_item_query = 'SELECT (''{"md5":"'' || reverse( md5( ( a.a )::VARCHAR ) ) || ''"}'' )::JSON AS parameters FROM eventmanagertest.tb_a a JOIN eventmanagertest.tb_b b ON b.a = a.a WHERE a.a = ( ?pk_value?::INTEGER - 10000 )';

DO
 $_$
BEGIN
    RAISE NOTICE 'PASSED: event/work queue failed item test';
END
 $_$
    LANGUAGE 'plpgsql';
