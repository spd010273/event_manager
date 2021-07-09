--require: event_manager

UPDATE event_manager.tb_setting
   SET value = 't'
 WHERE key IN(
           'event_manager.execute_asynchronously',
           'event_manager.disable_work_queue'
       );
UPDATE event_manager.tb_setting SET value = 'f' WHERE key = 'event_manager.disable_event_queue';

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_setting
      WHERE key = 'event_manager.disable_work_queue';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: GUC for work queue disable is missing';
    END IF;

    RAISE NOTICE 'PASSED: GUC for work queue exists';
END
 $_$
    LANGUAGE plpgsql;

DO
 $_$
BEGIN
    INSERT INTO event_manager.tb_work_queue
                (
                    parameters,
                    action,
                    uid,
                    recorded,
                    execute_asynchronously,
                    session_values,
                    failed
                )
         VALUES
                (
                    ('{"md5":"' || reverse( md5( 2::VARCHAR ) ) || '"}')::JSONB,
                    1,
                    0,
                    now(),
                    TRUE,
                    NULL::JSONB,
                    FALSE
                );

    PERFORM pg_sleep( 5 );

    PERFORM *
       FROM event_manager.tb_work_queue;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: work queue is empty';
    END IF;
END
 $_$
    LANGUAGE plpgsql;

UPDATE event_manager.tb_setting
   SET value = 'f'
 WHERE key = 'event_manager.disable_work_queue';

DO
 $_$
BEGIN
    PERFORM pg_sleep( 5 );

    PERFORM *
       FROM event_manager.tb_work_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: work queue item present after enabling queue';
    END IF;

    RAISE NOTICE 'PASSED: Work queue disable, re-enable with SIGHUP';
END
 $_$
    LANGUAGE plpgsql;
