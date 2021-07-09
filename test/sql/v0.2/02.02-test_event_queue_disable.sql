--require: event_manager

UPDATE event_manager.tb_setting
   SET value = 't'
 WHERE key IN(
           'event_manager.execute_asynchronously',
           'event_manager.disable_event_queue'
       );

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_setting
      WHERE key = 'event_manager.disable_event_queue';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: GUC for event queue disable is missing';
    END IF;

    RAISE NOTICE 'PASSED: GUC for event queue disable exists';
END
 $_$
    LANGUAGE plpgsql;

INSERT INTO eventmanagertest.tb_a
            (
                foo,
                bar
            )
     VALUES
            (
                'event_queue_disable_check',
                'event_queue_disable_check'
            );

DO
 $_$
BEGIN
    PERFORM pg_sleep( 5 );

    PERFORM *
       FROM event_manager.tb_event_queue;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: Event processed despite queue being disabled';
    END IF;
END
 $_$
    LANGUAGE plpgsql;

-- This will test the SIGHUP functionality
UPDATE event_manager.tb_setting
   SET value = 'f'
 WHERE key = 'event_manager.disable_event_queue';

DO
 $_$
BEGIN
    PERFORM pg_sleep( 5 );

    PERFORM *
       FROM event_manager.tb_event_queue;

    IF FOUND THEN
        RAISE EXCEPTION 'FAILED: Event present after enabling queue';
    END IF;

    RAISE NOTICE 'PASSED: Event queue disable, re-enable with SIGHUP';
END
 $_$
    LANGUAGE plpgsql;
