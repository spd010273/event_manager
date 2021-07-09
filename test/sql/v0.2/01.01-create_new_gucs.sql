INSERT INTO event_manager.tb_setting
            (
                key,
                value
            )
     VALUES ( 'event_manager.disable_event_queue', 'f' ),
            ( 'event_manager.disable_work_queue', 'f' );

DO
 $_$
BEGIN
    RAISE NOTICE 'PASSED: Create v0.2 GUCs';
END
 $_$
    LANGUAGE plpgsql;
