INSERT INTO event_manager.tb_event_table
            (
                table_name,
                schema_name
            )
     VALUES
            (
                'tb_a',
                'eventmanagertest'
            );

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_event_table
      WHERE table_name = 'tb_a'
        AND schema_name = 'eventmanagertest';

    IF FOUND THEN
        RIASE NOTICE 'PASSED: new event table';
        RETURN;
    END IF;

    RAISE EXCEPTION 'FAILED: new event table';
END
 $_$
    LANGUAGE plpgsql;
