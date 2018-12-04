INSERT INTO event_manager.tb_action
            (
                query,
                uri,
                static_parameters
            )
     VALUES
            (
                'DELETE FROM eventmanagertest.tb_b b USING eventmanagertest.tb_a a WHERE a.a = b.a AND b.foo = ?md5?::VARCHAR',
                NULL,
                NULL
            ),
            (
                NULL,
                'https://ises.chris.neadwerx.com/api/current/locations',
                '{"number":"121","testparam2":"val2"}'::JSONB
            );

DO
 $_$
DECLARE
    my_count    INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO my_count
      FROM event_manager.tb_action;
    
    IF( my_count != 2 ) THEN
        RAISE EXCEPTION 'FAILED: create actions';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: create action';
    RETURN;
END
 $_$
    LANGUAGE 'plpgsql';
