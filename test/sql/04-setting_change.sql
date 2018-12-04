
UPDATE event_manager.tb_setting
   SET value = 'false'
 WHERE key = 'event_manager.execute_asynchronously';

DO
 $_$
BEGIN
    PERFORM *
       FROM event_manager.tb_setting
      WHERE key = 'event_manager.execute_asynchronously'
        AND value = 'false';
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: change tb_setting';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: change tb_setting';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;

DO
 $_$
DECLARE
    my_value    BOOLEAN;
BEGIN
    SELECT current_setting( 'event_manager.execute_asynchronously', TRUE )::BOOLEAN
      INTO my_value;

    IF( my_value IS DISTINCT FROM FALSE ) THEN
        RAISE EXCEPTION 'FAILED: tb_setting GUC';
        RETURN;
    END IF;
    
    RAISE NOTICE 'PASSED: tb_setting GUC';
    RETURN;
END
 $_$
    LANGUAGE plpgsql; 
