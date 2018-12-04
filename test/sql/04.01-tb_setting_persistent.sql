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
