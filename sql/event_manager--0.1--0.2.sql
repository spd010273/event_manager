CREATE FUNCTION @extschema@.fn_validate_event_table_work_item()
RETURNS TRIGGER AS
 $_$
BEGIN
    PERFORM pid
       FROM pg_catalog.pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'pg_restore';

    IF FOUND THEN
        RETURN NEW;
    END IF;

    IF( TG_OP = 'UPDATE' AND NEW::TEXT IS NOT DISTINCT FROM OLD::TEXT ) THEN
        RETURN NEW;
    END IF;

    PERFORM *
       FROM @extschema@.tb_event_table_work_item etwi
      WHERE etwi.event_table_work_item = NEW.event_table_work_item;

    IF NOT FOUND THEN
        RAISE NOTICE 'Rejecting % to @extschema@.% due to FK violation (% does not exist in tb_event_table_work_item)',
            TG_OP,
            TG_TABLE_NAME,
            NEW.event_table_work_item;
        RETURN NULL;
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE plpgsql;

CREATE TRIGGER tr_event_table_work_item_instance_event_table_work_item_check
    AFTER INSERT OR UPDATE OF event_table_work_item ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_validate_event_table_work_item();

CREATE OR REPLACE FUNCTION @extschema@.fn_set_configuration()
RETURNS TRIGGER AS
 $_$
BEGIN
    IF( NEW.key NOT ILIKE '@extschema@.%' ) THEN
        RAISE EXCEPTION '% is not an extension GUC and cannot be modified using this table', NEW.key;
    END IF;

    -- GUC takes effect for new sessions
    EXECUTE format(
        'ALTER DATABASE %I SET %s = %L',
        current_database(),
        NEW.key,
        NEW.value
    );

    -- GUC takes effect for current session
    EXECUTE format(
        'SET %s = %L',
        NEW.key,
        NEW.value
    );

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: set configuration parameter % to %', NEW.key, NEW.value;
    END IF;

    NOTIFY configuration_change;
    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE SECURITY DEFINER;


