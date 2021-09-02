/*------------------------------------------------------------------------
 *
 * event_manager--0.1--0.2.sql
 *     Upgrade script for transitioning from version 0.1 to 0.2
 *
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        event_manager--0.1--0.2.sql
 *
 *------------------------------------------------------------------------
 */

CREATE OR REPLACE FUNCTION @extschema@.fn_get_boolean_guc_value
(
    in_name VARCHAR
)
RETURNS BOOLEAN AS
 $_$
    WITH tt_base_tuple AS
    (   -- Just need a tuple so the function doesn't return void
        SELECT in_name AS guc_name
    )
        SELECT COALESCE(
                   CASE WHEN lower( current_setting( tt.guc_name, TRUE )::VARCHAR ) LIKE 't%'
                          OR current_setting( tt.guc_name, TRUE )::VARCHAR = '1'
                        THEN TRUE
                        WHEN lower( current_setting( tt.guc_name, TRUE )::VARCHAR ) LIKE 'f%'
                          OR current_setting( tt.guc_name, TRUE )::VARCHAR = '0'
                        THEN FALSE
                        ELSE NULL::BOOLEAN
                         END,
                   CASE WHEN lower( s.value ) LIKE 't%'
                          OR s.value = '1'
                        THEN TRUE
                        WHEN lower( s.value ) LIKE 'f%'
                          OR s.value = '0'
                        THEN FALSE
                        ELSE NULL::BOOLEAN
                         END
               ) AS result
          FROM tt_base_tuple tt
     LEFT JOIN @extschema@.tb_setting s
            ON s.key = tt.guc_name;
 $_$
    LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_handle_new_event_queue_item()
RETURNS TRIGGER AS
  $_$
DECLARE
    my_is_async     BOOLEAN;
    my_query        TEXT;
    my_parameters   JSONB;
    my_uid          INTEGER;
    my_action       INTEGER;
    my_transaction_label    VARCHAR;
    my_key          VARCHAR;
    my_value        VARCHAR;
BEGIN
    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.disable_event_queue' ) IS TRUE ) THEN
        RETURN NEW;
    END IF;

    my_is_async := TRUE;
    SELECT COALESCE(
               etwi.execute_asynchronously,
               @extschema@.fn_get_boolean_guc_value( '@extschema@.execute_asynchronously' )
           ) AS is_async,
           etwi.work_item_query,
           etwi.action,
           etwi.transaction_label
      INTO my_is_async,
           my_query,
           my_action,
           my_transaction_label
      FROM @extschema@.tb_event_table_work_item etwi
     WHERE etwi.event_table_work_item = NEW.event_table_work_item;

    IF( my_is_async IS TRUE ) THEN
        NOTIFY new_event_queue_item;

        IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
            RAISE DEBUG '@extschema@: event processing - async notify sent';
        END IF;
        RETURN NEW;
    END IF;

    EXECUTE 'SELECT ' || COALESCE(
                current_setting( '@extschema@.get_uid_function', TRUE ),
                'NULL'
            ) || '::INTEGER'
      INTO my_uid;

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: event processing - uid %', my_uid;
    END IF;

    my_query := regexp_replace( my_query, '\?pk_value\?', NEW.pk_value::VARCHAR, 'g' );
    my_query := regexp_replace( my_query, '\?recorded\?', quote_nullable( NEW.recorded::VARCHAR ), 'g' );
    my_query := regexp_replace( my_query, '\?uid\?', quote_nullable( NEW.uid::VARCHAR ), 'g' );
    my_query := regexp_replace( my_query, '\?op\?', quote_nullable( NEW.op::VARCHAR ), 'g' );
    my_query := regexp_replace( my_query, '\?event_table_work_item\?', NEW.event_table_work_item::VARCHAR, 'g' );

    FOR my_key, my_value IN(
                                SELECT 'OLD.' || key,
                                       value
                                  FROM jsonb_each_text( NEW.old )
                                 UNION ALL
                                SELECT 'NEW.' || key,
                                       value
                                  FROM jsonb_each_text( NEW.new )
                           ) LOOP
        my_query := regexp_replace( my_query, '\?' || my_key || '\?', quote_nullable( my_value ), 'g' );
    END LOOP;

    FOR my_key, my_value IN(
                            SELECT key,
                                   value
                              FROM jsonb_each_text( NEW.session_values )
                           ) LOOP
        my_query := regexp_replace( my_query, '\?' || my_key || '\?', quote_nullable( my_value ), 'g' );
    END LOOP;

    -- Replace any remaining bindpoints with NULL
    my_query := regexp_replace( my_query, '\?(((OLD)|(NEW))\.)?\w+\?', 'NULL', 'g' );

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: Event processing - Final query %', my_query;
    END IF;

    FOR my_parameters IN EXECUTE my_query LOOP
        INSERT INTO @extschema@.tb_work_queue
                    (
                        parameters,
                        uid,
                        recorded,
                        transaction_label,
                        action,
                        execute_asynchronously,
                        session_values
                    )
             VALUES
                    (
                        my_parameters,
                        my_uid,
                        NEW.recorded,
                        my_transaction_label,
                        my_action,
                        my_is_async,
                        NEW.session_values
                    );

        IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
            RAISE DEBUG '@extschema@: Created work queue row (%,%,%,%,%,%,%)',
                        my_parameters,
                        my_uid,
                        NEW.recorded,
                        my_transaction_label,
                        my_action,
                        my_is_async,
                        NEW.session_values;
        END IF;
    END LOOP;

    DELETE FROM @extschema@.tb_event_queue eq
          WHERE eq.event_table_work_item IS NOT DISTINCT FROM NEW.event_table_work_item
            AND eq.uid IS NOT DISTINCT FROM NEW.uid
            AND eq.recorded IS NOT DISTINCT FROM NEW.recorded
            AND eq.pk_value IS NOT DISTINCT FROM NEW.pk_value
            AND eq.op IS NOT DISTINCT FROM NEW.op
            AND eq.old::VARCHAR IS NOT DISTINCT FROM NEW.old::VARCHAR
            AND eq.new::VARCHAR IS NOT DISTINCT FROM NEW.new::VARCHAR;

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: Processed event queue item';
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_handle_new_work_queue_item()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_is_async          BOOLEAN;
    my_query             TEXT;
    my_static_parameters JSONB;
    my_key               TEXT;
    my_value             TEXT;
    my_set_uid_query     TEXT;
BEGIN
    my_is_async := TRUE;

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.disable_work_queue' ) IS TRUE ) THEN
        RETURN NEW;
    END IF;

    SELECT COALESCE(
               NEW.execute_asynchronously,
               @extschema@.fn_get_boolean_guc_value( '@extschema@.execute_asynchronously' )
           )
      INTO my_is_async;

    IF( my_is_async IS TRUE ) THEN
        NOTIFY new_work_queue_item;

        IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
            RAISE DEBUG '@extschema@: work processing - sent async notify';
        END IF;
        RETURN NULL;
    END IF;

    SELECT static_parameters,
           query
      INTO my_static_parameters,
           my_query
      FROM @extschema@.tb_action
     WHERE action = NEW.action;

    IF( my_query IS NULL ) THEN
        RAISE NOTICE 'Cannot execute API endpoint call in synchronous mode!';
        NOTIFY new_work_queue_item;
        RETURN NULL;
    END IF;

    SELECT 'SELECT ' || COALESCE(
                    regexp_replace(
                        COALESCE( current_setting( '@extschema@.set_uid_function', TRUE ), 'NULL' ),
                        '\?uid\?',
                        quote_nullable( NEW.uid ),
                        'g'
                    ),
                    'NULL'
           )
      INTO my_set_uid_query;

    my_query := regexp_replace( my_query, '\?recorded\?', quote_nullable( NEW.recorded::VARCHAR ), 'g' );
    my_query := regexp_replace( my_query, '\?uid\?', quote_nullable( NEW.uid::VARCHAR ), 'g' );
    my_query := regexp_replace( my_query, '\?transaction_label\?', quote_nullable( NEW.transaction_label::VARCHAR ), 'g' );

    FOR my_key, my_value IN(
                            SELECT key,
                                   value
                              FROM jsonb_each_text( NEW.parameters )
                           ) LOOP
        my_query := regexp_replace( my_query, '\?' || my_key || '\?', quote_nullable( my_value ), 'g' );
    END LOOP;

    FOR my_key, my_value IN(
                            SELECT key,
                                   value
                              FROM jsonb_each_text( my_static_parameters )
                           ) LOOP
        my_query := regexp_replace( my_query, '\?' || my_key || '\?', quote_nullable( my_value ), 'g' );
    END LOOP;

    FOR my_key, my_value IN(
                            SELECT key,
                                   value
                              FROM jsonb_each_text( NEW.session_values )
                           ) LOOP
        my_query := regexp_replace( my_query, '\?' || my_key || '\?', quote_nullable( my_value ), 'g' );
        my_set_uid_query := regexp_replace( my_set_uid_query, '\?' || my_key || '\?', quote_nullable( my_value ), 'g' );
    END LOOP;

    my_query := regexp_replace( my_query, '\?(((OLD)|(NEW))\.)?\w+\?', 'NULL', 'g' );

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: work processing - final query is %', my_query;
    END IF;

    EXECUTE my_set_uid_query;
    EXECUTE my_query;

    PERFORM p.proname
       FROM pg_proc p
 INNER JOIN pg_namespace n
         ON n.oid = p.pronamespace
        AND n.nspname::VARCHAR = 'cyanaudit'
      WHERE p.proname = 'fn_label_transaction';

    IF FOUND THEN
        EXECUTE 'SELECT fn_label_transaction( $1 )'
          USING NEW.transaction_label;

        IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
            RAISE DEBUG '@extschema@: cyanaudit hook fired';
        END IF;
    END IF;

    DELETE FROM @extschema@.tb_work_queue
          WHERE parameters::VARCHAR IS NOT DISTINCT FROM NEW.parameters::VARCHAR
            AND action IS NOT DISTINCT FROM NEW.action
            AND uid IS NOT DISTINCT FROM NEW.uid
            AND recorded IS NOT DISTINCT FROM NEW.recorded
            AND transaction_label IS NOT DISTINCT FROM NEW.transaction_label
            AND execute_asynchronously IS NOT DISTINCT FROM NEW.execute_asynchronously;

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: Processed work queue item';
    END IF;
    RETURN NULL;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_enqueue_event()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_pk_value                 INTEGER;
    my_when_function            VARCHAR;
    my_when_result              BOOLEAN;
    my_record                   RECORD;
    new_record                  JSONB;
    old_record                  JSONB;
    my_uid                      INTEGER;
    my_event_table_work_item    INTEGER;
    my_guc_values               JSONB;
    my_source_columns           VARCHAR;
    my_update_diff              BOOLEAN;
BEGIN
    IF( TG_OP = 'INSERT' ) THEN
        my_record := NEW;
        new_record := to_jsonb( NEW );
        old_record := NULL;
    ELSIF( TG_OP = 'UPDATE' ) THEN
        IF( NEW::VARCHAR IS DISTINCT FROM OLD::VARCHAR ) THEN
            my_record := NEW;
            new_record := to_jsonb( NEW );
            old_record := to_jsonb( OLD );
        ELSE
            -- Reject dubious UPDATE
            RETURN NEW;
        END IF;
    ELSE
        my_record := OLD;
        old_record := to_jsonb( OLD );
        new_record := NULL;
    END IF;

    IF( TG_ARGV[0] IS NULL ) THEN
        RAISE NOTICE 'Unable to enqueue event: NULL pk_column provided';
        RETURN my_record;
    END IF;

    EXECUTE 'SELECT $1.' || TG_ARGV[0]::VARCHAR
       INTO my_pk_value
      USING my_record;

    EXECUTE 'SELECT ' || COALESCE( current_setting( '@extschema@.get_uid_function', TRUE ),
                        'NULL'
                    ) || '::INTEGER'
       INTO my_uid;

    IF( length( current_setting( '@extschema@.session_gucs', TRUE ) ) > 0 ) THEN
        SELECT jsonb_object(
                   array_agg( x ORDER BY x ),
                   array_agg( current_setting( x, TRUE ) ORDER BY x )
               )
          INTO my_guc_values
          FROM regexp_split_to_table(
                   current_setting( '@extschema@.session_gucs', TRUE ),
                   ','
               ) x;
    END IF;

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: event_enqueue - uid %', my_uid;
    END IF;

    FOR my_when_function,
        my_event_table_work_item,
        my_source_columns
                        IN(
                            SELECT etwi.when_function,
                                   etwi.event_table_work_item,
                                   etwi.source_column_name
                              FROM @extschema@.tb_event_table_work_item etwi
                        INNER JOIN @extschema@.tb_event_table et
                                ON et.event_table = etwi.source_event_table
                               AND et.table_name = TG_TABLE_NAME::VARCHAR
                               AND et.schema_name = TG_TABLE_SCHEMA::VARCHAR
                               AND (
                                        substr( TG_OP, 1, 1 ) = ANY( etwi.op )
                                     OR etwi.op IS NULL
                                   )
                         ) LOOP
        EXECUTE 'SELECT ' || my_when_function
             || '( $1::INTEGER, $2::INTEGER, $3::CHAR(1), $4::JSONB, $5::JSONB )::BOOLEAN'
           INTO my_when_result
          USING my_event_table_work_item,
                my_pk_value,
                substr( TG_OP, 1, 1 ), -- Get either 'U', 'I', or 'D'
                new_record,
                old_record;

        IF( TG_OP = 'UPDATE' AND my_source_columns IS NOT NULL ) THEN
            -- Check new / old to see if the source_column_names for this event have actually changed
            SELECT TRUE = ANY(
                       array_agg(
                           new_record->>x IS DISTINCT FROM old_record->>x
                       )
                   ) AS diff
              INTO my_update_diff
              FROM unnest( string_To_array( my_source_columns, ',' ) ) x;

            CONTINUE WHEN my_update_diff IS FALSE;
        END IF;

        IF( my_when_result IS TRUE ) THEN
            INSERT INTO @extschema@.tb_event_queue
                        (
                            event_table_work_item,
                            uid,
                            recorded,
                            pk_value,
                            op,
                            old,
                            new,
                            session_values
                        )
                 VALUES
                        (
                            my_event_table_work_item,
                            my_uid,
                            clock_timestamp(),
                            my_pk_value,
                            substr( TG_OP, 1, 1 ),
                            old_record,
                            new_record,
                            my_guc_values
                        );

            IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
                RAISE DEBUG '@extschema@: event enqueued';
            END IF;
        END IF;
    END LOOP;
    RETURN my_record;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_validate_event_table_work_item()
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

CREATE OR REPLACE FUNCTION @extschema@.fn_validate_function()
RETURNS TRIGGER AS
 $_$
BEGIN
    PERFORM *
       FROM pg_proc p
 INNER JOIN pg_namespace n
         ON n.oid = p.pronamespace
      WHERE regexp_replace( NEW.when_function, '^.*\.',  '' ) = p.proname::VARCHAR
        AND (
                (
                    regexp_replace( NEW.when_function, '\..*$', '' )
                  = regexp_replace( NEW.when_function, '^.*\.', '' )
              AND n.nspname::VARCHAR = 'public'
                )
             OR regexp_replace( NEW.when_function, '\..*$', '' ) = n.nspname::VARCHAR
            );
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Function % is not in catalog', NEW.when_function;
    END IF;

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: when function validated';
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

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

    IF( @extschema@.fn_get_boolean_guc_value( '@extschema@.debug' ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: set configuration parameter % to %', NEW.key, NEW.value;
    END IF;

    IF( NEW.key IN( '@extschema@.override_work_process_count', '@extschema@.override_event_process_count' ) ) THEN
        -- These are periodically polled by the parent process
        RETURN NEW;
    END IF;

    NOTIFY configuration_update;
    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE SECURITY DEFINER;
