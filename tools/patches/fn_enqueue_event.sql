CREATE OR REPLACE FUNCTION event_manager.fn_enqueue_event()
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

    EXECUTE 'SELECT ' || COALESCE( current_setting( 'event_manager.get_uid_function', TRUE ),
                        'NULL'
                    ) || '::INTEGER'
       INTO my_uid;

    IF( length( current_setting( 'event_manager.session_gucs', TRUE ) ) > 0 ) THEN
        SELECT jsonb_object(
                   array_agg( x ORDER BY x ),
                   array_agg( current_setting( x, TRUE ) ORDER BY x )
               )
          INTO my_guc_values
          FROM regexp_split_to_table(
                   current_setting( 'event_manager.session_gucs', TRUE ),
                   ','
               ) x;
    END IF;

    IF( COALESCE( current_setting( 'event_manager.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG 'event_manager: event_enqueue - uid %', my_uid;
    END IF;

    FOR my_when_function,
        my_event_table_work_item,
        my_source_columns
                        IN(
                            SELECT etwi.when_function,
                                   etwi.event_table_work_item,
                                   etwi.source_column_name
                              FROM event_manager.tb_event_table_work_item etwi
                        INNER JOIN event_manager.tb_event_table et
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
            INSERT INTO event_manager.tb_event_queue
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

            IF( COALESCE( current_setting( 'event_manager.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
                RAISE DEBUG 'event_manager: event enqueued';
            END IF;
        END IF;
    END LOOP;
    RETURN my_record;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;
