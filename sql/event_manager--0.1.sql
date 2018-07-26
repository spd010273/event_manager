/*-----------------------------------------------------------------------------
 *
 * event_manager--0.1.sql
 *     Event Manager extension schema
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        event_manager--0.1.sql
 *
 *-----------------------------------------------------------------------------
 */

/*
 *  Note to user, this extension is configurable,
 *     within the head of this file, there are GUCs that can be set prior
 *     to installation that changes the behavior of this extension. These are
 *     enumerated below:
 *
 *     @extschema@.unlogged_queue: TRUE/FALSE - when true, creates queue tables as UNLOGGED, speeding up DML to these tables.
 *                                 The queues lose crash safety when this is TRUE, and will be truncated on crash recovery.
 *                                 Additionally, the queues will NOT be replicated when set to TRUE
 *     @extschema@.
 *
 *
 *  Note:
 *      9.6 compatibility is for 2-parameter current_setting()
 *      (introduced in 9.6) and to_jsonb (introduced in 9.5)
 */

/* Version Check */
DO
 $$
BEGIN
    IF( regexp_matches( version(), 'PostgreSQL (\d)+\.(\d+)\.(\d+)' )::INTEGER[] < ARRAY[9,6,0]::INTEGER[] ) THEN
        RAISE EXCEPTION 'Event Manager requires PostgreSQL 9.6 or above';
    END IF;
END
 $$
    LANGUAGE 'plpgsql';

SET @extschema@.unlogged_queue = 'FALSE';

CREATE SEQUENCE @extschema@.sq_pk_event_table;
CREATE TABLE @extschema@.tb_event_table
(
    event_table INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table'),
    schema_name VARCHAR NOT NULL DEFAULT 'public',
    table_name  VARCHAR(63) NOT NULL UNIQUE,
    no_trigger  BOOLEAN NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE @extschema@.tb_event_table IS 'Stores tables being watched for events';
COMMENT ON COLUMN @extschema@.tb_event_table.schema_name IS 'Stores the schema to which the table belongs';
COMMENT ON COLUMN @extschema@.tb_event_table.table_name IS 'Stores the table name of the relation';
COMMENT ON COLUMN @extschema@.tb_event_table.no_trigger IS 'Indicates that this entry is only referenced by target_event_table (to be used by middleware for determining scope of action)';

CREATE SEQUENCE @extschema@.sq_pk_action;
CREATE TABLE @extschema@.tb_action
(
    action              INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_action'),
    label               JSONB,
    query               VARCHAR,
    uri                 VARCHAR,
    method              VARCHAR(4),
    static_parameters   JSONB,
    use_ssl             BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK( uri IS NOT NULL OR query IS NOT NULL ),
    CHECK( ( method IS NULL OR method IN( 'PUT', 'POST', 'GET' ) ) )
);

CREATE SEQUENCE @extschema@.sq_pk_event_table_work_item;
CREATE TABLE @extschema@.tb_event_table_work_item
(
    event_table_work_item   INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table_work_item'),
    source_event_table      INTEGER NOT NULL REFERENCES @extschema@.tb_event_table,
    source_column_name      VARCHAR(63),
    target_event_table      INTEGER REFERENCES @extschema@.tb_event_table,
    action                  INTEGER NOT NULL REFERENCES @extschema@.tb_action,
    description             JSONB,
    transaction_label       VARCHAR,
    work_item_query         TEXT NOT NULL,
    when_function           VARCHAR DEFAULT current_setting( '@extschema@.default_when_function', TRUE )::VARCHAR,
    op                      CHAR(1)[],
    execute_asynchronously  BOOLEAN DEFAULT COALESCE( current_setting( '@extschema@.execute_asynchronously', TRUE )::BOOLEAN, TRUE ),
    CHECK( ( op <@ ARRAY[ 'I','U','D' ]::CHAR(1)[] ) )
);

CREATE UNIQUE INDEX ix_source_action_target_unique ON @extschema@.tb_event_table_work_item(
    COALESCE( target_event_table, -1 ),
    source_event_table,
    COALESCE( source_column_name, '' ),
    action
);

COMMENT ON TABLE @extschema@.tb_event_table_work_item IS 'A list of actions that should occur for any given event table';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.source_event_table IS 'Indicates the table that can trigger this work item';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.target_event_table IS 'Indicates the target of this work items action. Not necessary but useful for any user interface built around this';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.source_column_name IS 'Indicated the column of the source table this event if firing on. This is not used for selectively firing update triggers but to help prevent the user from implementing identical/similar events';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.action IS 'Foreign key to tb_action - indicates what this work item generates parameters for';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.description IS 'User-facing description for that this work item is / does';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.transaction_label IS 'Label for what the action is performing. Used in Cyanaudit integration';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.work_item_query IS 'Generates a list of parameters for the action. This query has named bind point for the columns in this table. Query should generate JSONB aliased as parameters';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.when_function IS 'Filters events entering tb_event_queue. Example prototype is fn_dummy_when_function. Function should return BOOLEAN';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.op IS 'Indicates what DML operation this work item applies: U - Update, I - Insert, D - Delete.';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.execute_asynchronously IS 'Determines what mode of execution this work item will be ran under.';

DO
 $_$
BEGIN
    IF( COALESCE( current_setting( '@extschema@.unlogged_queue', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        CREATE UNLOGGED TABLE @extschema@.tb_event_queue
        (
            foo BOOLEAN
        );
    ELSE
        CREATE TABLE @extschema@.tb_event_queue
        (
            foo BOOLEAN
        );
    END IF;
END
 $_$
    LANGUAGE 'plpgsql';

ALTER TABLE @extschema@.tb_event_queue
    DROP COLUMN foo,
    ADD COLUMN event_table_work_item INTEGER,
    ADD COLUMN uid INTEGER,
    ADD COLUMN recorded TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN pk_value INTEGER NOT NULL,
    ADD COLUMN op CHAR(1) NOT NULL,
    ADD COLUMN old JSONB,
    ADD COLUMN new JSONB,
    ADD COLUMN session_values JSONB,
    ADD CONSTRAINT op_check CHECK ( ( op IN( 'D', 'U', 'I' ) ) );

COMMENT ON TABLE @extschema@.tb_event_queue IS 'Queue for events arriving from tb_event_tables. Contents are copied from their corresponding event_table_work_item entry.';
COMMENT ON COLUMN @extschema@.tb_event_queue.event_table_work_item IS 'reference to the trigger deinition this event corresponds to';
COMMENT ON COLUMN @extschema@.tb_event_queue.uid IS 'Stores the optional session-level user identifier that triggered this event, set by the @extschema@.get_uid_function call';
COMMENT ON COLUMN @extschema@.tb_event_queue.recorded IS 'timestamp of when the event was triggered';
COMMENT ON COLUMN @extschema@.tb_event_queue.pk_value IS 'The primary key of the source_table whose modification caused this event to fire';
COMMENT ON COLUMN @extschema@.tb_event_queue.op IS 'The DML operation that caused this event to fire, either I, U, or D';
COMMENT ON COLUMN @extschema@.tb_event_queue.old IS 'Copy of the plpgsql OLD psuedorecord';
COMMENT ON COLUMN @extschema@.tb_event_queue.new IS 'Copy of the plpgsql new psuedorecord';
COMMENT ON COLUMN @extschema@.tb_event_queue.session_values IS 'Copy of the comma-delimited session GUCs specified in @extschema@.session_gucs';

DO
 $_$
BEGIN
    IF( COALESCE( current_setting( '@extschema@.unlogged_queue', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        CREATE UNLOGGED TABLE @extschema@.tb_work_queue
        (
            foo BOOLEAN
        );
    ELSE
        CREATE TABLE @extschema@.tb_work_queue
        (
            foo BOOLEAN
        );
    END IF;
END
 $_$
    LANGUAGE 'plpgsql';

ALTER TABLE @extschema@.tb_work_queue
    DROP COLUMN foo,
    ADD COLUMN parameters JSONB NOT NULL,
    ADD COLUMN action INTEGER NOT NULL REFERENCES @extschema@.tb_action,
    ADD COLUMN uid INTEGER,
    ADD COLUMN recorded TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN transaction_label VARCHAR,
    ADD COLUMN execute_asynchronously  BOOLEAN DEFAULT COALESCE( current_setting( '@extschema@.execute_asynchronously', TRUE )::BOOLEAN, TRUE ),
    ADD COLUMN session_values JSONB;

COMMENT ON TABLE @extschema@.tb_work_queue IS 'Queue for work_item_query results. Remaining contents copied from the corresponding event_queue entry';
COMMENT ON COLUMN @extschema@.tb_work_queue.parameters IS 'Parameters returned by work_item_query';
COMMENT ON COLUMN @extschema@.tb_work_queue.action IS 'Action that will be executed';
COMMENT ON COLUMN @extschema@.tb_work_queue.uid IS 'uid from event_queue';
COMMENT ON COLUMN @extschema@.tb_work_queue.recorded IS 'Recorded timestamp from event_queue';
COMMENT ON COLUMN @extschema@.tb_work_queue.transaction_label IS 'Label for transaction in Cyanaudit, if installed';
COMMENT ON COLUMN @extschema@.tb_work_queue.execute_asynchronously IS 'Indicates how this action should be executed';
COMMENT ON COLUMN @extschema@.tb_work_queue.session_values IS 'Copy of the session values from the event queue';

CREATE TABLE @extschema@.tb_setting
(
    key     VARCHAR,
    value   VARCHAR,
    CHECK( key ~ '^@extschema@\.' )
);
CREATE UNIQUE INDEX ix_unique_setting_key ON @extschema@.tb_setting( lower( key ) );

CREATE FUNCTION @extschema@.fn_set_configuration()
RETURNS TRIGGER AS
 $_$
BEGIN
    IF( NEW.key NOT LIKE '@extschema@.%' ) THEN
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

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE SECURITY DEFINER;

CREATE TRIGGER tr_set_configuration
    AFTER INSERT OR UPDATE ON @extschema@.tb_setting
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_set_configuration();

INSERT INTO @extschema@.tb_setting
            (
                key,
                value
            )
     VALUES ( '@extschema@.execute_asynchronously', 't' ),
            ( '@extschema@.set_uid_function', 'NULL' ),
            ( '@extschema@.get_uid_function', 'NULL' ),
            ( '@extschema@.default_when_function', '@extschema@.fn_dummy_when_function' ),
            ( '@extschema@.session_gucs', '' ),
            ( '@extschema@.base_url', 'localhost' );

CREATE SEQUENCE @extschema@.sq_pk_event_table_work_item_instance;
CREATE TABLE @extschema@.tb_event_table_work_item_instance
(
    event_table_work_item_instance INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table_work_item_instance'),
    event_table_work_item   INTEGER NOT NULL REFERENCES @extschema@.tb_event_table_work_item,
    source_pk               INTEGER,
    target_pk               INTEGER NOT NULL,
    metadata                JSONB
);

COMMENT ON TABLE @extschema@.tb_event_table_work_item_instance IS 'Can be used to aid work item queries or action queries when the scope of an action query is non-deterministic or too broad. This table is intended for use by any integrating application, but is not required';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item_instance.metadata IS 'Can be used to store static parameters for an instance of a event_table_work_item';

CREATE OR REPLACE FUNCTION @extschema@.fn_dummy_when_function
(
    in_event_table_work_item    INTEGER,
    in_pk_value                 INTEGER,
    in_op                       CHAR(1),
    in_new                      JSONB,
    in_old                      JSONB
)
RETURNS BOOLEAN AS
 $_$
    SELECT TRUE;
 $_$
    LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_test_work_item_query()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_work_item_query  VARCHAR;
BEGIN
    my_work_item_query := regexp_replace( NEW.work_item_query, '\?[\.\w]+\?', 'NULL', 'g' );

    EXECUTE 'CREATE TEMP TABLE tt_work_item_test AS( ' || my_work_item_query || ' LIMIT 0)';

    PERFORM a.attname
       FROM pg_attribute a
 INNER JOIN pg_class c
         ON c.oid = a.attrelid
        AND c.relname::VARCHAR = 'tt_work_item_test'
      WHERE a.attnum > 0
        AND a.attname = 'parameters';

    IF NOT FOUND THEN
        DROP TABLE IF EXISTS tt_work_item_test;
        RAISE EXCEPTION 'work_item_query does not return a ''parameters'' JSONB column';
    END IF;

    DROP TABLE IF EXISTS tt_work_item_test;
    RETURN NEW;
END
 $_$
    LANGUAGE plpgsql;

CREATE TRIGGER tr_work_item_query_test
    AFTER INSERT OR UPDATE ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_test_work_item_query();

CREATE OR REPLACE FUNCTION @extschema@.fn_catalog_check()
RETURNS TRIGGER AS
 $_$
BEGIN
    PERFORM *
       FROM pg_class c
 INNER JOIN pg_namespace n
         ON n.oid = c.relnamespace
      WHERE c.relname::VARCHAR = NEW.table_name
        AND n.nspname::VARCHAR = NEW.schema_name
        AND c.relkind::CHAR IN( 'm', 'v', 'r' );

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Event table does not exist or is not a relation.';
    END IF;

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: Catalog check for event table passed';
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_event_table_catalog_check
    AFTER INSERT OR UPDATE ON @extschema@.tb_event_table
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_catalog_check();

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

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: event_enqueue - uid %', my_uid;
    END IF;

    FOR my_when_function,
        my_event_table_work_item
                        IN(
                            SELECT etwi.when_function,
                                   etwi.event_table_work_item
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
                            now(),
                            my_pk_value,
                            substr( TG_OP, 1, 1 ),
                            old_record,
                            new_record,
                            my_guc_values
                        );

            IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
                RAISE DEBUG '@extschema@: event enqueued';
            END IF;
        END IF;
    END LOOP;
    RETURN my_record;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE OR REPLACE FUNCTION @extschema@.fn_no_ddl_check()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_query TEXT;
    my_regexp TEXT;
BEGIN
    /*
     * Try to prevent malicious behavior by preventing DDL or overly broad DML from being ran
     * This is in no way exhaustive or complete.
     */
    EXECUTE 'SELECT $1.' || TG_ARGV[0]::VARCHAR
       INTO my_query
      USING NEW;

    IF( my_query IS NULL ) THEN
        RETURN NEW;
    END IF;

    FOR my_regexp IN(
                        SELECT unnest( ARRAY[
                            'drop\s+',
                            'created\+', -- We'll handle CREATE TEMP TABLE ... as a special case
                            'alter\s+',
                            'grant\s+',
                            'revoke\s+'
                        ]::TEXT[] )
                    ) LOOP
        PERFORM regexp_matches( my_query, my_regexp, 'i' );

        IF FOUND THEN
            PERFORM regexp_matches( my_query, 'create\s+((global\s+)|(local\s+))?temp', 'i' );

            IF FOUND THEN
                CONTINUE;
            END IF;
            RAISE EXCEPTION 'Cannot have DDL within %.%.%', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_ARGV[0];
        END IF;
    END LOOP;

    PERFORM regexp_matches( my_query, 'truncate\s+', 'i' );

    IF FOUND THEN
        RAISE EXCEPTION 'Cannot have TRUNCATE within %.%.%', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_ARGV[0];
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql';

CREATE TRIGGER tr_no_ddl_check
    BEFORE INSERT OR UPDATE ON @extschema@.tb_action
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_no_ddl_check( 'query' );

CREATE TRIGGER tr_no_ddl_check
    BEFORE INSERT OR UPDATE ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_no_ddl_check( 'work_item_query' );

CREATE FUNCTION @extschema@.fn_new_event_trigger()
RETURNS TRIGGER AS
 $_$
DECLARE
    my_pk_column    VARCHAR;
BEGIN
    SELECT a.attname::VARCHAR
      INTO my_pk_column
      FROM pg_class c
INNER JOIN pg_namespace n
        ON n.oid = c.relnamespace
INNER JOIN pg_attribute a
        ON a.attrelid = c.oid
INNER JOIN pg_constraint cn
        ON cn.conrelid = c.oid
       AND cn.contype = 'p'
       AND cn.conkey[1] = a.attnum
     WHERE c.relname::VARCHAR = NEW.table_name
       AND n.nspname::VARCHAR = NEW.schema_name;

    IF( my_pk_column IS NULL ) THEN
        RAISE EXCEPTION 'Target table, %.% needs to have a surrogate integer primary key!',
            NEW.schema_name,
            NEW.table_name;
    END IF;

    IF( NEW.no_trigger IS TRUE ) THEN
        RETURN NEW;
    END IF;

    EXECUTE format(
                'CREATE TRIGGER tr_event_enqueue '
             || '    AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
             || '    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_enqueue_event( %L );',
                NEW.schema_name,
                NEW.table_name,
                my_pk_column
            );

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: created trigger on %.%', NEW.schema_name, NEW.column_name;
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_new_enqueue_trigger
    AFTER INSERT ON @extschema@.tb_event_table
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_new_event_trigger();

CREATE FUNCTION @extschema@.fn_remove_event_trigger()
RETURNS TRIGGER AS
 $_$
BEGIN
    IF( OLD.no_trigger IS TRUE ) THEN
        RETURN OLD;
    END IF;

    EXECUTE format(
                'DROP TRIGGER tr_event_enqueue '
             || ' ON %I.%I',
                OLD.schema_name,
                OLD.table_name
            );

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: dropped event trigger on %.%', OLD.schema_name, OLD.table_name;
    END IF;

    RETURN OLD;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_remove_enqueue_trigger
    AFTER DELETE ON @extschema@.tb_event_table
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_remove_event_trigger();

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
    my_is_async := TRUE;
    SELECT COALESCE(
               etwi.execute_asynchronously,
               CASE WHEN lower( value ) LIKE '%t%'
                    THEN TRUE
                    WHEN lower( value ) LIKE '%f%'
                    THEN FALSE
                    ELSE NULL
                     END
           ) AS is_async,
           etwi.work_item_query,
           etwi.action,
           etwi.transaction_label
      INTO my_is_async,
           my_query,
           my_action,
           my_transaction_label
      FROM @extschema@.tb_event_table_work_item etwi
 LEFT JOIN @extschema@.tb_setting s
        ON s.key = '@extschema@.execute_asynchronously'
     WHERE etwi.event_table_work_item = NEW.event_table_work_item;

    IF( my_is_async IS TRUE ) THEN
        NOTIFY new_event_queue_item;

        IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
            RAISE DEBUG '@extschema@: event processing - async notify sent';
        END IF;
        RETURN NEW;
    END IF;

    EXECUTE 'SELECT ' || COALESCE(
                current_setting( '@extschema@.get_uid_function', TRUE ),
                'NULL'
            ) || '::INTEGER'
      INTO my_uid;

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
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

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
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

        IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
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

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: Processed event queue item';
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_handle_new_event_queue_item
    AFTER INSERT ON @extschema@.tb_event_queue
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_handle_new_event_queue_item();

CREATE FUNCTION @extschema@.fn_handle_new_work_queue_item()
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

    SELECT COALESCE(
               NEW.execute_asynchronously,
               CASE WHEN lower( value ) LIKE '%t%'
                    THEN TRUE
                    ELSE FALSE
                     END
           ) AS is_async
      INTO my_is_async
      FROM @extschema@.tb_setting
     WHERE key = '@extschema@.execute_asynchronously';

    IF( my_is_async IS TRUE ) THEN
        NOTIFY new_work_queue_item;

        IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
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

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: work processing - final query is %', my_query;
    END IF;

    EXECUTE my_set_uid_function;
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

        IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
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

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: Processed work queue item';
    END IF;
    RETURN NULL;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_handle_new_work_queue_item
    AFTER INSERT ON @extschema@.tb_work_queue
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_handle_new_work_queue_item();

CREATE FUNCTION @extschema@.fn_validate_function()
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

    IF( COALESCE( current_setting( '@extschema@.debug', TRUE )::BOOLEAN, FALSE ) IS TRUE ) THEN
        RAISE DEBUG '@extschema@: when function validated';
    END IF;

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_validate_when_function
    BEFORE INSERT OR UPDATE OF when_function ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_validate_function();

CREATE FUNCTION @extschema@.fn_validate_work_item_reference()
RETURNS TRIGGER AS
 $_$
BEGIN
    -- Perform sanity check on tb_event_table
    PERFORM *
       FROM @extschema@.tb_event_table et
 INNER JOIN @extschema@.tb_event_table_work_item etwi
         ON etwi.source_event_table = et.event_table
        AND etwi.event_table_work_item = NEW.event_table_work_item
      WHERE et.no_trigger IS TRUE;

    IF FOUND THEN
        RAISE EXCEPTION 'Event table work item references a table that does not have a trigger on it. Flip the no_trigger bit on tb_event_table prior to creating events on it';
    END IF;
    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' STABLE PARALLEL UNSAFE;

CREATE TRIGGER tr_event_table_triggering_sanity_check
    AFTER INSERT OR UPDATE OF source_event_table ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_validate_work_item_reference();

GRANT ALL ON @extschema@.tb_event_queue TO public;
GRANT ALL ON @extschema@.tb_work_queue TO public;
GRANT SELECT ON @extschema@.tb_event_table_work_item TO public;
GRANT SELECT ON @extschema@.tb_action TO public;
GRANT SELECT ON @extschema@.tb_setting TO public;
GRANT SELECT ON @extschema@.tb_event_table TO public;
GRANT ALL ON @extschema@.tb_event_table_work_item_instance TO public;
GRANT USAGE, SELECT ON SEQUENCE @extschema@.sq_pk_event_table_work_item_intsance TO public;
GRANT USAGE ON SCHEMA @extschema@ TO public;
