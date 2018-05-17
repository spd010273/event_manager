/* Version Check */
DO
 $$
BEGIN
    IF( regexp_matches( version(), 'PostgreSQL (\d)+\.(\d+)\.(\d+)' )::INTEGER[] < ARRAY[9,4,0]::INTEGER[] ) THEN
        RAISE EXCEPTION 'Event Manager requires PostgreSQL 9.4 or above';
    END IF;
END
 $$
    LANGUAGE 'plpgsql';

CREATE SEQUENCE @extschema@.sq_pk_event_table;
CREATE TABLE @extschema@.tb_event_table
(
    event_table INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table'),
    schema_name VARCHAR NOT NULL DEFAULT 'public',
    table_name  VARCHAR(63) NOT NULL UNIQUE,
    column_name VARCHAR[]
);

COMMENT ON TABLE @extschema@.tb_event_table IS 'Stores tables being watched for events';
COMMENT ON COLUMN @extschema@.tb_event_table.schema_name IS 'Stores the schema to which the table belongs';
COMMENT ON COLUMN @extschema@.tb_event_table.table_name IS 'Stores the table name of the relation';

CREATE SEQUENCE @extschema@.sq_pk_action;
CREATE TABLE @extschema@.tb_action
(
    action              INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_action'),
    label               JSONB,
    query               VARCHAR,
    uri                 VARCHAR,
    method              VARCHAR(4),
    static_parameters   JSONB,
    CHECK( uri IS NOT NULL OR query IS NOT NULL ),
    CHECK( ( method IN( 'PUT', 'POST', 'GET' ) ) )
);

CREATE SEQUENCE @extschema@.sq_pk_event_table_work_item;
CREATE TABLE @extschema@.tb_event_table_work_item
(
    event_table_work_item   INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table_work_item'),
    source_event_table      INTEGER NOT NULL REFERENCES @extschema@.tb_event_table,
    target_event_table      INTEGER REFERENCES @extschema@.tb_event_table,
    action                  INTEGER NOT NULL REFERENCES @extschema@.tb_action,
    description             JSONB,
    transaction_label       VARCHAR,
    work_item_query         TEXT NOT NULL,
    when_function           VARCHAR DEFAULT current_setting( '@extschema@.default_when_function', TRUE )::VARCHAR,
    op                      CHAR(1),
    execute_asynchronously  BOOLEAN DEFAULT COALESCE( current_setting( '@extschema@.execute_asynchronously', TRUE )::BOOLEAN, TRUE ),
    CHECK( ( op IN( 'D', 'I', 'U' ) ) )
);
CREATE UNIQUE INDEX ix_source_action_target_unique ON @extschema@.tb_event_table_work_item(
    COALESCE( target_event_table, -1 ),
    source_event_table,
    action
);

COMMENT ON TABLE @extschema@.tb_event_table_work_item IS 'A list of actions that should occur for any given event table';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.source_event_table IS 'Indicates the table that can trigger this work item';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.target_event_table IS 'Indicates the target of this work items action. Not necessary but useful for any user interface built around this';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.action IS 'Foreign key to tb_action - indicates what this work item generates parameters for';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.description IS 'User-facing description for that this work item is / does';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.transaction_label IS 'Label for what the action is performing. Used in Cyanaudit integration';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.work_item_query IS 'Generates a list of parameters for the action. This query has named bind point for the columns in this table. Query should generate JSONB aliased as parameters';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.when_function IS 'Filters events entering tb_event_queue. Example prototype is fn_dummy_when_function. Function should return BOOLEAN';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.op IS 'Indicates what DML operation this work item applies: U - Update, I - Insert, D - Delete.';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.execute_asynchronously IS 'Determines what mode of execution this work item will be ran under.';

CREATE TABLE @extschema@.tb_event_queue
(
    event_table_work_item   INTEGER,
    uid                     INTEGER,
    recorded                TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    pk_value                INTEGER NOT NULL,
    op                      CHAR(1),
    execute_asynchronously  BOOLEAN DEFAULT COALESCE( current_setting( '@extschema@.execute_asynchronously', TRUE )::BOOLEAN, TRUE ),
    transaction_label       VARCHAR,
    work_item_query         TEXT,
    action                  INTEGER,
    CHECK( (  op IN( 'D', 'U', 'I' ) ) )
);

COMMENT ON TABLE @extschema@.tb_event_queue IS 'Queue for events arriving from tb_event_tables. Contents are copied from their corresponding event_table_work_item entry.';

CREATE TABLE @extschema@.tb_work_queue
(
    parameters              JSONB NOT NULL,
    action                  INTEGER NOT NULL REFERENCES @extschema@.tb_action,
    uid                     INTEGER,
    recorded                TIMESTAMP NOT NULL DEFAULT clock_timestamp(),
    transaction_label       VARCHAR,
    execute_asynchronously  BOOLEAN DEFAULT COALESCE( current_setting( '@extschema@.execute_asynchronously', TRUE )::BOOLEAN, TRUE )
);

COMMENT ON TABLE @extschema@.tb_work_queue IS 'Queue for work_item_query results. Remaining contents copied from the corresponding event_queue entry';

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

    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || NEW.key || ' = ''' || NEW.value || '''';
    EXECUTE 'SET ' || NEW.key || ' = ''' || NEW.value || '''';
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
            ( '@extschema@.default_when_function', '@extschema@.fn_dummy_when_function' );

CREATE SEQUENCE @extschema@.sq_pk_event_table_work_item_instance;
CREATE TABLE @extschema@.tb_event_table_work_item_instance
(
    event_table_work_item_instance INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table_work_item_instance'),
    event_table_work_item   INTEGER NOT NULL REFERENCES @extschema@.tb_event_table_work_item,
    target_pk               INTEGER NOT NULL
);

COMMENT ON TABLE @extschema@.tb_event_table_work_item_instance IS 'Can be used to aid work item queries or action queries when the scope of an action query is non-deterministic or too broad.';

CREATE OR REPLACE FUNCTION @extschema@.fn_dummy_when_function
(
    in_event_table_work_item    INTEGER,
    in_pk_value                 INTEGER,
    in_op                       CHAR(1)
)
RETURNS BOOLEAN AS
 $_$
    SELECT TRUE;
 $_$
    LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

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
    my_uid                      INTEGER;
    my_transaction_label        VARCHAR;
    my_action                   INTEGER;
    my_execute_asynchronously   BOOLEAN;
    my_work_item_query          VARCHAR;
    my_event_table_work_item    INTEGER;
BEGIN
    IF( TG_OP = 'INSERT' ) THEN
        my_record := NEW;
    ELSIF( TG_OP = 'UPDATE' ) THEN
        IF( NEW::VARCHAR IS DISTINCT FROM OLD::VARCHAR ) THEN
            my_record := NEW;
        ELSE
            -- Reject dubious UPDATE
            RETURN NEW;
        END IF;
    ELSE
        my_record := OLD;
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

    FOR my_when_function,
        my_transaction_label,
        my_action,
        my_execute_asynchronously,
        my_work_item_query,
        my_event_table_work_item
                        IN(
                            SELECT when_function,
                                   transaction_label,
                                   action,
                                   execute_asynchronously,
                                   work_item_query,
                                   event_table_work_item
                              FROM @extschema@.tb_event_table_work_item etwi
                        INNER JOIN @extschema@.tb_event_table et
                                ON et.event_table = etwi.source_event_table
                               AND et.table_name = TG_TABLE_NAME::VARCHAR
                               AND et.schema_name = TG_TABLE_SCHEMA::VARCHAR
                               AND (
                                        etwi.op = substr( TG_OP, 1, 1 )
                                     OR etwi.op IS NULL
                                   )
                         ) LOOP
        EXECUTE 'SELECT ' || my_when_function
             || '( $1::INTEGER, $2::INTEGER, $3::CHAR(1) )::BOOLEAN'
           INTO my_when_result
          USING my_event_table_work_item,
                my_pk_value,
                substr( TG_OP, 1, 1 ); -- Get either 'U', 'I', or 'D'

        IF( my_when_result IS TRUE ) THEN
            INSERT INTO @extschema@.tb_event_queue
                        (
                            event_table_work_item,
                            uid,
                            recorded,
                            pk_value,
                            op,
                            execute_asynchronously,
                            transaction_label,
                            work_item_query,
                            action
                        )
                 VALUES
                        (
                            my_event_table_work_item,
                            my_uid,
                            now(),
                            my_pk_value,
                            substr( TG_OP, 1, 1 ),
                            my_execute_asynchronously,
                            my_transaction_label,
                            my_work_item_query,
                            my_action
                        );
        END IF;
    END LOOP;
    RETURN my_record;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

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

    EXECUTE format(
                'CREATE TRIGGER tr_event_enqueue '
             || '    AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
             || '    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_enqueue_event( %L );',
                NEW.schema_name,
                NEW.table_name,
                my_pk_column
            );
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
    EXECUTE format(
                'DROP TRIGGER tr_event_enqueue '
             || ' ON %I.%I',
                OLD.schema_name,
                OLD.table_name
            );

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
BEGIN
    my_is_async := TRUE;
    SELECT COALESCE(
               NEW.execute_asynchronously,
               CASE WHEN lower( value ) LIKE '%t%'
                    THEN TRUE
                    WHEN lower( value ) LIKE '%f%'
                    THEN FALSE
                    ELSE NULL
                     END
           ) AS is_async
      INTO my_is_async
      FROM @extschema@.tb_setting
     WHERE key = '@extschema@.execute_asynchronously';

    IF( my_is_async IS TRUE ) THEN
        NOTIFY new_event_queue_item;
        RETURN NEW;
    END IF;

    EXECUTE 'SELECT ' || COALESCE(
                current_setting( '@extschema@.get_uid_function', TRUE ),
                'NULL'
            ) || '::INTEGER'
      INTO my_uid;

    my_query := regexp_replace( NEW.work_item_query, '\?pk_value\?', NEW.pk_value::VARCHAR, 'g' );
    my_query := regexp_replace( my_query, '\?recorded\?', NEW.recorded::VARCHAR, 'g' );
    my_query := regexp_replace( my_query, '\?uid\?', quote_nullable( NEW.uid::VARCHAR ), 'g' );
    my_query := regexp_replace( my_query, '\?op\?', NEW.op::VARCHAR, 'g' );
    my_query := regexp_replace( my_query, '\?event_table_work_item\?', NEW.event_table_work_item::VARCHAR, 'g' );

    FOR my_parameters IN EXECUTE my_query LOOP
        INSERT INTO @extschema@.tb_work_queue
                    (
                        parameters,
                        uid,
                        recorded,
                        transaction_label,
                        action,
                        execute_asynchronously
                    )
             VALUES
                    (
                        my_parameters,
                        my_uid,
                        NEW.recorded,
                        NEW.transaction_label,
                        NEW.action,
                        my_is_async
                    );
    END LOOP;

    DELETE FROM @extschema@.tb_event_queue
          WHERE event_table_work_item IS NOT DISTINCT FROM NEW.event_table_work_item
            AND uid IS NOT DISTINCT FROM NEW.uid
            AND recorded IS NOT DISTINCT FROM NEW.recorded
            AND pk_value IS NOT DISTINCT FROM NEW.pk_value
            AND op IS NOT DISTINCT FROM NEW.op
            AND execute_asynchronously IS NOT DISTINCT FROM NEW.execute_asynchronously
            AND work_item_query IS NOT DISTINCT FROM NEW.work_item_query
            AND transaction_label IS NOT DISTINCT FROM NEW.transaction_label
            AND action IS NOT DISTINCT FROM NEW.action;

    RETURN NULL;
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
BEGIN
    my_is_async := TRUE;

    SELECT CASE WHEN lower( value ) LIKE '%t%'
                THEN TRUE
                ELSE FALSE
                 END AS is_async
      INTO my_is_async
      FROM @extschema@.tb_setting
     WHERE key = '@extschema@.execute_asynchronously';

    IF( my_is_async IS TRUE ) THEN
        NOTIFY new_work_queue_item;
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

    EXECUTE 'SELECT ' || COALESCE(
                regexp_replace(
                    COALESCE( current_setting( '@extschema@.set_uid_function', TRUE ), 'NULL' ),
                    '\?uid\?',
                    quote_nullable( NEW.uid ),
                    'g'
                ),
                'NULL'
            );

    my_query := regexp_replace( my_query, '\?recorded\?', NEW.recorded::VARCHAR, 'g' );
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
    END IF;

    DELETE FROM @extschema@.tb_work_queue
          WHERE parameters::VARCHAR IS NOT DISTINCT FROM NEW.parameters::VARCHAR
            AND action IS NOT DISTINCT FROM NEW.action
            AND uid IS NOT DISTINCT FROM NEW.uid
            AND recorded IS NOT DISTINCT FROM NEW.recorded
            AND transaction_label IS NOT DISTINCT FROM NEW.transaction_label
            AND execute_asynchronously IS NOT DISTINCT FROM NEW.execute_asynchronously;
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

    RETURN NEW;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_validate_when_function
    BEFORE INSERT OR UPDATE OF when_function ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_validate_function();

