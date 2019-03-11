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
 *     @extschema@. #TODO
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

CREATE TABLE @extschema@.tb_setting
(
    key     VARCHAR,
    value   VARCHAR,
    CHECK( key ~ '^@extschema@\.' )
);

CREATE UNIQUE INDEX ix_unique_setting_key ON @extschema@.tb_setting( lower( key ) );

CREATE FUNCTION @extschema@.fn_get_config
(
    in_name VARCHAR
)
RETURNS VARCHAR AS
 $_$
    SELECT COALESCE(
               NULLIF( current_setting( in_name, TRUE ), '' ),
               (
                   SELECT set_config( key, value, TRUE )
                     FROM @extschema@.tb_setting
                    WHERE key = in_name
               )
           );
 $_$
LANGUAGE SQL STABLE PARALLEL SAFE;

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

/* Avoid TOC errors on restore by using antijoin when creating defaults */
WITH tt_unnest AS
(
    SELECT unnest(
               ARRAY[
                   '@extschema@.execute_asynchronously',
                   '@extschema@.set_uid_function',
                   '@extschema@.get_uid_function',
                   '@extschema@.default_when_function',
                   '@extschema@.session_gucs',
                   '@extschema@.base_url'
               ]::VARCHAR[]
           ) AS key,
           unnest(
               ARRAY[
                   't',
                   'NULL',
                   'NULL',
                   '@extschema@.fn_dummy_when_function',
                   '',
                   'localhost'
               ]::VARCHAR[]
           ) AS value
)
INSERT INTO @extschema@.tb_setting
            (
                key,
                value
            )
     SELECT tt.key,
            tt.value
       FROM tt_unnest tt
  LEFT JOIN @extschema@.tb_setting s
         ON s.key = tt.key
      WHERE s.value IS NULL;

CREATE SEQUENCE @extschema@.sq_pk_event_table;

CREATE TABLE @extschema@.tb_event_table
(
    event_table INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table'),
    schema_name VARCHAR NOT NULL DEFAULT 'public',
    table_name  VARCHAR(63) NOT NULL UNIQUE
);

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table', '' );
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
    use_ssl             BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK( uri IS NOT NULL OR query IS NOT NULL ),
    CHECK( ( method IS NULL OR method IN( 'PUT', 'POST', 'GET' ) ) )
);

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_action', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_action', '' );
COMMENT ON TABLE @extschema@.tb_action IS 'Stores a list of available end results of a given event';
COMMENT ON COLUMN @extschema@.tb_action.label IS 'User-facing label of the action';
COMMENT ON COLUMN @extschema@.tb_action.query IS 'Allows the developer to specify DML as the action';
COMMENT ON COLUMN @extschema@.tb_action.uri IS 'Allows the developer to specify an API endpoint as the action';
COMMENT ON COLUMN @extschema@.tb_action.method IS 'HTTP method for the above endpoint (PUT,GET,POST)';
COMMENT ON COLUMN @extschema@.tb_action.static_parameters IS 'A list of static parameters for either the query or URI parameter list';
COMMENT ON COLUMN @extschema@.tb_action.use_ssl IS 'Indicates that event_manager should turn SSL on in cURL prior to making an HTTP request';

CREATE SEQUENCE @extschema@.sq_pk_event_table_work_item;
CREATE TABLE @extschema@.tb_event_table_work_item
(
    event_table_work_item   INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table_work_item'),
    source_event_table      INTEGER NOT NULL REFERENCES @extschema@.tb_event_table,
    source_column_name      VARCHAR(63),
    target_event_table      INTEGER REFERENCES @extschema@.tb_event_table,
    action                  INTEGER NOT NULL REFERENCES @extschema@.tb_action,
    label                   JSONB,
    description             JSONB,
    transaction_label       VARCHAR,
    work_item_query         TEXT NOT NULL,
    when_function           VARCHAR DEFAULT @extschema@.fn_get_config( '@extschema@.default_when_function' ),
    op                      CHAR(1)[],
    execute_asynchronously  BOOLEAN DEFAULT COALESCE( @extschema@.fn_get_config( '@extschema@.execute_asynchronously' )::BOOLEAN, TRUE ),
    inverse_event           INTEGER,
    CHECK( ( op <@ ARRAY[ 'I','U','D' ]::CHAR(1)[] ) )
);

/*
CREATE UNIQUE INDEX ix_source_action_target_unique ON @extschema@.tb_event_table_work_item(
    COALESCE( target_event_table, -1 ),
    source_event_table,
    COALESCE( source_column_name, '' ),
    action
);
*/

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table_work_item', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table_work_item', '' );
COMMENT ON TABLE @extschema@.tb_event_table_work_item IS 'A list of actions that should occur for any given event table';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.source_event_table IS 'Indicates the table that can trigger this work item';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.target_event_table IS 'Indicates the target of this work items action. Not necessary but useful for any user interface built around this';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.source_column_name IS 'Indicated the column of the source table this event if firing on. This is not used for selectively firing update triggers but to help prevent the user from implementing identical/similar events. NOTE: This column can be comma delimited';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.action IS 'Foreign key to tb_action - indicates what this work item generates parameters for';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.label IS 'User-facing label for this work item';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.description IS 'User-facing description for that this work item is / does';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.transaction_label IS 'Label for what the action is performing. Used in Cyanaudit integration';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.work_item_query IS 'Generates a list of parameters for the action. This query has named bind point for the columns in this table. Query should generate JSONB aliased as parameters';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.when_function IS 'Filters events entering tb_event_queue. Example prototype is fn_dummy_when_function. Function should return BOOLEAN';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.op IS 'Indicates what DML operation this work item applies: U - Update, I - Insert, D - Delete.';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.execute_asynchronously IS 'Determines what mode of execution this work item will be ran under.';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.inverse_event IS 'Indicates that this event has an inverse event buy linking to it';

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

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_queue', '' );
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
    ADD COLUMN execute_asynchronously  BOOLEAN DEFAULT @extschema@.fn_get_config( '@extschema@.execute_asynchronously' )::BOOLEAN,
    ADD COLUMN session_values JSONB;

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_work_queue', '' );
COMMENT ON TABLE @extschema@.tb_work_queue IS 'Queue for work_item_query results. Remaining contents copied from the corresponding event_queue entry';
COMMENT ON COLUMN @extschema@.tb_work_queue.parameters IS 'Parameters returned by work_item_query';
COMMENT ON COLUMN @extschema@.tb_work_queue.action IS 'Action that will be executed';
COMMENT ON COLUMN @extschema@.tb_work_queue.uid IS 'uid from event_queue';
COMMENT ON COLUMN @extschema@.tb_work_queue.recorded IS 'Recorded timestamp from event_queue';
COMMENT ON COLUMN @extschema@.tb_work_queue.transaction_label IS 'Label for transaction in Cyanaudit, if installed';
COMMENT ON COLUMN @extschema@.tb_work_queue.execute_asynchronously IS 'Indicates how this action should be executed';
COMMENT ON COLUMN @extschema@.tb_work_queue.session_values IS 'Copy of the session values from the event queue';

CREATE SEQUENCE @extschema@.sq_pk_event_table_work_item_instance;
CREATE TABLE @extschema@.tb_event_table_work_item_instance
(
    event_table_work_item_instance INTEGER PRIMARY KEY DEFAULT nextval('@extschema@.sq_pk_event_table_work_item_instance'),
    event_table_work_item   INTEGER NOT NULL REFERENCES @extschema@.tb_event_table_work_item,
    source_pk               INTEGER,
    target_pk               INTEGER,
    metadata                JSONB,
    CHECK( source_pk IS NOT NULL OR target_pk IS NOT NULL )
);

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table_work_item_instance', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table_work_item_instance', '' );
COMMENT ON TABLE @extschema@.tb_event_table_work_item_instance IS 'Can be used to aid work item queries or action queries when the scope of an action query is non-deterministic or too broad. This table is intended for use by any integrating application, but is not required';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item_instance.metadata IS 'Can be used to store static parameters for an instance of a event_table_work_item';

CREATE INDEX ix_event_table_work_item_instance_src ON @extschema@.tb_event_table_work_item_instance( event_table_work_item, source_pk );
CREATE INDEX ix_event_table_work_item_instance_tgt ON @extschema@.tb_event_table_work_item_instance( event_table_work_item, target_pk );

CREATE TABLE @extschema@.tb_statistic
(
    tx_success      BIGINT,
    tx_fail         BIGINT,
    tx_duration     DOUBLE PRECISION,
    type            VARCHAR,
    recorded        TIMESTAMP NOT NULL DEFAULT now()
);

COMMENT ON TABLE @extschema@.tb_statistic IS 'Contains basic worker statistics';
COMMENT ON COLUMN @extschema@.tb_statistic.tx_success IS 'The number of successful transactions committed by this type of worker';
COMMENT ON COLUMN @extschema@.tb_statistic.tx_fail IS 'The number of failed transactions by this type of worker';
COMMENT ON COLUMN @extschema@.tb_statistic.tx_duration IS 'The cumulative duration, in seconds, of all transactions during the reporting window for this worker';
COMMENT ON COLUMN @extschema@.tb_statistic.recorded IS 'The timestamp which these statistics were recorded';

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
    PERFORM pid
       FROM pg_catalog.pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'pg_restore';

    IF FOUND THEN
        RETURN NEW;
    END IF;

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
    PERFORM pid
       FROM pg_catalog.pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'pg_restore';

    IF FOUND THEN
        RETURN NEW;
    END IF;

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

CREATE FUNCTION @extschema@.fn_manage_triggers()
RETURNS VOID AS
 $_$
DECLARE
    my_statement    VARCHAR;
BEGIN
    PERFORM pid
       FROM pg_catalog.pg_stat_activity
      WHERE datname = current_database()
        AND application_name = 'pg_restore';

    IF FOUND THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tt_desired_triggers AS
    (
        WITH tt_op_expansion AS
        (
            SELECT source_event_table,
                   unnest( regexp_split_to_array( source_column_name, ',' ) ) AS source_column_name,
                   unnest( op ) AS op
              FROM @extschema@.tb_event_table_work_item
        ),
        tt_aggregate AS
        (
            SELECT n.nspname::VARCHAR AS schema_name,
                   c.relname::VARCHAR AS table_name,
                   array_agg( DISTINCT a.attname::VARCHAR ) AS column_name,
                   a_pk.attname::VARCHAR AS primary_key,
                   array_agg( DISTINCT tt.op ) AS op
              FROM @extschema@.tb_event_table et
        INNER JOIN tt_op_expansion tt
                ON tt.source_event_table = et.event_table
        INNER JOIN pg_catalog.pg_class c
                ON c.relname::VARCHAR = et.table_name
        INNER JOIN pg_catalog.pg_namespace n
                ON n.oid = c.relnamespace
               AND n.nspname::VARCHAR = et.schema_name
         LEFT JOIN pg_catalog.pg_attribute a
                ON a.attrelid = c.oid
               AND a.attnum > 0
               AND a.attname::VARCHAR = tt.source_column_name
        INNER JOIN pg_catalog.pg_attribute a_pk
                ON a_pk.attrelid = c.oid
               AND a_pk.attnum > 0
        INNER JOIN pg_catalog.pg_constraint cn
                ON cn.conrelid = c.oid
               AND cn.contype = 'p'
               AND cn.conkey[1] = a_pk.attnum
          GROUP BY n.nspname,
                   c.relname,
                   a_pk.attname
        )
            SELECT schema_name,
                   table_name,
                   ( SELECT array_agg( col ) FROM unnest( column_name ) col WHERE col IS NOT NULL ) AS column_name,
                   primary_key,
                   CASE WHEN 'I' = ANY( op ) THEN TRUE
                        ELSE FALSE
                         END AS i,
                   CASE WHEN 'U' = ANY( op ) THEN TRUE
                        ELSE FALSE
                         END AS u,
                   CASE WHEN 'D' = ANY( op ) THEN TRUE
                        ELSE FALSE
                         END AS d
              FROM tt_aggregate
    );

    CREATE TEMP TABLE tt_existing_triggers AS
    (
        WITH tt_triggers AS
        (
            SELECT nc.nspname::VARCHAR AS schema_name,
                   c.relname::VARCHAR AS table_name,
                   array_agg( DISTINCT a.attname::VARCHAR ) AS column_name,
                   a_pk.attname::VARCHAR AS primary_key,
                   CASE WHEN ( ( t.tgtype::int4::bit(16) ) << 13 )::bit = 1::BIT THEN TRUE
                        ELSE FALSE
                         END AS i,
                   CASE WHEN ( ( t.tgtype::int4::bit(16) ) << 12 )::bit = 1::BIT THEN TRUE
                        ELSE FALSE
                         END AS u,
                   CASE WHEN ( ( t.tgtype::int4::bit(16) ) << 11 )::bit = 1::bit THEN TRUE
                        ELSE FALSE
                         END AS d
              FROM pg_catalog.pg_trigger t
        INNER JOIN pg_catalog.pg_proc p
                ON p.oid = t.tgfoid
               AND p.proname::VARCHAR = 'fn_enqueue_event'
        INNER JOIN pg_catalog.pg_namespace n
                ON n.oid = p.pronamespace
               AND n.nspname::VARCHAR = '@extschema@'
        INNER JOIN pg_catalog.pg_class c
                ON c.oid = t.tgrelid
         LEFT JOIN pg_catalog.pg_attribute a
                ON a.attrelid = c.oid
               AND a.attnum > 0
               AND a.attnum = ANY( t.tgattr )
        INNER JOIN pg_catalog.pg_attribute a_pk
                ON a_pk.attnum > 0
               AND a_pk.attrelid = c.oid
        INNER JOIN pg_catalog.pg_constraint cn
                ON cn.conrelid = c.oid
               AND cn.contype = 'p'
               AND cn.conkey[1] = a_pk.attnum
        INNER JOIN pg_catalog.pg_namespace nc
                ON nc.oid = c.relnamespace
             WHERE t.tgname::VARCHAR = 'tr_event_enqueue'
          GROUP BY nc.nspname,
                   c.relname,
                   a_pk.attname,
                   t.tgtype
        )
            SELECT schema_name,
                   table_name,
                   ( SELECT array_agg( col ) FROM unnest( column_name ) col WHERE col IS NOT NULL ) AS column_name,
                   primary_key,
                   i,
                   u,
                   d
              FROM tt_triggers
    );

    FOR my_statement IN(
                SELECT 'DROP TRIGGER tr_event_enqueue ON ' || COALESCE( tte.schema_name || '.', '' ) || tte.table_name
                  FROM tt_existing_triggers tte
             LEFT JOIN tt_desired_triggers ttd
                    ON ttd.table_name = tte.table_name
                   AND ttd.schema_name = tte.schema_name
                 WHERE ttd.primary_key IS NULL
                    OR (
                            ttd.primary_key IS DISTINCT FROM tte.primary_key
                         OR ttd.column_name IS DISTINCT FROM tte.column_name
                         OR ttd.i IS DISTINCT FROM tte.i
                         OR ttd.u IS DISTINCT FROM tte.u
                         OR ttd.d IS DISTINCT FROM tte.d
                       )
                       ) LOOP
        EXECUTE my_statement;
    END LOOP;

    DELETE FROM tt_existing_triggers tte
          USING tt_desired_triggers ttd
          WHERE tte.schema_name = ttd.schema_name
            AND tte.table_name = ttd.table_name
            AND (
                    tte.primary_key IS DISTINCT FROM ttd.primary_key
                 OR tte.column_name IS DISTINCT FROM ttd.column_name
                 OR tte.i IS DISTINCT FROM ttd.i
                 OR tte.u IS DISTINCT FROM ttd.u
                 OR tte.d IS DISTINCT FROM ttd.d
                );

    FOR my_statement IN(
                SELECT FORMAT(
                           'CREATE TRIGGER tr_event_enqueue AFTER '
                        || array_to_string(
                               ARRAY[
                                   CASE WHEN ttd.i IS TRUE THEN 'INSERT' ELSE NULL END,
                                   CASE WHEN ttd.u IS TRUE THEN
                                                           CASE WHEN array_length( ttd.column_name, 1 ) >= 1
                                                                THEN 'UPDATE OF ' || array_to_string( ttd.column_name, ', ' )
                                                                ELSE 'UPDATE'
                                                                 END
                                                           ELSE NULL
                                                            END,
                                   CASE WHEN ttd.d IS TRUE THEN 'DELETE' ELSE NULL END
                               ]::VARCHAR[],
                               ' OR '
                           )
                        || ' ON ' || COALESCE( ttd.schema_name || '.', '' ) || ttd.table_name
                        || ' FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_enqueue_event( %L )',
                           ttd.primary_key
                        ) AS tgdef
                   FROM tt_desired_triggers ttd
              LEFT JOIN tt_existing_triggers tte
                     ON tte.schema_name = ttd.schema_name
                    AND tte.table_name = ttd.table_name
                  WHERE tte.primary_key IS NULL
                       ) LOOP
        EXECUTE my_statement;
    END LOOP;

    DROP TABLE tt_desired_triggers;
    DROP TABLE tt_existing_triggers;
    
    RETURN;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE FUNCTION @extschema@.fn_manage_trigger_wrapper()
RETURNS TRIGGER AS
 $_$
BEGIN
    PERFORM @extschema@.fn_manage_triggers();

    IF( TG_OP = 'DELETE' ) THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END
 $_$
    LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE;

CREATE TRIGGER tr_new_enqueue_trigger
    AFTER INSERT OR UPDATE OR DELETE ON @extschema@.tb_event_table
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_manage_trigger_wrapper();

CREATE TRIGGER tr_new_enqueue_trigger
    AFTER INSERT OR UPDATE OF source_event_table, source_column_name OR DELETE ON @extschema@.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE @extschema@.fn_manage_trigger_wrapper();

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

GRANT ALL ON @extschema@.tb_event_queue TO public;
GRANT ALL ON @extschema@.tb_work_queue TO public;
GRANT SELECT ON @extschema@.tb_event_table_work_item TO public;
GRANT SELECT ON @extschema@.tb_action TO public;
GRANT SELECT ON @extschema@.tb_setting TO public;
GRANT SELECT ON @extschema@.tb_event_table TO public;
GRANT ALL ON @extschema@.tb_event_table_work_item_instance TO public;
GRANT ALL ON @extschema@.tb_statistic TO public;
GRANT USAGE, SELECT ON SEQUENCE @extschema@.sq_pk_event_table_work_item_instance TO public;
GRANT USAGE ON SCHEMA @extschema@ TO public;
