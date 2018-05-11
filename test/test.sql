DROP EXTENSION IF EXISTS event_manager CASCADE;
DROP TABLE IF EXISTS tb_b CASCADE;
DROP TABLE IF EXISTS tb_a CASCADE;
DROP SEQUENCE IF EXISTS sq_a;
DROP SEQUENCE IF EXISTS sq_b;

CREATE EXTENSION IF NOT EXISTS event_manager;
UPDATE event_manager.tb_setting
   SET value = 'true'
 WHERE key = 'event_manager.execute_asynchronously';
CREATE SEQUENCE sq_a;
CREATE TABLE tb_a
(
    a INTEGER PRIMARY KEY DEFAULT nextval('sq_a'),
    foo VARCHAR,
    bar VARCHAR
);

INSERT INTO tb_a( foo, bar )
     SELECT md5( generate_series( 1, 1000 )::VARCHAR ) AS foo,
            reverse( md5( generate_Series( 1, 1000 )::VARCHAR ) ) AS bar;

CREATE SEQUENCE sq_b;
CREATE TABLE tb_b
(
    b INTEGER PRIMARY KEY DEFAULT nextval('sq_b'),
    foo VARCHAR,
    bar VARCHAR,
    a INTEGER REFERENCES tb_a
);

INSERT INTO tb_b
            (
                foo,
                bar,
                a
            )
     SELECT reverse( a.foo ) AS foo,
            NULL AS bar,
            a.a
       FROM tb_a a
   ORDER BY random()
      LIMIT 50;

INSERT INTO event_manager.tb_event_table
            (
                table_name,
                schema_name
            )
     VALUES
            (
                'tb_a',
                'public'
            );

DO
 $_$
BEGIN
    PERFORM *
       FROM pg_trigger t
       JOIN pg_class c
         ON c.oid = t.tgrelid
        AND c.relkind = 'r'
        AND c.relname::VARCHAR = 'tb_a'
       JOIN pg_namespace n
         ON n.nspname::VARCHAR = 'public'
        AND n.oid = c.relnamespace
      WHERE t.tgname::VARCHAR = 'tr_event_enqueue';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Trigger not automatically created on tb_a. Test failed.';
    END IF;

    RETURN;
END
 $_$
    LANGUAGE plpgsql;

INSERT INTO event_manager.tb_action
            (
                query
            )
      VALUES
            (
                'DELETE FROM tb_b b USING tb_a a WHERE a.a = b.a AND b.foo = ?md5?'
            );
INSERT INTO event_manager.tb_event_table_work_item
            (
                source_event_table,
                action,
                work_item_query
            )
     SELECT et.event_table,
            a.action,
            'SELECT ( ''{"md5":"'' || reverse( md5( ( a.a - 1000 )::VARCHAR ) ) || ''"}'' )::JSONB AS parameters FROM tb_a a WHERE a.a = ?pk_value?::INTEGER'
       FROM event_manager.tb_event_table et
 INNER JOIN event_manager.tb_action a
         ON TRUE;

SELECT 'Causing event' AS status;
INSERT INTO tb_a
            (
                foo,
                bar
            )
     VALUES
            (
                'test',
                'test'
            );

SELECT pg_sleep(5);
--will fail if process is not running or failed
SELECT CASE WHEN 50 - ( COUNT(*) ) = 0
            THEN 'Event Queue test failed - is the queue processor running?'
            ELSE 'Event queue test passed.'
             END AS status
  FROM tb_b;

INSERT INTO event_manager.tb_action
            (
                uri,
                static_parameters
            )
     VALUES
            (
                'http://ises.chris.neadwerx.com/api/current/locations',
                '{"number":"121","testparam2":"val2"}'::JSONB
            );

UPDATE event_manager.tb_event_table_work_item
   SET action = a.action
  FROM event_manager.tb_action a
 WHERE a.uri IS NOT NULL;

SELECT 'Causing event' AS status;
INSERT INTO tb_a
            (
                foo,
                bar
            )
     VALUES
            (
                'uritest',
                'uritest'
            );

SELECT pg_sleep(5);
