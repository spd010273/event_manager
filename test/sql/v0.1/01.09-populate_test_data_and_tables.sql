CREATE SCHEMA eventmanagertest;
CREATE SEQUENCE eventmanagertest.sq_a;
CREATE TABLE eventmanagertest.tb_a
(
    a   INTEGER PRIMARY KEY DEFAULT nextval('eventmanagertest.sq_a'),
    foo VARCHAR,
    bar VARCHAR
);

INSERT INTO eventmanagertest.tb_a( foo, bar )
     SELECT md5( generate_series( 1, 1000 )::VARCHAR ) AS foo,
            reverse( md5( generate_series( 1, 1000 )::VARCHAR ) ) AS bar;

CREATE SEQUENCE eventmanagertest.sq_b;
CREATE TABLE eventmanagertest.tb_b
(
    b INTEGER PRIMARY KEY DEFAULT nextval('eventmanagertest.sq_b'),
    foo VARCHAR,
    bar VARCHAR,
    a INTEGER REFERENCES eventmanagertest.tb_a
);

INSERT INTO eventmanagertest.tb_b
            (
                foo,
                bar,
                a
            )
     SELECT reverse( a.foo ) AS foo,
            NULL AS bar,
            a.a
       FROM eventmanagertest.tb_a a;

-- bare minimum for subsequent runs to pass
INSERT INTO event_manager.tb_setting
            (
                key,
                value
            )
     VALUES ( 'event_manager.execute_asynchronously', 'true' ),
            ( 'event_manager.default_when_function', 'event_manager.fn_dummy_when_function' ),
            ( 'event_manager.session_gucs', 'event_manager.base_url' );
DO
 $_$
DECLARE
    my_count    INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO my_count
      FROM eventmanagertest.tb_a;

    IF( my_count != 1000 ) THEN
        RAISE EXCEPTION 'FAILED: Populate data (tb_a)';
        RETURN;
    END IF;

    SELECT COUNT(*)
      INTO my_count
      FROM eventmanagertest.tb_b;

    IF( my_count != 1000 ) THEN
        RAISE EXCEPTION 'FAILED: Populate data (tb_b)';
        RETURN;
    END IF;


    SELECT COUNT(*)
      INTO my_count
      FROM event_manager.tb_setting;

    IF( my_count = 0 ) THEN
        RAISE EXCEPTION 'FAILED: populate event_manager.tb_setting';
        RETURN;
    END IF;

    RAISE NOTICE 'PASSED: Populate data';
    RETURN;
END
 $_$
    LANGUAGE plpgsql;

