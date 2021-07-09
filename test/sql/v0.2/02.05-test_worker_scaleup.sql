--require: event_manager

DELETE FROM event_manager.tb_setting WHERE key = 'event_manager.override_work_process_count';
DELETE FROM event_manager.tb_setting WHERE key = 'event_manager.override_event_process_count';

CREATE TEMP TABLE tt_new_settings AS
(
    SELECT unnest(
               ARRAY[
                   'event_manager.override_work_process_count',
                   'event_manager.override_event_process_count'
               ]::VARCHAR[]
           ) AS key,
           unnest(
               ARRAY[
                   '5',
                   '5'
               ]::VARCHAR[]
           ) AS value
);

INSERT INTO event_manager.tb_setting( key, value )
     SELECT tt.key,
            tt.value
       FROM tt_new_settings tt
  LEFT JOIN event_manager.tb_setting s
         ON s.key = tt.key
      WHERE s.key IS NULL;

UPDATE event_manager.tb_setting s
   SET value = tt.value
  FROM tt_new_settings tt
 WHERE tt.key = s.key
   AND tt.value != s.value;

DROP TABLE tt_new_settings;

DO
 $_$
DECLARE
    my_count    INTEGER;
BEGIN
    PERFORM *
       FROM event_manager.tb_setting
      WHERE key = 'event_manager.override_work_process_count'
        AND value = '5';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'GUC change for work process override did not stick';
    END IF;

    PERFORM *
       FROM event_manager.tb_setting
      WHERE key = 'event_manager.override_event_process_count'
        AND value = '5';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'GUC change for event process override did not stick';
    END IF;

    RAISE NOTICE 'PASSED: Process override via GUC';

    PERFORM pg_sleep( 20 ); -- approx max loop time for parent maint loop, may need more for VG runs
    SELECT COUNT(*)
      INTO my_count
      FROM pg_locks l
INNER JOIN pg_stat_activity a
        ON a.pid = l.pid
     WHERE l.locktype = 'advisory'
       AND l.classid IN( 'event_manager.tb_work_queue'::REGCLASS::OID::BIGINT );

    IF( my_count != 5 ) THEN
        RAISE EXCEPTION 'FAILED: Incorrect number of work processors: %', my_count;
    END IF;

    SELECT COUNT(*)
      INTO my_count
      FROM pg_locks l
INNER JOIN pg_stat_activity a
        ON a.pid = l.pid
     WHERE l.locktype = 'advisory'
       AND l.classid IN( 'event_manager.tb_event_queue'::REGCLASS::OID::BIGINT );

    IF( my_count != 5 ) THEN
        RAISE EXCEPTION 'FAILED: Incorrect number of event processors: %', my_count;
    END IF;

    RAISE NOTICE 'PASSED: Process scale up';
END
 $_$
    LANGUAGE plpgsql;
