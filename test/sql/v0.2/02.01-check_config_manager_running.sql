--require: event_manager

DO
 $_$
BEGIN
    PERFORM pg_sleep( 5 );

    PERFORM *
       FROM pg_locks l
 INNER JOIN pg_stat_activity a
         ON a.pid = l.pid
 INNER JOIN pg_namespace n
         ON n.nspname = 'event_manager'
 INNER JOIN pg_class c
         ON c.relkind = 'r'
        AND c.relname = 'tb_setting'
        AND c.relnamespace = n.oid
        AND c.oid = l.classid
      WHERE l.locktype = 'advisory'
        AND a.datname = current_database();

    IF NOT FOUND THEN
        RAISE EXCEPTION 'FAILED: Configuration Manager not running';
    END IF;

    RAISE NOTICE 'PASSED: Configration Manager is running';
END
 $_$
    LANGUAGE plpgsql;
