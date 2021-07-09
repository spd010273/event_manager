/*
 *    Uses the advisory locks to determine if workers are running
 */
    SELECT l.objid AS worker_pid,
           CASE WHEN l.classid = 'event_manager.tb_work_queue'::REGCLASS::OID::BIGINT
                THEN 'work processor'
                WHEN l.classid = 'event_manager.tb_event_queue'::REGCLASS::OID::BIGINT
                THEN 'event processor'
                WHEN l.classid = 'event_manager.tb_setting'::REGCLASS::OID::BIGINT
                THEN 'config manager'
                ELSE NULL
                 END AS worker_type,
           CASE WHEN a.client_addr IS NULL
                THEN '127.0.0.1'::INET
                ELSE a.client_addr
                 END AS client_address,
           CASE WHEN a.client_addr IS NULL AND a.client_hostname IS NULL
                THEN 'localhost'
                WHEN a.client_addr IS NOT NULL AND a.client_hostname IS NULL
                THEN '<unknown>'
                ELSE a.client_hostname
                 END AS client_hostname,
           a.client_port,
           a.pid AS backend_pid
      FROM pg_locks l
INNER JOIN pg_stat_activity a
        ON a.pid = l.pid
     WHERE l.locktype = 'advisory'
       AND l.classid IN(
               'event_manager.tb_work_queue'::REGCLASS::OID::BIGINT,
               'event_manager.tb_event_queue'::REGCLASS::OID::BIGINT,
               'event_manager.tb_setting'::REGCLASS::OID::BIGINT
           );

