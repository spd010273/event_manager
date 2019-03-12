/*
 *    Uses the advisory locks to determine if workers are running
 */
SELECT objid AS pid,
       CASE WHEN classid = 'event_manager.tb_work_queue'::REGCLASS::OID::BIGINT
            THEN 'work processor'
            WHEN classid = 'event_manager.tb_event_queue'::REGCLASS::OID::BIGINT
            THEN 'event processor'
            ELSE NULL
             END AS worker_type
  FROM pg_locks
 WHERE locktype = 'advisory'
   AND classid IN(
           'event_manager.tb_work_queue'::REGCLASS::OID::BIGINT,
           'event_manager.tb_event_queue'::REGCLASS::OID::BIGINT
       );

