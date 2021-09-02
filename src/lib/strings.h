/*------------------------------------------------------------------------
 *
 * strings.h
 *     Static string declarations (Queries n' such)
 *
 * Copyright (c) 2018-2021 MerchLogix Inc
 *
 * IDENTIFICATION
 *        src/lib/strings.h
 *
 *------------------------------------------------------------------------
 */

#ifndef STRINGS_H
#define STRINGS_H
#define EXTENSION_NAME "event_manager"
#define MAX_DEQUEUE "25"
#define MAX_RANGE "'0.5 seconds'::INTERVAL"

/* Static Strings */
static const char * user_agent = "\
EventManagerbot/0.1 (+https://bitbucket.org/neadwerx/event_manager/src/master/)";

/* Queries */
static const char * extension_check_query = "\
    SELECT n.nspname AS ext_schema \
      FROM pg_catalog.pg_extension e \
INNER JOIN pg_catalog.pg_namespace n \
        ON n.oid = e.extnamespace \
     WHERE e.extname = $1 \
       AND e.extversion = $2";

/*
 * This and the work queue bulk dequeue work similarly -
 * Perform a single dequeue, then append to that set other items of the same nature
 * that occurred (temporally) within a small window of time, up to a limit.
 */
static const char * get_bulk_event_queue_item __attribute__((unused)) = "\
WITH ct_bulk_dequeue AS \
( \
    WITH ct_peek AS \
    ( \
        SELECT eq.*, \
               eq.ctid \
          FROM " EXTENSION_NAME ".tb_event_queue eq \
         WHERE eq.failed IS FALSE \
           AND eq.execute_asynchronously IS TRUE \
      ORDER BY eq.failed ASC, \
               eq.recorded ASC \
         LIMIT 1 \
    FOR UPDATE OF eq SKIP LOCKED \
    ), \
    ct_batched_lock AS \
    ( \
        SELECT eq.*, \
               eq.ctid \
          FROM " EXTENSION_NAME ".tb_event_queue eq \
    INNER JOIN ct_peek tt \
            ON tt.event_table_work_item = eq.event_table_work_item \
         WHERE eq.failed IS FALSE \
           AND eq.execute_asynchronously IS TRUE \
           AND eq.recorded >= tt.recorded \
           AND eq.recorded < ( tt.recorded + COALESCE( $1::INTERVAL, " MAX_RANGE " ) ) \
    FOR UPDATE OF eq SKIP LOCKED \
         LIMIT COALESCE( $2::INTEGER - 1, " MAX_DEQUEUE " - 1 ) \
    ) \
        SELECT p.* \
          FROM ct_peek p \
         UNION ALL \
        SELECT bl.* \
          FROM ct_batched_lock bl \
) \
    SELECT eq.event_table_work_item, \
           eq.uid, \
           eq.recorded, \
           eq.pk_value, \
           eq.op, \
           etwi.action, \
           etwi.transaction_label, \
           etwi.work_item_query, \
           etwi.execute_asynchronously, \
           etwi.can_deduplicate, \
           eq.old, \
           eq.new, \
           eq.session_values, \
           eq.ctid \
      FROM ct_bulk_dequeue eq \
INNER JOIN " EXTENSION_NAME ".tb_event_table_work_item etwi \
        ON etwi.event_table_work_item = eq.event_table_work_item";

static const char * get_event_queue_item = "\
WITH ct_lock AS \
( \
    SELECT eq.*, \
           eq.ctid \
      FROM " EXTENSION_NAME ".tb_event_queue eq \
     WHERE NOT eq.failed \
  ORDER BY eq.failed ASC, \
           eq.recorded ASC \
     LIMIT 1 \
FOR UPDATE OF eq SKIP LOCKED \
) \
    SELECT eq.event_table_work_item, \
           eq.uid, \
           eq.recorded, \
           eq.pk_value, \
           eq.op, \
           etwi.action, \
           etwi.transaction_label, \
           etwi.work_item_query, \
           etwi.execute_asynchronously, \
           etwi.can_deduplicate, \
           etwi.can_bulk_execute, \
           eq.old, \
           eq.new, \
           eq.session_values, \
           eq.ctid \
      FROM ct_lock eq \
INNER JOIN " EXTENSION_NAME ".tb_event_table_work_item etwi \
        ON etwi.event_table_work_item = eq.event_table_work_item";

static const char * delete_event_queue_item = "\
DELETE FROM " EXTENSION_NAME ".tb_event_queue eq \
      WHERE eq.event_table_work_item = $1::INTEGER \
        AND eq.uid IS NOT DISTINCT FROM $2::INTEGER \
        AND eq.recorded = $3::TIMESTAMP \
        AND eq.pk_value = $4::INTEGER \
        AND eq.op = $5::CHAR(1) \
        AND eq.old::TEXT IS NOT DISTINCT FROM $6::TEXT \
        AND eq.new::TEXT IS NOT DISTINCT FROM $7::TEXT \
        AND eq.session_values::TEXT IS NOT DISTINCT FROM $8::TEXT \
        AND eq.ctid = $9::TID";

static const char * update_event_queue_item_failed = "\
UPDATE " EXTENSION_NAME ".tb_event_queue eq \
   SET failed = TRUE \
 WHERE eq.event_table_work_item = $1::INTEGER \
   AND eq.uid IS NOT DISTINCT FROM $2::INTEGER \
   AND eq.recorded = $3::TIMESTAMP \
   AND eq.pk_value = $4::INTEGER \
   AND eq.op = $5::CHAR(1) \
   AND eq.old::TEXT IS NOT DISTINCT FROM $6::TEXT \
   AND eq.new::TEXT IS NOT DISTINCT FROM $7::TEXT \
   AND eq.session_values::TEXT IS NOT DISTINCT FROM $8::TEXT \
   AND eq.ctid = $9::TID";

static const char * get_bulk_work_queue_item __attribute__((unused)) = "\
WITH ct_bulk_dequeue AS \
( \
    WITH ct_peek AS \
    ( \
        SELECT wq.*, \
               wq.ctid \
          FROM " EXTENSION_NAME ".tb_work_queue wq \
         WHERE NOT wq.failed \
           AND wq.execute_asynchronously IS TRUE \
      ORDER BY wq.failed ASC, \
               wq.recorded ASC \
    FOR UPDATE OF wq SKIP LOCKED \
         LIMIT 1 \
    ), \
    ct_batched_lock AS \
    ( \
        SELECT wq.*, \
               wq.ctid \
          FROM " EXTENSION_NAME ".tb_work_queue wq \
    INNER JOIN ct_peek tt \
            ON tt.action = wq.action \
         WHERE NOT wq.failed \
           AND wq.recorded >= tt.recorded \
           AND wq.recorded < ( tt.recorded + COALESCE( $1, " MAX_RANGE " ) ) \
    FOR UPDATE OF wq SKIP LOCKED \
         LIMIT COALESCE( $2 - 1, " MAX_DEQUEUE " - 1 ) \
    ) \
        SELECT p.* \
          FROM ct_peek p \
         UNION ALL \
        SELECT bl.* \
          FROM ct_batched_lock \
) \
    SELECT wq.parameters, \
           a.static_parameters, \
           regexp_replace( \
               a.uri, \
               '__BASE_URL__', \
               COALESCE( \
                   wq.session_values->>'" EXTENSION_NAME ".base_url', \
                   current_setting( '" EXTENSION_NAME ".base_url', TRUE ), \
                   'localhost' \
               ) \
           ) AS uri, \
           COALESCE( a.method, 'GET' ) AS method, \
           a.query, \
           a.use_ssl, \
           a.can_deduplicate, \
           wq.uid, \
           wq.recorded, \
           wq.transaction_label, \
           wq.action, \
           wq.session_values, \
           wq.ctid \
      FROM ct_bulk_dequeue wq \
INNER JOIN " EXTENSION_NAME ".tb_action a \
        ON a.action = wq.action";

static const char * get_work_queue_item = "\
    SELECT wq.parameters, \
           a.static_parameters, \
           regexp_replace( \
                a.uri, \
                '__BASE_URL__', \
                COALESCE( \
                    wq.session_values->>'" EXTENSION_NAME ".base_url', \
                    current_setting( '" EXTENSION_NAME ".base_url', TRUE ), \
                    'localhost' \
                ) \
           ) AS uri, \
           COALESCE( a.method, 'GET' ) AS method, \
           a.query, \
           a.use_ssl, \
           a.can_deduplicate, \
           a.can_bulk_execute, \
           wq.uid, \
           wq.recorded, \
           wq.transaction_label, \
           wq.action, \
           wq.session_values, \
           wq.ctid \
      FROM " EXTENSION_NAME ".tb_work_queue wq \
INNER JOIN " EXTENSION_NAME ".tb_action a \
        ON a.action = wq.action \
     WHERE wq.failed IS FALSE \
  ORDER BY wq.failed ASC, \
           wq.recorded ASC  \
     LIMIT 1 \
       FOR UPDATE OF wq SKIP LOCKED";

static const char * delete_work_queue_item = "\
DELETE FROM " EXTENSION_NAME ".tb_work_queue \
      WHERE parameters::TEXT IS NOT DISTINCT FROM $1::JSONB::TEXT \
        AND uid IS NOT DISTINCT FROM $2::INTEGER \
        AND recorded = $3::TIMESTAMP \
        AND transaction_label IS NOT DISTINCT FROM $4::VARCHAR \
        AND action = $5::INTEGER \
        AND session_values::TEXT IS NOT DISTINCT FROM $6::TEXT \
        AND ctid = $7::TID";

static const char * update_work_queue_item_failed = "\
UPDATE " EXTENSION_NAME ".tb_work_queue wq \
   SET failed = TRUE \
 WHERE wq.parameters::TEXT IS NOT DISTINCT FROM $1::JSONB::TEXT \
   AND wq.uid IS NOT DISTINCT FROM $2::INTEGER \
   AND wq.recorded = $3::TIMESTAMP \
   AND wq.transaction_label IS NOT DISTINCT FROM $4::VARCHAR \
   AND wq.action = $5::INTEGER \
   AND wq.session_values::TEXT IS NOT DISTINCT FROM $6::TEXT \
   AND wq.ctid = $7::TID";

static const char * new_work_item_query = "\
INSERT INTO " EXTENSION_NAME ".tb_work_queue \
            ( \
                parameters, \
                uid, \
                recorded, \
                transaction_label, \
                action, \
                execute_asynchronously, \
                session_values \
            ) \
     VALUES \
            ( \
                $1::JSONB, \
                NULLIF( $2, '' )::INTEGER, \
                COALESCE( NULLIF( $3, '' )::TIMESTAMP, clock_timestamp() ), \
                NULLIF( $4, '' )::VARCHAR, \
                NULLIF( $5, '' )::INTEGER, \
                NULLIF( $6, '' )::BOOLEAN, \
                NULLIF( $7::TEXT, '' )::JSONB \
            )";

static const char * _uid_function = "\
    SELECT current_setting( \
               '" EXTENSION_NAME ".' || $1::VARCHAR, \
               TRUE \
           ) AS uid_function";

static const char * cyanaudit_check = "\
    SELECT p.proname::VARCHAR \
      FROM pg_proc p \
INNER JOIN pg_namespace n \
        ON n.oid = p.pronamespace \
       AND n.nspname::VARCHAR = 'cyanaudit' \
     WHERE p.proname = 'fn_label_transaction'";

static const char * cyanaudit_label_tx = "\
    SELECT cyanaudit.fn_label_last_transaction( $1 )";

static const char * set_guc = "\
    SELECT set_config( $1, $2, TRUE )";

static const char * clear_guc = "\
    SELECT set_config( $1, NULL, TRUE )";

static const char * set_application_name = "\
    SET application_name = '";

static const char * test_stat_table = "\
    SELECT c.relname \
      FROM pg_class c \
INNER JOIN pg_namespace n \
        ON n.nspname = '" EXTENSION_NAME "' \
       AND n.oid = c.relnamespace \
     WHERE c.relkind = 'r' \
       AND c.relname = 'tb_statistic'";

static const char * insert_stat_rollup = "\
    INSERT INTO " EXTENSION_NAME ".tb_statistic \
                ( \
                    tx_success, \
                    tx_fail, \
                    tx_duration, \
                    type \
                ) \
         VALUES \
                ( \
                    $1::BIGINT, \
                    $2::BIGINT, \
                    $3::DOUBLE PRECISION, \
                    $4::VARCHAR \
                )";

static const char * pid_lock = "\
    SELECT pg_try_advisory_lock( \
               $1::REGCLASS::OID::INTEGER, \
               $2::INTEGER \
           ) AS result";

// Gated later with version flag for v0.2, attribute is just for suppression
// of unused variable with -Weverything or -Wunused-variable
static const char * check_event_queue_guc __attribute__((unused)) = "\
    SELECT " EXTENSION_NAME ".fn_get_boolean_guc_value( \
               '" EXTENSION_NAME ".disable_event_queue' \
           ) IS TRUE AS value";

static const char * check_work_queue_guc __attribute__((unused)) = "\
    SELECT " EXTENSION_NAME ".fn_get_boolean_guc_value( \
               '" EXTENSION_NAME ".disable_work_queue' \
           ) IS TRUE AS value";

static const char * get_work_processor_count __attribute__((unused)) = "\
    SELECT NULLIF( current_setting( \
               '" EXTENSION_NAME ".override_work_process_count', \
               TRUE \
           ), '' )::INTEGER AS work_count";

static const char * get_event_processor_count __attribute__((unused)) = "\
    SELECT NULLIF( current_setting( \
               '" EXTENSION_NAME ".override_event_process_count', \
               TRUE \
           ), '' )::INTEGER AS event_count";

static const char * get_event_table_work_item_cache __attribute__((unused)) = "\
    SELECT event_table_work_item, \
           action, \
           transaction_label, \
           can_deduplicate, \
           work_item_query, \
           execute_asynchronously \
      FROM " EXTENSION_NAME ".tb_event_table_work_item \
  ORDER BY event_table_work_item";

static const char * get_action_cache __attribute__((unused)) = "\
    SELECT action, \
           query, \
           use_ssl, \
           method, \
           can_deduplicate, \
           can_bulk_execute, \
      FROM " EXTENSION_NAME ".tb_action \
  ORDER BY action";

#endif // STRINGS_H
