/*------------------------------------------------------------------------
 *
 * strings.h
 *     Static string declarations (Queries n' such)
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        src/lib/strings.h
 *
 *------------------------------------------------------------------------
 */

#ifndef STRINGS_H
#define STRINGS_H
#define EXTENSION_NAME "event_manager"

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
WITH tt_uid_function_lookup AS \
( \
    SELECT current_setting( \
               '" EXTENSION_NAME ".' || $1::VARCHAR, \
               TRUE \
           ) AS uid_function, \
           1 AS rank_order \
     UNION ALL \
    SELECT value AS uid_function, \
           2 AS rank_order \
      FROM " EXTENSION_NAME ".tb_setting \
     WHERE key = '" EXTENSION_NAME ".' || $2 \
) \
    SELECT uid_function \
      FROM tt_uid_function_lookup \
     WHERE uid_function IS NOT NULL \
  ORDER BY rank_order ASC \
     LIMIT 1";

static const char * cyanaudit_check = "\
    SELECT p.proname::VARCHAR \
      FROM pg_proc p \
INNER JOIN pg_namespace n \
        ON n.oid = p.pronamespace \
       AND n.nspname::VARCHAR = 'cyanaudit' \
     WHERE p.proname = 'fn_label_transaction'";

static const char * cyanaudit_label_tx = "\
    SELECT cyanaudit.fn_label_last_transaction( $1 )";

// Prevent malicious user from changing a server GUC
static const char * set_guc = "\
WITH tt_guc_check AS \
( \
  SELECT $1 AS name \
) \
    SELECT set_config( tt.name, $2, TRUE ) \
      FROM tt_guc_check tt \
 LEFT JOIN pg_catalog.pg_settings s \
        ON s.name = tt.name \
     WHERE s.setting IS NULL";

static const char * clear_guc = "\
WITH tt_guc_check AS \
( \
    SELECT $1 as name \
) \
    SELECT set_config( tt.name, NULL, TRUE ) \
      FROM tt_guc_check tt \
 LEFT JOIN pg_catalog.pg_settings s \
        ON s.name = tt.name \
     WHERE s.setting IS NULL";

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

static const char * get_event_manager_running = "\
    SELECT pg_try_advisory_lock( \
               '" EXTENSION_NAME ".tb_event_table_work_item'::REGCLASS::OID::BIGINT \
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
#endif // STRINGS_H
