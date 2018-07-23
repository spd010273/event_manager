/*------------------------------------------------------------------------
 *
 * strings.h
 *     Static string declarations (Queries n' such)
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        strings.h
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
     WHERE e.extname = $1";

static const char * get_event_queue_item = "\
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
      FROM " EXTENSION_NAME ".tb_event_queue eq \
INNER JOIN " EXTENSION_NAME ".tb_event_table_work_item etwi \
        ON etwi.event_table_work_item = eq.event_table_work_item \
  ORDER BY eq.recorded DESC \
     LIMIT 1 \
       FOR UPDATE OF eq SKIP LOCKED";

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
  ORDER BY wq.recorded DESC  \
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

#endif
