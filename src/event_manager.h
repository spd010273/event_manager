/*------------------------------------------------------------------------
 *
 * event_manager.h
 *     Prototypes for main event / work handlers and helper functions
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        event_manager.h
 *
 *------------------------------------------------------------------------
 */

#ifndef EVENT_MANAGER_H
#define EVENT_MANAGER_H

#include <math.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "lib/util.h"
#include "lib/strings.h"
#include "lib/query_helper.h"
#include "lib/jsmn/jsmn.h"

/* Constants */
#define MAX_CONN_RETRIES 3
#define STAT_UPDATE_INTERVAL 60 // In seconds

// In seconds, the longest time we can go without hearing from our parent process
#define MAX_HEARTBEAT_DURATION 60.0
#define SELECT_TIMEOUT_SECONDS 5

// Channels
#define EVENT_QUEUE_CHANNEL "new_event_queue_item"
#define WORK_QUEUE_CHANNEL "new_work_queue_item"
#ifdef ALLOW_CONFIG_MANAGER
#define CONFIG_MANAGER_CHANNEL "configuration_update"
#endif // ALLOW_CONFIG_MANAGER

// GUCs
#define DEFAULT_WHEN_GUC_NAME "default_when_function"
#define SET_UID_GUC_NAME "set_uid_function"
#define GET_UID_GUC_NAME "get_uid_function"
#define ASYNC_GUC_NAME "execute_asynchronously"
#define BASE_URL_GUC_NAME "base_url"
#define SESSION_GUCS_NAME "session_gucs"

// Regular Expression Settings
#define MAX_REGEX_GROUPS 1
#define MAX_REGEX_MATCHES 100

// Timeout for both curl connections and request duration
#define TIMEOUT_RETRY_LIMIT 1L
#define CURL_TIMEOUT 600L // 10 minutes for request to complete
#define CURL_CONNECT_TIMEOUT 5L // 5 Secs for connection
#define RETRY_BACKOFF 5L

// SQL States
#define SQL_STATE_TERMINATED_BY_ADMINISTRATOR "57P01"
#define SQL_STATE_CANCELED_BY_ADMINISTRATOR "57014"
#define SQL_STATE_CONNECTION_FAILURE "08006"
#define SQL_STATE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION "08001"
#define SQL_STATE_CONNECTION_DOES_NOT_EXIST "08003"
#define SQL_STATE_CONNECTION_EXCEPTION "08000"

// Structures
struct curl_response {
    char * pointer;
    size_t size;
};

struct action_result {
    char * query;
    char * uri;
    char * method;
    bool   use_ssl;
    char * parameters;
    char * static_parameters;
    char * session_values;
    char * uid;
    char * transaction_label;
    char * recorded;
};

/* Function Prototypes */
// Main functions
static void _queue_loop( struct worker * );
static void _queue_loop_wrapper( void * );
static int work_queue_handler( struct worker * );
static int event_queue_handler( struct worker * );
static bool execute_action( struct worker *, PGresult *, int );
static bool execute_action_query( struct worker *, struct action_result * );
static bool execute_remote_uri_call( struct worker *, struct action_result * );
static bool set_uid( struct worker *, char *, char * );
static size_t _curl_write_callback( void *, size_t, size_t, void * );
#ifdef ALLOW_CONFIG_MANAGER
static int _config_manager_loop( struct worker * );
#endif // ALLOW_CONFIG_MANAGER

// Helper functions
static PGresult * _execute_query( struct worker *, char *, char **, int );
static void _gather_and_update_stats( struct worker *, struct em_stat ** );
static char * get_column_value( int, PGresult *, char * );
static bool is_column_null( int, PGresult *, char * );
static bool _rollback_transaction( struct worker * );
static bool _commit_transaction( struct worker * );
static bool _begin_transaction( struct worker * );
static void set_session_gucs( struct worker *, char * );
static void clear_session_gucs( struct worker *, char * );
static void _set_application_name( struct worker * );
static bool _get_advisory_lock( struct worker * );
static bool db_connect( struct worker * );
#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
static void _get_child_counts_from_db( void );
#endif // ALLOW_OVERRIDE_WORKER_COUNTS
// Integration functions
static void _cyanaudit_integration( struct worker *, char * );
static bool parent_get_advisory_lock( void );

// Program Entry
int main( int, char ** );
#endif // EVENT_MANAGER_H
