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
#define MAX_HEARTBEAT_DURATION 60

// Channels
#define EVENT_QUEUE_CHANNEL "new_event_queue_item"
#define WORK_QUEUE_CHANNEL "new_work_queue_item"

// GUCs
#define DEFAULT_WHEN_GUC_NAME "default_when_function"
#define SET_UID_GUC_NAME "set_uid_function"
#define GET_UID_GUC_NAME "get_uid_function"
#define ASYNC_GUC_NAME "execute_asynchronously"

// Regular Expression Settings
#define MAX_REGEX_GROUPS 1
#define MAX_REGEX_MATCHES 100

// Timeout for both curl connections and request duration
#define TIMEOUT_RETRY_LIMIT 1L
#define CURL_TIMEOUT 10L
#define RETRY_BACKOFF 5L

// SQL States
#define SQL_STATE_TERMINATED_BY_ADMINISTRATOR "57P01"
#define SQL_STATE_CANCELED_BY_ADMINISTRATOR "57014"

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
void _queue_loop( struct worker * );
void _queue_loop_wrapper( void * );
int work_queue_handler( struct worker * );
int event_queue_handler( struct worker * );
bool execute_action( struct worker *, PGresult *, int );
bool execute_action_query( struct worker *, struct action_result * );
bool execute_remote_uri_call( struct worker *, struct action_result * );
bool set_uid( struct worker *, char *, char * );
static size_t _curl_write_callback( void *, size_t, size_t, void * );

// Helper functions
PGresult * _execute_query( struct worker *, char *, char **, int );
void _gather_and_update_stats( struct worker *, struct em_stat ** );
char * get_column_value( int, PGresult *, char * );
bool is_column_null( int, PGresult *, char * );
bool _rollback_transaction( struct worker * );
bool _commit_transaction( struct worker * );
bool _begin_transaction( struct worker * );
void set_session_gucs( struct worker *, char * );
void clear_session_gucs( struct worker *, char * );
void _set_application_name( struct worker * );
bool _get_advisory_lock( struct worker * );

// Integration functions
void _cyanaudit_integration( struct worker *, char * );

// Program Entry
int main( int, char ** );
#endif
