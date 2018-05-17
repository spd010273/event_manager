/*------------------------------------------------------------------------
 *
 * event_manager.c
 *     Main event_manager routine and functions
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        event_manager.c
 *
 *------------------------------------------------------------------------
 */

// Compile with -DDEBUG to get debug messages

/* Includes */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <math.h>
#include <libpq-fe.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <signal.h>

#include <curl/curl.h>
#include "lib/strings.h"
#include "lib/query_helper.h"
#include "lib/util.h"

/* Constants */
#define MAX_CONN_RETRIES 3

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

// SQL States
#define SQL_STATE_TERMINATED_BY_ADMINISTRATOR "57P01"
#define SQL_STATE_CANCELED_BY_ADMINISTRATOR "57014"

#define LOG_LEVEL_WARNING "WARNING"
#define LOG_LEVEL_ERROR "ERROR"
#define LOG_LEVEL_FATAL "FATAL"
#define LOG_LEVEL_DEBUG "DEBUG"
#define LOG_LEVEL_INFO "INFO"

// Global Variables
PGconn * conn                = NULL;
char *   ext_schema          = NULL;
bool     cyanaudit_installed = false;
bool     enable_curl         = false;
CURL *   curl_handle         = NULL;


// Structures
struct curl_response {
    char * pointer;
    size_t size;
};

/* Function Prototypes */

// Main functions
void _queue_loop( const char *, void (*)(void) );
void work_queue_handler( void );
void event_queue_handler( void );
bool execute_action( PGconn *, PGresult *, int );
bool execute_action_query( PGconn *, char *, char *, char *, char *, char *, char * );
bool execute_remote_uri_call( PGconn *,  char *, char *, char *, char * );
bool set_uid( char * );
PGresult * get_parameters( PGconn *, char *, char * );
static size_t _curl_write_callback( void *, size_t, size_t, void * );

// Helper functions
PGresult * _execute_query( char *, char **, int, bool );
char * get_column_value( int, PGresult *, char * );
bool is_column_null( int, PGresult *, char * );
bool _rollback_transaction( void );
bool _commit_transaction( void );
bool _begin_transaction( void );

// Integration functions
void _cyanaudit_integration( PGconn *, char * );

// Program Entry
int main( int, char ** );


/* Functions */
PGresult * _execute_query( char * query, char ** params, int param_count, bool transaction_mode )
{
    PGresult * result;
    int        retry_counter     = 0;
    int        last_backoff_time = 0;
    char *     last_sql_state    = NULL;
#ifdef DEBUG
    int        i;
#endif

    if( conn == NULL )
    {
        if( transaction_mode )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Connection handle empty in transaction"
            );
            return NULL;
        }

        conn = PQconnectdb( conninfo );
    }

#ifdef DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "Executing query: '%s':",
        query
    );

    if( param_count > 0 )
    {
        _log( LOG_LEVEL_DEBUG, "With params:" );
        for( i = 0; i < param_count; i++ )
        {
            _log( LOG_LEVEL_DEBUG, "%d: %s", i, params[i] );
        }
    }
#endif

    while(
            PQstatus( conn ) != CONNECTION_OK &&
            retry_counter < MAX_CONN_RETRIES
         )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Failed to connect to DB server (%s). Retrying...",
            PQerrorMessage( conn )
        );

        if( transaction_mode )
        {
            return NULL;
        }

        _log(
            LOG_LEVEL_DEBUG,
            "Conninfo is: %s",
            conninfo
        );

        retry_counter++;
        last_backoff_time = (int) ( rand() / 1000 ) + last_backoff_time;

        if( conn != NULL )
        {
            _log(
                LOG_LEVEL_DEBUG,
                "Backoff time is %d",
                last_backoff_time
            );

            PQfinish( conn );
        }

        sleep( last_backoff_time );
        conn = PQconnectdb( conninfo );
    }

    _log(
        LOG_LEVEL_DEBUG,
        "Connection OK"
    );

    while(
             (
                 last_sql_state == NULL // No state (first pass)
              || strcmp(
                     last_sql_state,
                     SQL_STATE_TERMINATED_BY_ADMINISTRATOR
                 ) == 0
              || strcmp(
                     last_sql_state,
                     SQL_STATE_CANCELED_BY_ADMINISTRATOR
                 ) == 0
             )
          && retry_counter < MAX_CONN_RETRIES
         )
    {
        if( params == NULL )
        {
            result = PQexec( conn, query );
        }
        else
        {
            result = PQexecParams(
                conn,
                query,
                param_count,
                NULL,
                ( const char * const * ) params,
                NULL,
                NULL,
                0
            );
        }

        if(
            !(
                PQresultStatus( result ) == PGRES_COMMAND_OK ||
                PQresultStatus( result ) == PGRES_TUPLES_OK
            )
          )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Query '%s' failed: %s",
                query,
                PQerrorMessage( conn )
            );

            last_sql_state = PQresultErrorField( result, PG_DIAG_SQLSTATE );

            if( result != NULL )
            {
                PQclear( result );
            }

            retry_counter++;
        }
        else
        {
            return result;
        }
    }

    _log(
        LOG_LEVEL_ERROR,
        "Query failed after %i tries.",
        retry_counter
    );

    return NULL;
}

void _queue_loop( const char * channel, void (*dequeue_function)(void) )
{
    PGnotify * notify = NULL;
    char *     listen_command;
    PGresult * listen_result;

    listen_command = ( char * ) malloc(
        sizeof( char ) *
        (
            strlen( channel )
          + 10
        )
    );

    if( listen_command == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Malloc for listen channel failed"
        );
    }

    /* Command: 'LISTEN "?"\0' */
    strcpy( listen_command, "LISTEN \"" );
    strcat( listen_command, ( const char * ) channel );
    strcat( listen_command, "\"\0" );

    listen_result = _execute_query(
        listen_command,
        NULL,
        0,
        false
    );

    free( listen_command );

    if( listen_result == NULL )
    {
        return;
    }

    PQclear( listen_result );

    while( 1 )
    {
        sigset_t signal_set;
        int sock;
        fd_set input_mask;

        if( got_sigterm )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Exiting after receiving SIGTERM"
            );
            break;
        }

        sigaddset( &signal_set, SIGTERM );
        sock = PQsocket( conn );

        if( sock < 0 )
        {
            break;
        }

        FD_ZERO( &input_mask );
        FD_SET( sock, &input_mask );

        sigprocmask( SIG_BLOCK, &signal_set, NULL );
        if( select( sock + 1, &input_mask, NULL, NULL, NULL ) < 0 )
        {
            sigprocmask( SIG_UNBLOCK, &signal_set, NULL );
            _log(
                LOG_LEVEL_FATAL,
                "select() failed: %s",
                strerror( errno )
            );

            return;
        }

        sigprocmask( SIG_UNBLOCK, &signal_set, NULL );

        _log(
            LOG_LEVEL_DEBUG,
            "Handling notify"
        );

        PQconsumeInput( conn );

        while( ( notify = PQnotifies( conn ) ) != NULL )
        {
            _log(
                LOG_LEVEL_DEBUG,
                "ASYNCHRONOUS NOTIFY of '%s' received from "
                "backend PID %d WITH payload '%s'",
                notify->relname,
                notify->be_pid,
                notify->extra
            );

            // Get queue item
            PQfreemem( notify );
            (*dequeue_function)();
        }

        if( got_sigterm )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Exiting after receiving SIGTERM"
            );
            break;
        }
    }

    return;
}

/*
 *  These functions encapsulate the critical section of asynchronous mode that
 *  dequeues and executes arbitrary queries
 */

void event_queue_handler( void )
{
    PGresult * result;
    PGresult * work_item_result;
    PGresult * delete_result;
    PGresult * insert_result;

    struct query * work_item_query_obj = NULL;

    // Values that need to be copied to work_queue
    char * uid;
    char * recorded;
    char * transaction_label;
    char * execute_asynchronously;
    char * action;

    // Var's we need
    char * work_item_query;
    char * pk_value;
    char * op;
    char * ctid;
    char * event_table_work_item;

    char * parameters;
    char * params[9];
    int    i;

    if( !_begin_transaction() )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start event dequeue transaction"
        );

        return;
    }

    result = _execute_query(
        ( char * ) get_event_queue_item,
        NULL,
        0,
        true
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to dequeue event item"
        );

        _rollback_transaction();

        return;
    }

    if( PQntuples( result ) <= 0 )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Event queue processor received spurious NOTIFY"
        );

        _rollback_transaction();
        PQclear( result );

        return;
    }

    transaction_label      = get_column_value( 0, result, "transaction_label" );
    execute_asynchronously = get_column_value( 0, result, "execute_asynchronously" );
    action                 = get_column_value( 0, result, "action" );
    recorded               = get_column_value( 0, result, "recorded" );
    uid                    = get_column_value( 0, result, "uid" );

    ctid                   = get_column_value( 0, result, "ctid" );
    work_item_query        = get_column_value( 0, result, "work_item_query" );
    event_table_work_item  = get_column_value( 0, result, "event_table_work_item" );
    op                     = get_column_value( 0, result, "op" );
    pk_value               = get_column_value( 0, result, "pk_value" );

    work_item_query_obj = _new_query( work_item_query );

    work_item_query_obj = _add_parameter_to_query(
        work_item_query_obj,
        "event_table_work_item",
        event_table_work_item
    );

    work_item_query_obj = _add_parameter_to_query(
        work_item_query_obj,
        "uid",
        uid
    );

    work_item_query_obj = _add_parameter_to_query(
        work_item_query_obj,
        "op",
        op
    );

    work_item_query_obj = _add_parameter_to_query(
        work_item_query_obj,
        "pk_value",
        pk_value
    );

    work_item_query_obj = _add_parameter_to_query(
        work_item_query_obj,
        "recorded",
        recorded
    );

    if( work_item_query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "regex replace operation on work_item_query failed"
        );
        _rollback_transaction();
        PQclear( result );
        return;
    }

    _log( LOG_LEVEL_DEBUG, "WORK ITEM QUERY: " );
    _debug_struct( work_item_query_obj );
    work_item_result = _execute_query(
        work_item_query_obj->query_string,
        work_item_query_obj->_bind_list,
        work_item_query_obj->_bind_count,
        true
    );

    _free_query( work_item_query_obj );

    if( work_item_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to execute work item query"
        );

        PQclear( result );
        _rollback_transaction();
        return;
    }

    params[1] = uid;
    params[2] = recorded;
    params[3] = transaction_label;
    params[4] = action;
    params[5] = execute_asynchronously;

    for( i = 0; i < PQntuples( work_item_result ); i++ )
    {
        parameters = get_column_value( i, work_item_result, "parameters" );
        params[0]  = parameters;

        insert_result = _execute_query(
            ( char * ) new_work_item_query,
            params,
            6,
            true
        );

        if( insert_result == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to enqueue new work item"
            );

            PQclear( result );
            _rollback_transaction();
            return;
        }

        PQclear( insert_result );
    }

    // Get result from work item query, place into JSONB object and insert into work queue
    params[0] = event_table_work_item;
    params[1] = uid;
    params[2] = recorded;
    params[3] = pk_value;
    params[4] = op;
    params[5] = action;
    params[6] = transaction_label;
    params[7] = work_item_query;
    params[8] = ctid;

    delete_result = _execute_query(
        ( char * ) delete_event_queue_item,
        params,
        9,
        true
    );

    PQclear( result );

    if( delete_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to dequeue event queue item"
        );
        _rollback_transaction();
        return;
    }

    PQclear( delete_result );

    if( _commit_transaction() == false )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit event queue transaction"
        );
    }

    return;
}

void work_queue_handler( void )
{
    PGresult * result;
    PGresult * delete_result;

    bool   action_result;
    int    row_count = 0;
    int    i;
    char * params[6];

    _log(
        LOG_LEVEL_DEBUG,
        "handling work queue item"
    );

    /* Start transaction */
    if( !_begin_transaction() )
    {
        return;
    }

    result = _execute_query(
        ( char * ) get_work_queue_item,
        NULL,
        0,
        true
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Work queue dequeue operation failed"
        );

        _rollback_transaction();
        return;
    }

    /* Handle action execution */
    row_count = PQntuples( result );

    if( row_count == 0 )
    {
        _log(
            LOG_LEVEL_INFO,
            "Work queue processor received spurious NOTIFY"
        );
        _rollback_transaction();
        PQclear( result );
        return;
    }

    for( i = 0; i < row_count; i++ )
    {
        params[0] = get_column_value( i, result, "parameters" );
        params[1] = get_column_value( i, result, "uid" );
        params[2] = get_column_value( i, result, "recorded" );
        params[3] = get_column_value( i, result, "transaction_label" );
        params[4] = get_column_value( i, result, "action" );
        params[5] = get_column_value( i, result, "ctid" );

        /* Get detailed information about action, get parameter list */
        _log(
            LOG_LEVEL_DEBUG,
            "Executing action"
        );

        action_result = execute_action( conn, result, i );

        if( action_result == false )
        {
            PQclear( result );
            _rollback_transaction();
            return;
        }

        /* Flush queue item */
        delete_result = _execute_query(
            ( char * ) delete_work_queue_item,
            params,
            6,
            true
        );

        if( delete_result == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to flush work queue item"
            );

            PQclear( result );
            _rollback_transaction();
            return;
        }

        PQclear( delete_result );
    }

    PQclear( result );

    if( _commit_transaction() == false )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit work queue transaction: %s",
            PQerrorMessage( conn )
        );

        _rollback_transaction();
    }

    return;
}

char * get_column_value( int row, PGresult * result, char * column_name )
{
    if( is_column_null( row, result, column_name ) )
    {
        return NULL;
    }

    return PQgetvalue(
        result,
        row,
        PQfnumber(
            result,
            column_name
        )
    );
}

bool is_column_null( int row, PGresult * result, char * column_name )
{
    if(
        PQgetisnull(
            result,
            row,
            PQfnumber(
                result,
                column_name
            )
        ) == 1 )
    {
        return true;
    }

    return false;
}

static size_t _curl_write_callback( void * contents, size_t size, size_t n_mem_b, void * user_p )
{
    size_t real_size;
    struct curl_response * response_page;

    response_page = (struct curl_response *) user_p;

    real_size = size * n_mem_b;

    response_page->pointer = realloc(
        response_page->pointer,
        response_page->size + real_size + 1
    );

    if( response_page->pointer == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate enough memory for URI call"
        );

        return 0;
    }

    memcpy( &( response_page->pointer[ response_page->size ] ), contents, real_size );
    response_page->size += real_size;
    response_page->pointer[response_page->size] = 0;

    return real_size;
}

bool execute_remote_uri_call( PGconn * conn, char * uri, char * action, char * method, char * parameters )
{
    int malloc_size         = 0;
    int parameter_row_count = 0;
    int j;

    PGresult * jsonb_result;
    CURLcode   response;

    char * remote_call;
    char * param_list;
    char * key;
    char * value;

    bool first_param_pass = true;
    struct curl_response write_buffer;

    malloc_size = 2;

    param_list = ( char * ) malloc( sizeof( char ) * malloc_size );

    if( param_list == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Unable to allocate memory for parameters"
        );

        return false;
    }

    strcpy( param_list, "?" );

    jsonb_result = get_parameters( conn, action, parameters );

    if( jsonb_result == NULL )
    {
        free( param_list );
        return false;
    }

    parameter_row_count = PQntuples( jsonb_result );

    for( j = 0; j < parameter_row_count; j++ )
    {
        key   = get_column_value( j, jsonb_result, "key" );
        value = get_column_value( j, jsonb_result, "value" );
        _log(
            LOG_LEVEL_DEBUG,
            "Reallocating memory for %s:%s",
            key,
            value
        );
        // Reallocate the string to contain ?... &<key>=<value>
        malloc_size += strlen( key ) + strlen( value ) + 1;
        _log(
            LOG_LEVEL_DEBUG,
            "Malloc size is: %d",
            malloc_size
        );
        if( first_param_pass == false )
        {
            malloc_size++;
        }

        param_list = ( char * ) realloc(
            ( char * ) param_list,
            sizeof( char ) * malloc_size
        );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Unable to expand parameter_string"
            );
            PQclear( jsonb_result );
            return false;
        }

        if( first_param_pass == false )
        {
            strcat( param_list, "&" );
        }

        strcat( param_list, key );
        strcat( param_list, "=" );
        strcat( param_list, value );

        first_param_pass = false;
    }

    PQclear( jsonb_result );

    remote_call = ( char * ) malloc(
        sizeof( char )
      * ( strlen( uri ) + strlen( param_list ) + 1 )
    );

    if( remote_call == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Unable to prep final remote call string"
        );
        free( param_list );
        return false;
    }

    strcpy( remote_call, uri );
    strcat( remote_call, param_list );
    free( param_list );

    _log(
        LOG_LEVEL_DEBUG,
        "Making remote call to URI: %s",
        remote_call
    );

    if( enable_curl )
    {
        //Get: CURLOPT_HTTPGET
        //Post: CURLOPT_POST
        //Put: CURLOPT_PUT
        _log( LOG_LEVEL_DEBUG, "Curl is enabled, setting method to %s", method );
        if( strcmp( method, "GET" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting GET method" );
            response = curl_easy_setopt( curl_handle, CURLOPT_HTTPGET, 1 );
        }
        else if( strcmp( method, "PUT" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting PUT method" );
            response = curl_easy_setopt( curl_handle, CURLOPT_PUT, 1 );
        }
        else if( strcmp( method, "POST" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting POST method" );
            response = curl_easy_setopt( curl_handle, CURLOPT_POST, 1 );
        }
        else
        {
            _log(
                LOG_LEVEL_ERROR,
                "Unsupported method: %s",
                method
            );

            return false;
        }

        if( response != CURLE_OK )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to set curl method: %s",
                curl_easy_strerror( response )
            );
            return false;
        }

        // Initialize buffer
        write_buffer.pointer = malloc( 1 );
        write_buffer.size = 0;

        _log( LOG_LEVEL_DEBUG, "Setting URL to remote_call" );
        response = curl_easy_setopt( curl_handle, CURLOPT_URL, remote_call );
        _log( LOG_LEVEL_DEBUG, "Setting writer callback" );
        response = curl_easy_setopt(
            curl_handle,
            CURLOPT_WRITEFUNCTION,
            _curl_write_callback
        );

        response = curl_easy_setopt(
            curl_handle,
            CURLOPT_WRITEDATA,
            ( void * ) &write_buffer
        );

        if( response == CURLE_OK )
        {
            response = curl_easy_perform( curl_handle );
        }

        if( response != CURLE_OK )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed %s %s: %s",
                method,
                remote_call,
                curl_easy_strerror( response )
            );

            free( write_buffer.pointer );
            free( remote_call );
            return false;
        }

        _log(
            LOG_LEVEL_DEBUG,
            "Got response: '%s'",
            write_buffer.pointer
        );

        if( write_buffer.pointer != NULL )
        {
            free( write_buffer.pointer );
        }

        free( remote_call );
        return true;
    }
    else
    {
        _log(
            LOG_LEVEL_ERROR,
            "Could not make remote API call: %s, curl is disabled",
            remote_call
        );

        free( remote_call );
        return false;
    }

    free( remote_call );
    return true;
}

bool execute_action_query( PGconn * conn, char * query, char * action, char * parameters, char * uid, char * recorded, char * transaction_label )
{
    PGresult * parameter_result;
    PGresult * action_result;
    struct query * action_query;
    int parameter_count;
    char * key;
    char * value;
    int i;

    parameter_result = get_parameters( conn, action, parameters );

    if( parameter_result == NULL )
    {
        return false;
    }

    parameter_count = PQntuples( parameter_result );

    action_query = _new_query( query );

    if( action_query == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to initialize query struct"
        );
        PQclear( parameter_result );
        return false;
    }

    action_query = _add_parameter_to_query( action_query, "uid", uid );
    action_query = _add_parameter_to_query( action_query, "recorded", recorded );
    action_query = _add_parameter_to_query( action_query, "transaction_label", transaction_label );

    for( i = 0; i < parameter_count; i++ )
    {
        key   = get_column_value( i, parameter_result, "key" );
        value = get_column_value( i, parameter_result, "value" );

        action_query = _add_parameter_to_query( action_query, key, value );
    }

    PQclear( parameter_result );
    // Set UID
    set_uid( uid );

    // Execute query_copy
    _log(
        LOG_LEVEL_DEBUG,
        "Output query is: '%s'",
        action_query->query_string
    );

    _log( LOG_LEVEL_DEBUG, "ACTION QUERY: " );
    _debug_struct( action_query );

    action_result = _execute_query(
        action_query->query_string,
        action_query->_bind_list,
        action_query->_bind_count,
        true
    );

    _free_query( action_query );

    if( action_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to perform action query"
        );

        return false;
    }

    PQclear( action_result );

    return true;
}

PGresult * get_parameters( PGconn * conn, char * action, char * parameters )
{
    PGresult * jsonb_result;
    char * params[2];

    params[0] = parameters;
    params[1] = action;

    jsonb_result = _execute_query(
        ( char * ) expand_jsonb,
        params,
        2,
        true
    );

    if( jsonb_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to get static parameters for action"
        );
    }

    return jsonb_result;
}

bool execute_action( PGconn * conn, PGresult * result, int row )
{
    PGresult * action_result;
    bool       execute_action_result;

    char * parameters        = NULL;
    char * uid               = NULL;
    char * recorded          = NULL;
    char * transaction_label = NULL;
    char * action            = NULL;
    char * params[1];

    char * query  = NULL;
    char * uri    = NULL;
    char * method = NULL;

    parameters        = get_column_value( row, result, "parameters" );
    uid               = get_column_value( row, result, "uid" );
    recorded          = get_column_value( row, result, "recorded" );
    transaction_label = get_column_value( row, result, "transaction_label" );
    action            = get_column_value( row, result, "action" );

    params[0] = action;

    action_result = _execute_query(
        ( char * ) get_action,
        params,
        1,
        true
    );

    if( action_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to get action detail"
        );

        return false;
    }

    if( PQntuples( action_result ) <= 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Expected 1 row for action, got %d",
            PQntuples( action_result )
        );

        PQclear( action_result );
        return false;
    }

    query  = get_column_value( 0, action_result, "query" );
    uri    = get_column_value( 0, action_result, "uri" );
    method = get_column_value( 0, action_result, "method" );

    if( is_column_null( 0, action_result, "query" ) == false )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Executing action query"
        );

        execute_action_result = execute_action_query(
            conn,
            query,
            action,
            parameters,
            uid,
            recorded,
            transaction_label
        );

        if( execute_action_result == true && cyanaudit_installed == true )
        {
            _cyanaudit_integration( conn, transaction_label );
        }
    }
    else if( is_column_null( 0, action_result, "uri" ) == false )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Executing API call"
        );

        execute_action_result = execute_remote_uri_call( conn, uri, action, method, parameters );
    }
    else
    {
        // Wat
        _log(
            LOG_LEVEL_WARNING,
            "Dubious query / uri combination received as action"
        );
        execute_action_result = false;
    }

    PQclear( action_result );
    return execute_action_result;
}

void _cyanaudit_integration( PGconn * conn, char * transaction_label )
{
    PGresult * cyanaudit_result;
    char *     param[1];
    param[0] = transaction_label;

    cyanaudit_result = _execute_query(
        ( char * ) cyanaudit_label_tx,
        param,
        1,
        true
    );

    if( cyanaudit_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed call to fn_label_last_transaction()"
        );
        return;
    }

    PQclear( cyanaudit_result );
    return;
}

bool _rollback_transaction( void )
{
    PGresult * result;

    result = PQexec(
        conn,
        "ROLLBACK"
    );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to rollback transaction"
        );
        PQclear( result );
        return false;
    }

    PQclear( result );
    return true;
}

bool _commit_transaction( void )
{
    PGresult * result;

    result = PQexec(
        conn,
        "COMMIT"
    );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit transaction"
        );
        PQclear( result );
        return false;
    }

    PQclear( result );
    return true;
}

bool _begin_transaction( void )
{
    PGresult * result;

    result = PQexec(
        conn,
        "BEGIN"
    );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start transaction"
        );
        PQclear( result );
        return false;
    }

    PQclear( result );
    return true;
}

bool set_uid( char * uid )
{
    PGresult * uid_function_result;
    struct query * set_uid_query_obj = NULL;
    char *     params[1];
    char *     uid_function_name;
    char *     set_uid_query;

    params[0] = SET_UID_GUC_NAME;

    uid_function_result = _execute_query(
        ( char * ) _uid_function,
        params,
        1,
        true
    );

    if( uid_function_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to get set uid function"
        );

        return false;
    }

    if( is_column_null( 0, uid_function_result, "uid_function" ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Set UID function result is NULL"
        );

        PQclear( uid_function_result );
        return false;
    }

    uid_function_name = get_column_value(
        0,
        uid_function_result,
        "uid_function"
    );

    set_uid_query = ( char * ) malloc(
        sizeof( char )
      * ( strlen( uid_function_name ) + 8 )
    );

    if( set_uid_query == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for set uid operation"
        );
    }

    strcpy( set_uid_query, "SELECT " );
    strcat( set_uid_query, uid_function_name );

    set_uid_query_obj = _new_query( set_uid_query );
    free( set_uid_query );

    set_uid_query_obj = _add_parameter_to_query( set_uid_query_obj, "uid", uid );

    if( set_uid_query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to create query object for set uid function"
        );

        PQclear( uid_function_result );
        return false;
    }

    uid_function_result = _execute_query(
        set_uid_query_obj->query_string,
        set_uid_query_obj->_bind_list,
        set_uid_query_obj->_bind_count,
        true
    );

    _free_query( set_uid_query_obj );

    if( uid_function_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to set UID"
        );

        return false;
    }

    PQclear( uid_function_result );
    return true;
}

int main( int argc, char ** argv )
{
    PGresult * result;
    PGresult * cyanaudit_result;
    char *     params[1];

    int random_ind = 4; // determined by dice roll
    int row_count  = 0;

    //Crapily seed PRNG for backoff of connection attempts on DB failure
    srand( random_ind * time(0) );
    curl_handle = curl_easy_init();

    if( curl_handle != NULL  )
    {
        enable_curl = true;
        curl_easy_setopt( curl_handle, CURLOPT_NOSIGNAL, 1  );
        curl_easy_setopt( curl_handle, CURLOPT_USERAGENT, ( char * ) user_agent );
    }
    else
    {
        _log(
            LOG_LEVEL_ERROR,
            "CURL failed to initialize. Disabling"
        );

        enable_curl = false;
    }

    // Setup Signal Handlers
    signal ( SIGHUP, __sighup );
    signal ( SIGTERM, __sigterm );

    params[0] = EXTENSION_NAME;

    _parse_args( argc, argv );

    if( conninfo == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Invalid arguments!"
        );
    }

    result = _execute_query(
        ( char * ) extension_check_query,
        params,
        1,
        false
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Extension check failed: %s",
            PQerrorMessage( conn )
        );
    }

    row_count = PQntuples( result );

    if( row_count <= 0 )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Extension check failed. Is %s installed?",
            EXTENSION_NAME
        );
    }

    PQclear( result );

    /* Check for cyanaudit integration */
    cyanaudit_result = _execute_query(
        ( char * ) cyanaudit_check,
        NULL,
        0,
        false
    );

    if(
           cyanaudit_result != NULL
        && PQntuples( cyanaudit_result ) > 0
      )
    {
        cyanaudit_installed = true;
    }

    PQclear( cyanaudit_result );

    // Entry for other subs here
    if( work_listener )
    {
        _queue_loop( WORK_QUEUE_CHANNEL, &work_queue_handler );
    }
    else if( event_listener )
    {
        _queue_loop( EVENT_QUEUE_CHANNEL, &event_queue_handler );
    }

    if( conn != NULL )
    {
        PQfinish( conn );
    }

    free( conninfo );

    if( enable_curl )
    {
        curl_easy_cleanup( curl_handle );
    }

    return 0;
}
