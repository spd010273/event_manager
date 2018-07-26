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
#include "event_manager.h"
#include "lib/util.h"
#include "lib/strings.h"
#include "lib/query_helper.h"
#include "lib/jsmn/jsmn.h"

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

// Global Variables
PGconn * conn                = NULL;
char *   ext_schema          = NULL;
bool     cyanaudit_installed = false;
bool     enable_curl         = false;
CURL *   curl_handle         = NULL;
bool     tx_in_progress      = false;

// Flags
sig_atomic_t got_sighup  = false;
sig_atomic_t got_sigterm = false;

/* Functions */

/*
 * PGresult * _execute_query( char * query, char ** params, int param_count )
 *     Executes a given query. Has handlers present for:
 *         DB connection interruptions
 *         SQL command termination by administrator
 *         Error handling
 *
 * Arguments:
 *     - char * query:    SQL query string to execute.
 *     - char ** params:  Optional parameter list to be bound into the query.
 *     - int param_count: Length of above structure.
 * Return:
 *     PGresult * result: Result handle of the executed query.
 * Error Conditions:
 *     - Returns NULL on error.
 *     - Emits error on failure to execute query.
 *     - Emits error on disconnection of DB handle.
 *     - Emits error on syntax or improper termination of query.
 */
PGresult * _execute_query( char * query, char ** params, int param_count )
{
    PGresult * result            = NULL;
    int        retry_counter     = 0;
    int        last_backoff_time = 0;
    char *     last_sql_state    = NULL;
#ifdef DEBUG
    int        i = 0;
#endif

    if( conn == NULL )
    {
        if( tx_in_progress )
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

        if( tx_in_progress )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to connect to DB server (%s), while in a transaction."
                "Transaction was automatically aborted",
                PQerrorMessage( conn )
            );
            tx_in_progress = false;
            return NULL;
        }

        _log(
            LOG_LEVEL_WARNING,
            "Failed to connect to DB server (%s). Retrying...",
            PQerrorMessage( conn )
        );

        _log(
            LOG_LEVEL_DEBUG,
            "Conninfo is: %s",
            conninfo
        );

        retry_counter++;
        last_backoff_time = (int) ( 10 * ( rand() / RAND_MAX ) ) + last_backoff_time;

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

/*
 * void _queue_loop( const char * channel, int (*dequeue_function)(void) )
 *     Listens to the specified channel for asynchronous notifications, calling
 *     the dequeue_function when a new queue item is present.
 *
 * Arguments:
 *     - const char * channel:          Channel on which the LISTEN command
 *                                      should be issued.
 *     - int (*dequeue_function)(void): pointer to the subroutine that handles a
 *                                      NOTIFY issued on this channel.
 * Return:
 *     None
 * Error conditions:
 *     - Exits program on failure to allocate string memory.
 *     - Emits error when listen channel cannot be bound with select().
 *     - Emits error when a SIGTERM is received.
 */
void _queue_loop( const char * channel, int (*dequeue_function)(void) )
{
    PGnotify * notify          = NULL;
    char *     listen_command  = NULL;
    PGresult * listen_result   = NULL;
    int        processed_count = 0;

    // Check queue prior to entering main loop
    _log(
        LOG_LEVEL_DEBUG,
        "Processing queue entries prior to entering main loop"
    );

    while( (*dequeue_function)() > 0 )
    {
        processed_count++;
    }

    if( processed_count > 0 )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Processed %d queue entries prior to main loop",
            processed_count
        );

        processed_count = 0;
    }

    listen_command = ( char * ) calloc(
        ( strlen( channel ) + 10 ),
        sizeof( char )
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
        0
    );

    free( listen_command );

    if( listen_result == NULL )
    {
        return;
    }

    PQclear( listen_result );

    while( 1 )
    {
#ifdef BLOCKING_SELECT
        sigset_t signal_set;
#endif
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

#ifdef BLOCKING_SELECT
        sigaddset( &signal_set, SIGTERM );
#endif
        sock = PQsocket( conn );

        if( sock < 0 )
        {
            break;
        }

        FD_ZERO( &input_mask );
        FD_SET( sock, &input_mask );
#ifdef BLOCKING_SELECT
        sigprocmask( SIG_BLOCK, &signal_set, NULL );
#endif
        if( select( sock + 1, &input_mask, NULL, NULL, NULL ) < 0 )
        {
#ifdef BLOCKING_SELECT
            sigprocmask( SIG_UNBLOCK, &signal_set, NULL );
#endif
            _log(
                LOG_LEVEL_FATAL,
                "select() failed: %s",
                strerror( errno )
            );

            return;
        }
#ifdef BLOCKING_SELECT
        sigprocmask( SIG_UNBLOCK, &signal_set, NULL );
#endif

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
            while( (*dequeue_function)() > 0 )
            {
                processed_count++;
            }

            _log(
                LOG_LEVEL_INFO,
                "Processed %d queue entries",
                processed_count
            );
            processed_count = 0;
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

/*
 * int event_queue_handle( void )
 *     Handles new entries in event_manager.tb_event_queue.
 *
 * Arguments:
 *     None
 * Return:
 *     int rows_processed: 1 when a queue entry is successfully processed,
 *                         0 otherwise.
 * Error Conditions:
 *     - Emits error when a transaction fails to BEGIN, COMMIT or
 *       ROLLBACK (when necessary)
 *     - Emits error upon failure to allocate string memory
 *     - Emits error when a critical section step fails, including:
 *              - Queue item dequeue
 *              - Queue item processing (work item query preparation)
 *              - Work item query execution
 *              - Insertion into work queue
 *              - Deletion of dequeued queue item
 *              - commit of transaction
 */
int event_queue_handler( void )
{
    PGresult * result           = NULL;
    PGresult * work_item_result = NULL;
    PGresult * delete_result    = NULL;
    PGresult * insert_result    = NULL;

    struct query * work_item_query_obj = NULL;

    // Values that need to be copied to work_queue
    char * uid                    = NULL;
    char * recorded               = NULL;
    char * transaction_label      = NULL;
    char * execute_asynchronously = NULL;
    char * action                 = NULL;

    // Var's we need
    char * work_item_query       = NULL;
    char * pk_value              = NULL;
    char * op                    = NULL;
    char * ctid                  = NULL;
    char * event_table_work_item = NULL;
    char * old                   = NULL;
    char * new                   = NULL;
    char * session_values        = NULL;

    char * parameters = NULL;
    char * params[9]  = {NULL};
    int    i          = 0;

    if( !_begin_transaction() )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start event dequeue transaction"
        );

        return 0;
    }

    result = _execute_query(
        ( char * ) get_event_queue_item,
        NULL,
        0
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to dequeue event item"
        );

        _rollback_transaction();
        return 0;
    }

    if( PQntuples( result ) <= 0 )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Event queue processor received spurious NOTIFY"
        );

        _rollback_transaction();
        PQclear( result );

        return 0;
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
    old                    = get_column_value( 0, result, "old" );
    new                    = get_column_value( 0, result, "new" );
    session_values         = get_column_value( 0, result, "session_values" );

    set_session_gucs( session_values );
    work_item_query_obj = _new_query( work_item_query );

    _add_parameter_to_query(
        work_item_query_obj,
        "event_table_work_item",
        event_table_work_item
    );

    _add_parameter_to_query(
        work_item_query_obj,
        "uid",
        uid
    );

    _add_parameter_to_query(
        work_item_query_obj,
        "op",
        op
    );

    _add_parameter_to_query(
        work_item_query_obj,
        "pk_value",
        pk_value
    );

    _add_parameter_to_query(
        work_item_query_obj,
        "recorded",
        recorded
    );

    _add_json_parameter_to_query(
        work_item_query_obj,
        new,
        "NEW."
    );

    _add_json_parameter_to_query(
        work_item_query_obj,
        old,
        "OLD."
    );

    _add_json_parameter_to_query(
        work_item_query_obj,
        session_values,
        ( char * ) NULL
    );

    _finalize_query( work_item_query_obj );

    if( work_item_query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "regex replace operation on work_item_query failed"
        );
        _rollback_transaction();
        PQclear( result );
        return 0;
    }

    _log( LOG_LEVEL_DEBUG, "WORK ITEM QUERY: " );
    _debug_struct( work_item_query_obj );
    work_item_result = _execute_query(
        work_item_query_obj->query_string,
        work_item_query_obj->_bind_list,
        work_item_query_obj->_bind_count
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
        return 0;
    }

    params[1] = uid;
    params[2] = recorded;
    params[3] = transaction_label;
    params[4] = action;
    params[5] = execute_asynchronously;
    params[6] = session_values;

    for( i = 0; i < PQntuples( work_item_result ); i++ )
    {
        parameters = get_column_value( i, work_item_result, "parameters" );
        params[0]  = parameters;

        insert_result = _execute_query(
            ( char * ) new_work_item_query,
            params,
            7
        );

        if( insert_result == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to enqueue new work item"
            );

            PQclear( result );
            _rollback_transaction();
            return 0;
        }

        PQclear( insert_result );
    }

    // Get result from work item query, place into JSONB object and insert
    //  into work queue
    params[0] = event_table_work_item;
    params[1] = uid;
    params[2] = recorded;
    params[3] = pk_value;
    params[4] = op;
    params[5] = old;
    params[6] = new;
    params[7] = session_values;
    params[8] = ctid;

    delete_result = _execute_query(
        ( char * ) delete_event_queue_item,
        params,
        9
    );

    // Clear GUCs prior to freeing result handle
    clear_session_gucs( session_values );
    PQclear( result );
    PQclear( work_item_result );

    if( delete_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to dequeue event queue item"
        );
        _rollback_transaction();
        return 0;
    }

    PQclear( delete_result );

    if( _commit_transaction() == false )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit event queue transaction"
        );

        return 0;
    }

    return 1;
}

/*
 * int work_queue_handler( void )
 *     Handles new entries in event_manager.tb_event_queue
 *
 * Arguments:
 *     None
 * Return:
 *     int rows_processed: number of queue entries processed, 0 otherwise.
 * Error Conditions:
 *     - Emits error when a transaction fails to BEGIN, COMMIT or ROLLBACK
 *       (when necessary).
 *     - Emits error upon failure to allocate string memory.
 *     - Emits error when a critical section step fails, including:
 *              - Queue item dequeue
 *              - Queue item processing (action preparation)
 *              - action execution
 *              - Deletion of dequeued queue item
 *              - commit of transaction (if applicable)
 */
int work_queue_handler( void )
{
    PGresult * result        = NULL;
    PGresult * delete_result = NULL;

    bool   action_result = false;
    int    row_count     = 0;
    int    i             = 0;
    char * params[7]     = {NULL};

    _log(
        LOG_LEVEL_DEBUG,
        "handling work queue item"
    );

    /* Start transaction */
    if( !_begin_transaction() )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start transaction"
        );
        return 0;
    }

    result = _execute_query(
        ( char * ) get_work_queue_item,
        NULL,
        0
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Work queue dequeue operation failed"
        );

        _rollback_transaction();
        return 0;
    }

    /* Handle action execution */
    row_count = PQntuples( result );

    if( row_count == 0 )
    {
        _rollback_transaction();
        PQclear( result );
        return 0;
    }

    for( i = 0; i < row_count; i++ )
    {
        params[0] = get_column_value( i, result, "parameters" );
        params[1] = get_column_value( i, result, "uid" );
        params[2] = get_column_value( i, result, "recorded" );
        params[3] = get_column_value( i, result, "transaction_label" );
        params[4] = get_column_value( i, result, "action" );
        params[5] = get_column_value( i, result, "session_values" );
        params[6] = get_column_value( i, result, "ctid" );

        /* Get detailed information about action, get parameter list */
        _log(
            LOG_LEVEL_DEBUG,
            "Executing action"
        );

        action_result = execute_action( result, i );

        if( action_result == false )
        {
            PQclear( result );
            _rollback_transaction();
            return 0;
        }

        /* Flush queue item */
        delete_result = _execute_query(
            ( char * ) delete_work_queue_item,
            params,
            7
        );

        if( delete_result == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to flush work queue item"
            );

            PQclear( result );
            _rollback_transaction();
            return 0;
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

    return 1;
}

/*
 * char * get_column_value( int row, PGresult * result, char * column_name )
 *    libpq wrapper for PQgetvalue for code simplification.
 *
 * Arguments:
 *     - int row:            the row number to get the column value from.
 *     - PGresult * result:  The libpq result handle of a previously executed
 *                           query where the results are present.
 *     - char * column_name: the name of the column which contains the value
 *                           indexed by row.
 * Return:
 *     char * column_value:  The string representation of the value of the column.
 *                           NULL results are returned as ANSI C NULL.
 * Error Conditions:
 *     None - may emit libpq errors or warnings
 */
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

/*
 * bool is_column_null( int row, PGresult * result, char * column_name )
 *     checks the specified row/column for NULL
 *
 * Arguments:
 *    - int row:            The row number of the value to check.
 *    - PGresult * result:  The libpq result handle of a previously executed query.
 *    - char * column_name: the name of the column which contains the
 *                          to-be-checked value indexed by row.
 * Return:
 *    - bool is_null:       true if the row/column value is null, false otherwise.
 * Error Conditions:
 *    None - may emit libpq errors or warnings
 */
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

/*
 * static size_t _curl_write_callback(
 *     void * contents,
 *     size_t size,
 *     size_t n_mem_b,
 *     void * user_p
 * )
 *     callback handler which stores CuRL results into the curl_response
 *     buffer struct.
 *
 * Arguments:
 *     - void * contents:  Response contents from curl call.
 *     - size_t size:      Response contents size (length).
 *     - size_t n_mem_b:   Number of bytes of the response.
 *     - void * user_p:    Pointer to buffer struct.
 * Return:
 *     - size_t real_size: Size in allocated bytes of the buffer size increase.
 * Error Conditions:
 *     - Emits error on failure to allocate memory for buffer.
 */
static size_t _curl_write_callback(
    void * contents,
    size_t size,
    size_t n_mem_b,
    void * user_p
)
{
    size_t real_size                     = 0;
    struct curl_response * response_page = NULL;

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

    memcpy(
        &( response_page->pointer[ response_page->size ] ),
        contents,
        real_size
    );

    response_page->size += real_size;
    response_page->pointer[response_page->size] = 0;

    return real_size;
}

/*
 * bool execute_remote_uri_call( struct action_result * )
 *     Uses CuRL to execute a remote POST, PUT, or GET request over HTTP/HTTPS
 *
 * Arguments:
 *     struct action_result * action: All available information on the action to
 *                                    be executed.
 * Return:
 *     - bool is_success:             True if the call happened without error,
 *                                    false otherwise.
 * Error Conditions:
 *     - Emits error upon failure to allocate memory.
 *     - Emits error when unsupported method passed as argument.
 *     - Can emit CuRL errors / warnings.
 */
bool execute_remote_uri_call( struct action_result * action )
{
    struct curl_response write_buffer = {0};
    CURLcode response                 = {0};
    char * remote_call                = NULL;
    char * param_list                 = NULL;
    int malloc_size                   = 2;

    param_list = ( char * ) calloc( malloc_size, sizeof( char ) );

    if( param_list == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Unable to allocate memory for parameters"
        );

        return false;
    }

    if( strcmp( action->method, "GET" ) == 0 )
    {
        strcpy( param_list, "?" );
    }

    _add_json_parameters_to_param_list(
        curl_handle,
        param_list,
        action->parameters,
        &malloc_size
    );

    if( action->static_parameters != NULL )
    {
        malloc_size++;
        param_list = ( char * ) realloc( param_list, malloc_size );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Unable to allocate memory for simple string "
                "concatenation operation :("
            );
            return false;
        }

        strcat( param_list, "&" );

        _add_json_parameters_to_param_list(
            curl_handle,
            param_list,
            action->static_parameters,
            &malloc_size
        );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to substitute parameters in URI parameter list"
            );
            return false;
        }
    }

    if( action->session_values != NULL )
    {
        malloc_size++;
        param_list = ( char * ) realloc( param_list, malloc_size );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Unable to allocate memory for simple string"
                "concatenation operation :("
            );
            return false;
        }

        strcat( param_list, "&" );

        _add_json_parameters_to_param_list(
            curl_handle,
            param_list,
            action->session_values,
            &malloc_size
        );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to substitute session_values in URI parameter list"
            );
            return false;
        }
    }

    if( enable_curl )
    {
        //Get: CURLOPT_HTTPGET
        //Post: CURLOPT_POST
        //Put: CURLOPT_PUT
        _log(
            LOG_LEVEL_DEBUG,
            "Curl is enabled, setting method to %s",
            action->method
        );

        if( strcmp( action->method, "GET" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting GET method" );
            response = curl_easy_setopt( curl_handle, CURLOPT_HTTPGET, 1 );
        }
        else if( strcmp( action->method, "PUT" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting PUT method" );
            response = curl_easy_setopt( curl_handle, CURLOPT_PUT, 1 );
        }
        else if( strcmp( action->method, "POST" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting POST method" );
            response = curl_easy_setopt( curl_handle, CURLOPT_POST, 1 );
        }
        else
        {
            _log(
                LOG_LEVEL_ERROR,
                "Unsupported method: %s",
                action->method
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

        if( strcmp( action->method, "GET" ) == 0 )
        {
            _log( LOG_LEVEL_DEBUG, "Setting URL to remote_call" );
            remote_call = ( char * ) calloc(
                ( strlen( action->uri ) + strlen( param_list ) + 1 ),
                sizeof( char )
            );

            if( remote_call == NULL )
            {
                _log(
                    LOG_LEVEL_ERROR,
                    "Unable to prep final remote call string"
                );
                free( param_list );
                free( write_buffer.pointer );
                return false;
            }

            strcpy( remote_call, action->uri );
            strcat( remote_call, param_list );

            _log(
                LOG_LEVEL_DEBUG,
                "Making remote call to URI: %s",
                remote_call
            );
        }
        else
        {
            // Set post fields for PUT / POST
            remote_call = action->uri;
            curl_easy_setopt(
                curl_handle,
                CURLOPT_POSTFIELDS,
                param_list
            );
        }

        if( action->use_ssl )
        {
            response = curl_easy_setopt(
                curl_handle,
                CURLOPT_USE_SSL,
                CURLUSESSL_TRY
            );
        }

        response = curl_easy_setopt(
            curl_handle,
            CURLOPT_URL,
            remote_call
        );

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
            _log(
                LOG_LEVEL_DEBUG,
                "Making %s call with param list %s",
                action->method,
                param_list
            );
            response = curl_easy_perform( curl_handle );
        }

        free( param_list );

        if( response != CURLE_OK )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed %s %s: %s",
                action->method,
                remote_call,
                curl_easy_strerror( response )
            );

            free( write_buffer.pointer );
            if( strcmp( action->method, "GET" ) == 0 )
            {
                free( remote_call );
            }
            return false;
        }

        _log(
            LOG_LEVEL_DEBUG,
            "Got response: '%s'",
            write_buffer.pointer
        );

        free( write_buffer.pointer );

        if( strcmp( action->method, "GET" ) == 0 )
        {
            free( remote_call );
        }

        return true;
    }
    else
    {
        _log(
            LOG_LEVEL_ERROR,
            "Could not make remote API call: %s, curl is disabled",
            remote_call
        );

        if( strcmp( action->method, "GET" ) == 0 )
        {
            free( remote_call );
        }

        return false;
    }

    if( strcmp( action->method, "GET" ) == 0 )
    {
        free( remote_call );
    }

    return true;
}

/*
 * bool execute_action_query( struct action_result * )
 *     executes an action query
 *
 * Arguments:
 *     struct action_result *: All available information related to the action to
 *                             be executed.
 * Return:
 *     bool is_success:        true if the transaction completed successfully,
 *                             false otherwise.
 * Error Conditions:
 *     - Emit error on failure to allocate string memory.
 *     - Emit error on transaction failure
 */
bool execute_action_query( struct action_result * action )
{
    PGresult * action_result;
    struct query * action_query;

    action_query = _new_query( action->query );

    if( action_query == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to initialize query struct"
        );
        return false;
    }

    set_session_gucs( action->session_values );
    _add_parameter_to_query(
        action_query,
        "uid",
        action->uid
    );

    _add_parameter_to_query(
        action_query,
        "recorded",
        action->recorded
    );

    _add_parameter_to_query(
        action_query,
        "transaction_label",
        action->transaction_label
    );

    _log( LOG_LEVEL_DEBUG, "PARAMS: %s", action->parameters );

    _add_json_parameter_to_query(
        action_query,
        action->parameters,
        ( char * ) NULL
    );

    _add_json_parameter_to_query(
        action_query,
        action->static_parameters,
        ( char * ) NULL
    );

    _add_json_parameter_to_query(
        action_query,
        action->session_values,
        ( char * ) NULL
    );

    _finalize_query( action_query );

    // Set UID
    set_uid( action->uid, action->session_values );

    if( action_query == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Parameterization of action query failed"
        );
        return false;
    }

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
        action_query->_bind_count
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

    clear_session_gucs( action->session_values );
    PQclear( action_result );
    return true;
}

/*
 * bool execute_action( PGresult * result, int row )
 *     Wrapper for processing work_queue items and dispatching them to either
 *     the URI or query execution subroutines.
 *
 * Arguments:
 *     - PGresult * result: Dequeued work queue entry.
 *     - int row:           Row index of the work queue entry.
 * Return:
 *     bool is_success:     true indicates successful completion of the action,
 *                          false otherwise.
 * Error Conditions
 *     - Emits error on inability to allocate string memory.
 *     - Emits error from URI or query subroutines upon failure.
 */
bool execute_action( PGresult * result, int row )
{
    bool   execute_action_result = false;
    struct action_result action  = {0};
    struct action_result * action_ptr = NULL;
    char * use_ssl = NULL;

    action_ptr               = &action;
    action.parameters        = get_column_value( row, result, "parameters" );
    action.uid               = get_column_value( row, result, "uid" );
    action.recorded          = get_column_value( row, result, "recorded" );
    action.session_values    = get_column_value( row, result, "session_values" );
    action.uri               = get_column_value( row, result, "uri" );

    if( is_column_null( row, result, "static_parameters" ) == false )
    {
        action.static_parameters = get_column_value(
            row,
            result,
            "static_parameters"
        );
    }

    action.transaction_label = get_column_value(
        row,
        result,
        "transaction_label"
    );

    action.method = get_column_value( row, result, "method" );
    action.query  = get_column_value( row, result, "query" );
    use_ssl       = get_column_value( row, result, "use_ssl" );

    if( strcmp( use_ssl, "t" ) == 0 || strcmp( use_ssl, "T" ) == 0 )
    {
        action.use_ssl = true;
    }

    // Determine if action is query or URI based, send to correct handler
    if( is_column_null( 0, result, "query" ) == false )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Executing action query"
        );

        execute_action_result = execute_action_query( action_ptr );

        if( execute_action_result == true && cyanaudit_installed == true )
        {
            _cyanaudit_integration( action.transaction_label );
        }
    }
    else if( is_column_null( 0, result, "uri" ) == false )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Executing API call"
        );

        execute_action_result = execute_remote_uri_call( action_ptr );
    }
    else
    {
        // Wat
        _log(
            LOG_LEVEL_WARNING,
            "Conflicting query / uri combination received as action"
        );
        execute_action_result = false;
    }

    return execute_action_result;
}

/*
 * void _cyanaudit_integration( char * transaction_label )
 *     Labels the completed transaction in CyanAudit, if present.
 *
 * Arguments:
 *     char * transaction_label: Label with which to identify transaction.
 * Return:
 *     None
 * Error conditions:
 *     Emits error on failure to make a call to
 *     cyanaudit.fn_label_last_transaction().
 */
void _cyanaudit_integration( char * transaction_label )
{
    PGresult * cyanaudit_result = NULL;
    char *     param[1]         = {NULL};

    param[0] = transaction_label;

    cyanaudit_result = _execute_query(
        ( char * ) cyanaudit_label_tx,
        param,
        1
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

/*
 * bool _rollback_transaction( void )
 *     rolls back a SQL transaction
 *
 * Arguments:
 *     None
 * Return:
 *     bool is_success: true indicates the transaction was successfully rolled back
 * Error Conditions:
 *     Emits error on failure to rollback transaction
 */
bool _rollback_transaction( void )
{
    PGresult * result = NULL;

    if( !tx_in_progress )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempted to issue ROLLBACK when no transaction was in progress"
        );
        return false;
    }

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
    tx_in_progress = false;
    return true;
}

/*
 * bool _commit_transaction( void )
 *     Commits a SQL transaction.
 *
 * Arguments:
 *    None
 * Return:
 *    bool is_success: true indicates that the transaction was successfully
 *                     committed.
 * Error Conditions:
 *    Emits error on failure to commit transaction.
 */
bool _commit_transaction( void )
{
    PGresult * result = NULL;

    if( !tx_in_progress )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempted to issue COMMIT when not transaction was in progress"
        );
        return false;
    }

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
    tx_in_progress = false;
    return true;
}

/*
 * bool _begin_transaction( void )
 *     Begins a SQL transaction, sets the global tx state flag in the process.
 *
 * Arguments:
 *     None
 * Return:
 *     bool is_success: Indicates that the transaction was successfully begun.
 * Error Conditions:
 *     Emits error on failure to start transaction (one is already in progress.)
 */
bool _begin_transaction( void )
{
    PGresult * result = NULL;

    if( tx_in_progress )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempt to issue BEGIN when a transaction is already in progress"
        );
        return false;
    }

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
    tx_in_progress = true;
    return true;
}

/*
 * bool set_uid( char * uid, char * session_values )
 *     Makes a call to the function specified in event_manager.set_uid_function,
 *     binding in the uid to ?uid? and the originating transaction GUC values
 *     specified in event_manager.session_gucs to their respective names.
 *
 * Arguments:
 *     - char * uid:            String representation of the integer user ID
 *     - char * session_values: JSONB object containing the key-value pairs of
 *                              GUCs and their values.
 * Return:
 *     bool is_success:         Returns true on successful invokation of the
 *                              set_uid_function, false otherwise.
 * Error Conditions:
 *     - Emits error on failure to allocate string memory.
 *     - Emits error on failure to execute SQL function.
 */
bool set_uid( char * uid, char * session_values )
{
    PGresult *     uid_function_result = NULL;
    struct query * set_uid_query_obj   = NULL;

    char * params[1]         = {NULL};
    char * uid_function_name = NULL;
    char * set_uid_query     = NULL;

    params[0] = SET_UID_GUC_NAME;

    uid_function_result = _execute_query(
        ( char * ) _uid_function,
        params,
        1
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

    set_uid_query = ( char * ) calloc(
        ( strlen( uid_function_name ) + 8 ),
        sizeof( char )
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

    _add_parameter_to_query(
        set_uid_query_obj,
        "uid",
        uid
    );

    _add_json_parameter_to_query(
        set_uid_query_obj,
        session_values,
        ( char * ) NULL
    );

    _finalize_query( set_uid_query_obj );

    if( set_uid_query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to create query object for set uid function"
        );

        PQclear( uid_function_result );
        return false;
    }

    PQclear( uid_function_result );
    // Re-use handle
    uid_function_result = _execute_query(
        set_uid_query_obj->query_string,
        set_uid_query_obj->_bind_list,
        set_uid_query_obj->_bind_count
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

// Signal Handlers

/*
 * void __sigterm( int sig )
 *     SIGTERM signal handler
 *
 * Arguments:
 *     int sig: Signal number for SIGTERM
 * Return:
 *     None
 * Error Conditions:
 *     Emits error upon receiving SIGTERM
 */
void __sigterm( int sig )
{
    _log(
        LOG_LEVEL_ERROR,
        "Got SIGTERM. Completing current transaction..."
    );

    free( conninfo );
    if( enable_curl )
    {
        curl_easy_cleanup( curl_handle );
        curl_global_cleanup();
    }

    if( tx_in_progress )
    {
        _rollback_transaction();
    }

    if( conn != NULL )
    {
        PQfinish( conn );
    }

    exit( 1 );
}

void __sighup( int sig )
{
    got_sighup = true;
    signal ( sig, __sighup );
    return;
}

/*
 * int main( int argc, char ** argv )
 *     entry point for this program. Performs the following:
 *         Calls argument processing
 *         Connects to database
 *         starts the queue_loop function
 *
 * Arguments:
 *     - int argc:     Count of arguments which this program was invoked with.
 *     - char ** argv: Array of command-line parameters this program was
 *                     invoked with.
 * Return:
 *     - int errcode:  0 on success, errno on failure.
 * Error Conditions:
 *     - Emits error on failure to initialize:
 *           - CuRL library
 *           - DB Connection
 *           - Extension installation checks
 *     - Emits error on failure to validate arguments.
 *     - Emits error on failure to allocate string memory.
 *     - May emit CuRL warnings or errors.
 *     - May emit libpq-fe warnings or errors.
 */
int main( int argc, char ** argv )
{
    PGresult * result           = NULL;
    PGresult * cyanaudit_result = NULL;
    char *     params[1]        = {NULL};

    int random_ind = 4; // determined by dice roll
    int row_count  = 0;

    //Crapily seed PRNG for backoff of connection attempts on DB failure
    srand( random_ind * time(0) );
    curl_handle = curl_easy_init();

    if( curl_handle != NULL  )
    {
        enable_curl = true;
        curl_easy_setopt( curl_handle, CURLOPT_NOSIGNAL, 1  );
        curl_easy_setopt(
            curl_handle,
            CURLOPT_USERAGENT,
            ( char * ) user_agent
        );
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
        1
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
        0
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

    // We shouldn't get to this point, but just in case
    if( conn != NULL )
    {
        PQfinish( conn );
    }

    free( conninfo );

    if( enable_curl )
    {
        curl_easy_cleanup( curl_handle );
        curl_global_cleanup();
    }

    return 0;
}

/*
 * void set_session_gucs( char * session_gucs )
 *     Set the current session's GUCs based on stored values in the
 *     session_gucs JSON
 *
 * Arguments:
 *     char * session_gucs: JSON structure of key (GUC name) and value
 *                          (GUC value) pairs used to set the GUC in a
 *                          new session.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to parse JSON
 *     - Emits error on invalid input JSON (ARRAY, SCALAR)
 *     - Emits error on failure to set GUC via SQL commands.
 *
 */
void set_session_gucs( char * session_gucs )
{
    PGresult *  result           = NULL;
    jsmntok_t * json_tokens      = NULL;
    jsmntok_t   json_key_token   = {0};
    jsmntok_t   json_value_token = {0};
    char *      key              = NULL;
    char *      value            = NULL;
    char *      params[2]        = {NULL};
    int         i                = 0;
    int         max_tokens       = 0;

    if( session_gucs == NULL || strlen( session_gucs ) == 0 )
    {
        return;
    }

    json_tokens = json_tokenise( session_gucs, &max_tokens );

    if( json_tokens == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to parse session GUC strings"
        );
        return;
    }

    if( json_tokens[0].type != JSMN_OBJECT )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Root element of session GUCs structure is not an object"
        );

        free( json_tokens );
        return;
    }

    if( max_tokens < 3 )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Received empty JSON object for session_gucs"
        );
        free( json_tokens );
        return;
    }

    i = 1;
    for(;;)
    {
        json_key_token = json_tokens[i];

        if( json_key_token.type != JSMN_STRING )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Expected string key in JSON structure for session_gucs "
                "(got %d at index %d)",
                json_key_token.type,
                i
            );

            free( json_tokens );
            return;
        }

        key = ( char * ) calloc(
            ( json_key_token.end - json_key_token.start + 1 ),
            sizeof( char )
        );

        if( key == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for JSON key string"
            );

            free( json_tokens );
            return;
        }

        strncat(
            key,
            ( char * ) ( session_gucs + json_key_token.start ),
            json_key_token.end - json_key_token.start
        );

        key[json_key_token.end - json_key_token.start] = '\0';
        i++;

        if( i >= max_tokens )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Reached unexpected end of JSON object"
            );
            free( key );
            free( json_tokens );
            return;
        }

        json_value_token = json_tokens[i];

        value = ( char * ) calloc(
            ( json_value_token.end - json_value_token.start + 1 ),
            sizeof( char )
        );

        if( value == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for JSON value string"
            );

            free( key );
            free( json_tokens );
            return;
        }

        strncpy(
            value,
            ( char * ) ( session_gucs + json_value_token.start ),
            json_value_token.end - json_value_token.start
        );

        value[json_value_token.end - json_value_token.start] = '\0';

        if( strcmp( value, "null" ) == 0 || strcmp( value, "NULL" ) == 0 )
        {
            free( value );
            value = NULL;
        }

        params[0] = key;
        params[1] = value;
        result = _execute_query(
            ( char * ) set_guc,
            params,
            2
        );

        if( result == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to execute set_guc query"
            );

            _rollback_transaction();
            free( key );
            if( value != NULL )
            {
                free( value );
            }
            free( json_tokens );
            return;
        }

        PQclear( result );
        _log( LOG_LEVEL_DEBUG, "Found session_guc kv pair: %s:%s", key, value );
        free( key );
        if( value != NULL )
        {
            free( value );
        }

        if( i >= ( max_tokens - 1 ) )
        {
            break;
        }

        i++;
    }

    free( json_tokens );
    return;
}

/*
 * void clear_session_gucs( char * session_gucs )
 *     Clears the GUC names present in the session_guc JSON, returning the
 *     session to a base state.
 *
 * Arguments:
 *     char * session_gucs: JSON object containing key (GUC names) and value
 *                          (GUC value) pairs used to clear the GUCs.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to parse JSON.
 *     - Emits error on receipt of invalid JSON structure (ARRAY,SCALAR).
 *     - Emits error on failure to clear GUC via SQL commands.
 */

void clear_session_gucs( char * session_gucs )
{
    PGresult *  result           = NULL;
    jsmntok_t * json_tokens      = NULL;
    jsmntok_t   json_key_token   = {0};
    char *      key              = NULL;
    char *      params[1]        = {NULL};
    int         i                = 0;
    int         max_tokens       = 0;

    if( session_gucs == NULL || strlen( session_gucs ) == 0 )
    {
        return;
    }

    json_tokens = json_tokenise( session_gucs, &max_tokens );

    if( json_tokens == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to parse session GUC strings"
        );
        return;
    }

    if( json_tokens[0].type != JSMN_OBJECT )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Root element of session GUCs structure is not an object"
        );

        free( json_tokens );
        return;
    }

    if( max_tokens < 3 )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Received empty JSON object for session_gucs"
        );
        free( json_tokens );
        return;
    }

    i = 1;
    for(;;)
    {
        json_key_token = json_tokens[i];

        if( json_key_token.type != JSMN_STRING )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Expected string key in JSON structure for session_gucs"
                " (got %d at index %d)",
                json_key_token.type,
                i
            );

            free( json_tokens );
            return;
        }

        key = ( char * ) calloc(
            ( json_key_token.end - json_key_token.start + 1 ),
            sizeof( char )
        );

        if( key == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for JSON key string"
            );

            free( json_tokens );
            return;
        }

        strncat(
            key,
            ( char * ) ( session_gucs + json_key_token.start ),
            json_key_token.end - json_key_token.start
        );

        key[json_key_token.end - json_key_token.start] = '\0';
        i = i + 2;

        params[0] = key;
        _log(
            LOG_LEVEL_DEBUG,
            "Clearing GUC %s",
            key
        );

        result = _execute_query(
            ( char * ) clear_guc,
            params,
            1
        );

        if( result == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to execute set_guc query"
            );
            free( json_tokens );
            free( key );
            _rollback_transaction();
            return;
        }

        PQclear( result );
        free( key );

        if( i >= ( max_tokens - 1 ) )
        {
            break;
        }

    }

    free( json_tokens );
    return;
}
