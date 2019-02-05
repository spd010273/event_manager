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
#include "event_manager.h"

// Global Variables
char * ext_schema          = NULL;
bool   cyanaudit_installed = false;

/*
 * PGresult * _execute_query( struct worker * me, char * query, char ** params, int param_count )
 *     Executes a given query. Has handlers present for:
 *         DB connection interruptions
 *         SQL command termination by administrator
 *         Error handling
 *
 * Arguments:
 *     - struct worker * me: Structure containing DB handle
 *     - char * query:       SQL query string to execute.
 *     - char ** params:     Optional parameter list to be bound into the query
 *     - int param_count:    Length of above structure.
 * Return:
 *     PGresult * result: Result handle of the executed query.
 * Error Conditions:
 *     - Returns NULL on error.
 *     - Emits error on failure to execute query.
 *     - Emits error on disconnection of DB handle.
 *     - Emits error on syntax or improper termination of query.
 */

PGresult * _execute_query( struct worker * me, char * query, char ** params, int param_count )
{
    PGresult * result            = NULL;
    int        retry_counter     = 0;
    int        last_backoff_time = 0;
    char *     last_sql_state    = NULL;
#ifdef DEBUG
    int        i = 0;
#endif

    if( me->conn == NULL )
    {
        if( me->tx_in_progress )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Connection handle empty in transaction"
            );
            return NULL;
        }

        me->conn = PQconnectdb( conninfo );
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
            _log( LOG_LEVEL_DEBUG, "%d (bindpoint $%d): %s", i, i+1, params[i] );
        }
    }
#endif

    // Attempt to execute the query on our handle
    while(
            PQstatus( me->conn ) != CONNECTION_OK &&
            retry_counter < MAX_CONN_RETRIES
         )
    {

        if( me->tx_in_progress )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to connect to DB server (%s), while in a transaction. "
                "Transaction was automatically aborted",
                PQerrorMessage( me->conn )
            );

            me->tx_in_progress = false;
            return NULL;
        }

        _log(
            LOG_LEVEL_WARNING,
            "Failed to connect to DB server (%s). Retrying...",
            PQerrorMessage( me->conn )
        );

        _log(
            LOG_LEVEL_DEBUG,
            "Conninfo is: %s",
            conninfo
        );

        retry_counter++;
        // Randomly increment the backoff counter to prevent constant polling
        // of a database that may be in recovery
        last_backoff_time = (int) ( 10 * ( rand() / RAND_MAX ) )
                          + last_backoff_time;

        if( me->conn != NULL )
        {
            _log(
                LOG_LEVEL_DEBUG,
                "Backoff time is %d",
                last_backoff_time
            );

            PQfinish( me->conn );
            me->conn = NULL;
        }

        sleep( last_backoff_time );
        me->conn = PQconnectdb( conninfo );
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
            result = PQexec( me->conn, query );
        }
        else
        {
            result = PQexecParams(
                me->conn,
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
                PQerrorMessage( me->conn )
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
 * void _queue_loop( struct worker * me )
 *     Listens to the specified channel for asynchronous notifications, calling
 *     the dequeue_function when a new queue item is present.
 *
 * Arguments:
 *     - struct worker * me: Struct containing DB handle
 * Return:
 *     None
 * Error conditions:
 *     - Exits program on failure to allocate string memory.
 *     - Emits error when listen channel cannot be bound with select().
 *     - Emits error when a SIGTERM is received.
 */
void _queue_loop( struct worker * me )
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

    if( single_step_only )
    {
        _log( LOG_LEVEL_DEBUG, "Single stepping dequeue function." );
        me->dequeue_function( me );
        return;
    }

    while( me->dequeue_function( me ) > 0 )
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
        ( strlen( me->channel ) + 10 ),
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
    strcat( listen_command, ( const char * ) me->channel );
    strcat( listen_command, "\"\0" );

    listen_result = _execute_query(
        me,
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
        sock = PQsocket( me->conn );

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

        PQconsumeInput( me->conn );

        while( ( notify = PQnotifies( me->conn ) ) != NULL )
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
            while( me->dequeue_function( me ) > 0 )
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

        if( single_step_only )
        {
            _log(
                LOG_LEVEL_INFO,
                "exiting after single stepping..."
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
 * int event_queue_handle( struct worker * me )
 *     Handles new entries in event_manager.tb_event_queue.
 *
 * Arguments:
 *     struct worker * me:  Struct containing DB handle
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
int event_queue_handler( struct worker * me )
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

    if( !_begin_transaction( me ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start event dequeue transaction"
        );

        return 0;
    }

    result = _execute_query(
        me,
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

        _rollback_transaction( me );
        return 0;
    }

    if( PQntuples( result ) <= 0 )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Event queue processor received spurious NOTIFY"
        );

        _rollback_transaction( me );
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

    set_session_gucs( me, session_values );
    work_item_query_obj = _new_query( work_item_query );

    _add_parameter_to_query( work_item_query_obj, "event_table_work_item", event_table_work_item );
    _add_parameter_to_query( work_item_query_obj, "uid",                   uid                   );
    _add_parameter_to_query( work_item_query_obj, "op",                    op                    );
    _add_parameter_to_query( work_item_query_obj, "pk_value",              pk_value              );
    _add_parameter_to_query( work_item_query_obj, "recorded",              recorded              );

    _add_json_parameter_to_query( work_item_query_obj, old,           "OLD."           );
    _add_json_parameter_to_query( work_item_query_obj, new,           "NEW."           );
    _add_json_parameter_to_query( work_item_query_obj, session_values, ( char * ) NULL );

    _finalize_query( work_item_query_obj );

    if( work_item_query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "regex replace operation on work_item_query failed"
        );
        _rollback_transaction( me );
        PQclear( result );
        return 0;
    }

    _log( LOG_LEVEL_DEBUG, "WORK ITEM QUERY: " );
    _debug_struct( work_item_query_obj );
    work_item_result = _execute_query(
        me,
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
        _rollback_transaction( me );
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
            me,
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
            _rollback_transaction( me );
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
        me,
        ( char * ) delete_event_queue_item,
        params,
        9
    );

    // Clear GUCs prior to freeing result handle
    clear_session_gucs( me, session_values );
    PQclear( result );
    PQclear( work_item_result );

    if( delete_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to dequeue event queue item"
        );
        _rollback_transaction( me );
        return 0;
    }

    PQclear( delete_result );

    if( _commit_transaction( me ) == false )
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
 * int work_queue_handler( struct worker * me )
 *     Handles new entries in event_manager.tb_event_queue
 *
 * Arguments:
 *     struct worker * me:  Struct containing DB handle
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
int work_queue_handler( struct worker * me )
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
    if( !_begin_transaction( me ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start transaction"
        );
        return 0;
    }

    result = _execute_query(
        me,
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

        _rollback_transaction( me );
        return 0;
    }

    /* Handle action execution */
    row_count = PQntuples( result );

    if( row_count == 0 )
    {
        _rollback_transaction( me );
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

        action_result = execute_action( me, result, i );

        if( action_result == false )
        {
            PQclear( result );
            _rollback_transaction( me );
            return 0;
        }

        /* Flush queue item */
        delete_result = _execute_query(
            me,
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
            _rollback_transaction( me );
            return 0;
        }

        PQclear( delete_result );
    }

    PQclear( result );

    if( _commit_transaction( me ) == false )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit work queue transaction: %s",
            PQerrorMessage( me->conn )
        );

        _rollback_transaction( me );
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

    _log(
        LOG_LEVEL_DEBUG,
        "Writer callback called, resized response buffer to: %lu",
        real_size
    );
    return real_size;
}

/*
 * bool execute_remote_uri_call( struct worker * me, struct action_result * )
 *     Uses CuRL to execute a remote POST, PUT, or GET request over HTTP/HTTPS
 *
 * Arguments:
 *     struct worker * me:            Struct containing DB handle
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
bool execute_remote_uri_call( struct worker * me, struct action_result * action )
{
    struct curl_response write_buffer = {0};
    CURLcode             response     = {0};
    char *               remote_call  = NULL;
    char *               param_list   = NULL;
    unsigned int         malloc_size  = 2;

    if( action == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "cannot execute remote API call on NULL action_result handle"
        );

        return false;
    }

    // Replace any bindpoints that may exist in the uri prior to appending a parameter list
    _bind_uri_arguments( &(action->uri), action->parameters, NULL );

    // Append parameter list
    param_list = ( char * ) calloc( malloc_size, sizeof( char ) );

    if( param_list == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Unable to allocate memory for parameters"
        );

        return false;
    }

    if( strcmp( action->method, "GET" ) == 0 || strcmp( action->method, "PUT" ) == 0 )
    {
        strcpy( param_list, "?" );
    }

    param_list = _add_json_parameters_to_param_list(
        me->curl_handle,
        param_list,
        action->parameters,
        &malloc_size
    );

    if( param_list == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to add JSON parameters to param list"
        );
        return false;
    }

    if( action->static_parameters != NULL )
    {
        malloc_size++;
        param_list = ( char * ) realloc( param_list, malloc_size );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Unable to allocate memory for simple string "
                " concatenation operation :("
            );
            return false;
        }

        strcat( param_list, "&" );

        param_list = _add_json_parameters_to_param_list(
            me->curl_handle,
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

        param_list = _add_json_parameters_to_param_list(
            me->curl_handle,
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

    if( !(me->enable_curl ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Could not make remote API call: %s, curl is disabled",
            action->uri
        );

        if( param_list != NULL )
        {
            free( param_list );
            param_list = NULL;
        }

        return false;
    }

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
        response = curl_easy_setopt( me->curl_handle, CURLOPT_HTTPGET, 1L );
    }
    else if( strcmp( action->method, "PUT" ) == 0 )
    {
        _log( LOG_LEVEL_DEBUG, "Setting PUT method" );
        // CURLOPT_PUT is deprecated
        // TODO: Set the Content-type appropriately and the server ///should/// accept
        // POSTFIELDS for a PUT as per REST standard, but libcurl has deparecated
        // CURLOPT_PUT, so we use CUSTOMREQUEST.
        //
        // Right now, we're just hijacking GET logic to send our parameters, otherwise
        // the curl call for PUTs will deadlock and hang, as we are not actually uploading
        // a file. And the timeout doesn't seem to work either :)
        response = curl_easy_setopt( me->curl_handle, CURLOPT_CUSTOMREQUEST, "PUT" );
    }
    else if( strcmp( action->method, "POST" ) == 0 )
    {
        _log( LOG_LEVEL_DEBUG, "Setting POST method" );
        response = curl_easy_setopt( me->curl_handle, CURLOPT_POST, 1L );
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

    response = curl_easy_setopt( me->curl_handle, CURLOPT_TIMEOUT, API_CALL_TIMEOUT );

    if( response != CURLE_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to set curl TIMEOUT opt %s",
            curl_easy_strerror( response )
        );
        return false;
    }

    // Initialize buffer
    write_buffer.pointer = malloc( 1 );

    if( write_buffer.pointer == NULL )
    {
        //Really? You dont have 1 byte?
        _log( LOG_LEVEL_ERROR, "Failed to allocate memory for write buffer" );
        return false;
    }

    write_buffer.size    = 0;

    if( strcmp( action->method, "GET" ) == 0 || strcmp( action->method, "PUT" ) == 0 )
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
            me->curl_handle,
            CURLOPT_POSTFIELDS,
            param_list
        );
    }

    if( action->use_ssl )
    {
        response = curl_easy_setopt(
            me->curl_handle,
            CURLOPT_USE_SSL,
            CURLUSESSL_TRY
        );
    }

    response = curl_easy_setopt( me->curl_handle, CURLOPT_URL,           remote_call              );
    response = curl_easy_setopt( me->curl_handle, CURLOPT_WRITEFUNCTION, _curl_write_callback     );
    response = curl_easy_setopt( me->curl_handle, CURLOPT_WRITEDATA,     ( void * ) &write_buffer );

    if( response == CURLE_OK )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Making %s call with param list %s",
            action->method,
            param_list
        );
        response = curl_easy_perform( me->curl_handle );
        _log( LOG_LEVEL_DEBUG, "Call finished, parsing response" );
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

        if(
              strcmp( action->method, "GET" ) == 0
           || strcmp( action->method, "PUT" ) == 0
          )
        {
            if( remote_call != NULL )
            {
                free( remote_call );
                remote_call = NULL;
            }
        }

        return false;
    }

    _log(
        LOG_LEVEL_DEBUG,
        "Got response: '%s'",
        write_buffer.pointer
    );

    free( write_buffer.pointer );

    if(
            strcmp( action->method, "GET" ) == 0
         || strcmp( action->method, "PUT" ) == 0
      )
    {
        if( remote_call != NULL )
        {
            free( remote_call );
            remote_call = NULL;
        }
    }

    return true;
}

/*
 * bool execute_action_query( struct worker * me, struct action_result * )
 *     executes an action query
 *
 * Arguments:
 *     struct worker * me:     Struct containing DB handle
 *     struct action_result *: All available information related to the action to
 *                             be executed.
 * Return:
 *     bool is_success:        true if the transaction completed successfully,
 *                             false otherwise.
 * Error Conditions:
 *     - Emit error on failure to allocate string memory.
 *     - Emit error on transaction failure
 */
bool execute_action_query( struct worker * me, struct action_result * action )
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

    set_session_gucs( me, action->session_values );
    _add_parameter_to_query( action_query, "uid",                action->uid               );
    _add_parameter_to_query( action_query, "recorded",           action->recorded          );
    _add_parameter_to_query( action_query,  "transaction_label", action->transaction_label );

    _log( LOG_LEVEL_DEBUG, "PARAMS: %s", action->parameters );

    _add_json_parameter_to_query( action_query, action->parameters,        ( char * ) NULL );
    _add_json_parameter_to_query( action_query, action->static_parameters, ( char * ) NULL );
    _add_json_parameter_to_query( action_query, action->session_values,    ( char * ) NULL );

    _finalize_query( action_query );

    // Set UID
    set_uid( me, action->uid, action->session_values );

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
        me,
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

    clear_session_gucs( me, action->session_values );
    PQclear( action_result );
    return true;
}

/*
 * bool execute_action( struct worker * me, PGresult * result, int row )
 *     Wrapper for processing work_queue items and dispatching them to either
 *     the URI or query execution subroutines.
 *
 * Arguments:
 *     - struct worker * me: Struct containing DB handle
 *     - PGresult * result:  Dequeued work queue entry.
 *     - int row:            Row index of the work queue entry.
 * Return:
 *     bool is_success:      true indicates successful completion of the action,
 *                           false otherwise.
 * Error Conditions
 *     - Emits error on inability to allocate string memory.
 *     - Emits error from URI or query subroutines upon failure.
 */
bool execute_action( struct worker * me, PGresult * result, int row )
{
    bool                   execute_action_result = false;
    struct action_result   action                = {0};
    struct action_result * action_ptr            = NULL;
    char *                 use_ssl               = NULL;
    char *                 uri                   = NULL; // Copy string

    action_ptr            = &action;
    action.parameters     = get_column_value( row, result, "parameters"     );
    action.uid            = get_column_value( row, result, "uid"            );
    action.recorded       = get_column_value( row, result, "recorded"       );
    action.session_values = get_column_value( row, result, "session_values" );

    uri = get_column_value( row, result, "uri" );

    if( uri == NULL || is_column_null( row, result, "uri" ) )
    {
        action.uri = NULL;
    }
    else
    {
        action.uri = ( char * ) calloc(
            strlen( uri ) + 1,
            sizeof( char )
        );

        strcpy( action.uri, uri );
    }

    if( is_column_null( row, result, "static_parameters" ) == false )
    {
        action.static_parameters = get_column_value(
            row,
            result,
            "static_parameters"
        );
    }

    action.transaction_label = get_column_value( row, result, "transaction_label" );

    action.method = get_column_value( row, result, "method"  );
    action.query  = get_column_value( row, result, "query"   );
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

        execute_action_result = execute_action_query( me, action_ptr );

        if( execute_action_result == true && cyanaudit_installed == true )
        {
            _cyanaudit_integration( me, action.transaction_label );
        }
    }
    else if( is_column_null( 0, result, "uri" ) == false )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Executing API call"
        );

        execute_action_result = execute_remote_uri_call( me, action_ptr );
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

    free( action.uri );
    return execute_action_result;
}

/*
 * void _cyanaudit_integration( struct worker * me, char * transaction_label )
 *     Labels the completed transaction in CyanAudit, if present.
 *
 * Arguments:
 *     struct worker * me:  Struct containing DB handle
 *     char * transaction_label: Label with which to identify transaction.
 * Return:
 *     None
 * Error conditions:
 *     Emits error on failure to make a call to
 *     cyanaudit.fn_label_last_transaction().
 */
void _cyanaudit_integration( struct worker * me, char * transaction_label )
{
    PGresult * cyanaudit_result = NULL;
    char *     param[1]         = {NULL};

    param[0] = transaction_label;

    cyanaudit_result = _execute_query(
        me,
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
 * bool set_uid( struct worker * me, char * uid, char * session_values )
 *     Makes a call to the function specified in event_manager.set_uid_function,
 *     binding in the uid to ?uid? and the originating transaction GUC values
 *     specified in event_manager.session_gucs to their respective names.
 *
 * Arguments:
 *     - struct worker * me:    Struct containing DB handle
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
bool set_uid( struct worker * me, char * uid, char * session_values )
{
    PGresult *     uid_function_result = NULL;
    struct query * set_uid_query_obj   = NULL;

    char * params[1]         = {NULL};
    char * uid_function_name = NULL;
    char * set_uid_query     = NULL;

    params[0] = SET_UID_GUC_NAME;

    uid_function_result = _execute_query(
        me,
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
        me,
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
    PGresult *       result           = NULL;
    PGresult *       cyanaudit_result = NULL;
    char *           params[1]        = {NULL};
    unsigned int     tid              = 0;
    int              random_ind       = 4; // determined by dice roll
    int              row_count        = 0;

    _parse_args( argc, argv );

    if( !parent_init( argc, argv ) )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Could not allocate parent process memory"
        );
    }

    //Crapily seed PRNG for backoff of connection attempts on DB failure
    srand( random_ind * time(0) );

    params[0] = EXTENSION_NAME;

    if( conninfo == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Invalid arguments!"
        );
    }

    result = _execute_query(
        parent,
        ( char * ) extension_check_query,
        params,
        1
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Extension check failed: %s",
            PQerrorMessage( parent->conn )
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
        parent,
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

    if( parent->conn != NULL )
    {
        PQfinish( parent->conn );
        parent->conn = NULL;
    }

    // Entry for other subs here
    // Spawn
    _log(
        LOG_LEVEL_DEBUG,
        "Spawning %d workers (E: %d, W: %d)",
        event_jobs + work_jobs,
        event_jobs,
        work_jobs
    );

    for( tid = 0; tid < event_jobs; tid++ )
    {
        _log( LOG_LEVEL_DEBUG, "EL: %d", tid );
        new_worker( WORKER_TYPE_EVENT_PROCESSOR, tid, &_queue_loop_wrapper, argc, argv );
    }

    for( tid = event_jobs; tid < ( work_jobs + event_jobs ); tid++ )
    {
        _log( LOG_LEVEL_DEBUG, "WL: %d", tid );
        new_worker( WORKER_TYPE_WORK_PROCESSOR, tid, &_queue_loop_wrapper, argc, argv);
    }

    _manage_children( &_queue_loop_wrapper, argc, argv );

    return 0;
}

/*
 * void set_session_gucs( struct worker * me, char * session_gucs )
 *     Set the current session's GUCs based on stored values in the
 *     session_gucs JSON
 *
 * Arguments:
 *     struct worker * me:  Struct containing DB handle
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
void set_session_gucs( struct worker * me, char * session_gucs )
{
    PGresult *   result           = NULL;
    jsmntok_t *  json_tokens      = NULL;
    jsmntok_t    json_key_token   = {0};
    jsmntok_t    json_value_token = {0};
    char *       key              = NULL;
    char *       value            = NULL;
    char *       params[2]        = {NULL};
    unsigned int i                = 0;
    unsigned int max_tokens       = 0;

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
            me,
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

            _rollback_transaction( me );
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
 * void clear_session_gucs( struct worker * me, char * session_gucs )
 *     Clears the GUC names present in the session_guc JSON, returning the
 *     session to a base state.
 *
 * Arguments:
 *     struct worker * me:  Struct containing DB handle
 *     char * session_gucs: JSON object containing key (GUC names) and value
 *                          (GUC value) pairs used to clear the GUCs.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to parse JSON.
 *     - Emits error on receipt of invalid JSON structure (ARRAY,SCALAR).
 *     - Emits error on failure to clear GUC via SQL commands.
 */

void clear_session_gucs( struct worker * me, char * session_gucs )
{
    PGresult *   result         = NULL;
    jsmntok_t *  json_tokens    = NULL;
    jsmntok_t    json_key_token = {0};
    char *       key            = NULL;
    char *       params[1]      = {NULL};
    unsigned int i              = 0;
    unsigned int max_tokens     = 0;

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
            me,
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
            _rollback_transaction( me );
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

/*
 * void _queue_loop_wrapper( void * data )
 *     Wraps the queue loop function. Handles child process entry by
 *     initializing handles, DB connection, and process state
 *
 * Arguments:
 *     void * data: a struct worker * ( from the workers[] array) cast to void *
 * Return:
 *     None.
 * Error Conditions:
 *     - Emits error on failure to initialize DB connection
 *     - Emits error on failure to initialize CURL handle
 *     - Emits error on invalid argument
 */
void _queue_loop_wrapper( void * data )
{
    struct worker * me        = NULL;
    PGresult *      conn_test = NULL;

    _log( LOG_LEVEL_DEBUG, "Pid %d got data %p", getpid(), data );

    if( data == NULL )
    {
        _log( LOG_LEVEL_FATAL, "ERROR, Process %d started with empty pid table slice", getpid() );
        exit( 1 );
    }

    me = ( struct worker * ) data;

    if( me->pid != getpid() )
    {
        // Wat
        _log(
            LOG_LEVEL_FATAL,
            "PID mismatch: %d received workers[] entry belonging to %d.",
            getpid(),
            me->pid
        );
    }

    // Finish setting up private scope
    if( me->type == WORKER_TYPE_EVENT_PROCESSOR )
    {
        me->dequeue_function = &event_queue_handler;
        me->channel          = EVENT_QUEUE_CHANNEL;
    }
    else if( me->type == WORKER_TYPE_WORK_PROCESSOR )
    {
        me->dequeue_function = &work_queue_handler;
        me->channel          = WORK_QUEUE_CHANNEL;
        me->curl_handle      = curl_easy_init();

        if( me->curl_handle != NULL  )
        {
            me->enable_curl = true;
            curl_easy_setopt( me->curl_handle, CURLOPT_NOSIGNAL, 1  );
            curl_easy_setopt(
                me->curl_handle,
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

            me->enable_curl = false;
        }
    }
    else
    {
        _log(
            LOG_LEVEL_ERROR,
            "cannot run dequeue loop without a dequeue function:"\
            "invalid process type: %d",
            me->type
        );

        return;
    }

    // Initialize DB connection
    conn_test = _execute_query(
        me,
        "SELECT 1",
        NULL,
        0
    );

    if( conn_test == NULL || me->conn == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Failed to initialize DB connection"
        );

        return;
    }

    PQclear( conn_test );

    // Start main loop
    me->status = STATUS_WORKING;
    _queue_loop( me );

    // We should not get here but ehh
    me->status = STATUS_DEAD;
    exit( 0 );
}
