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
bool   enable_stats        = false;

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

static PGresult * _execute_query(
    struct worker * me,
    char *          query,
    char **         params,
    int             param_count
)
{
    PGresult *   result              = NULL;
    char *       last_sql_state      = NULL;
    char *       temp_last_sql_state = NULL;
    unsigned int retry_counter       = 0;
    unsigned int last_backoff_time   = 1;
    unsigned int i                   = 0;

    if( me == NULL )
    {
        return NULL;
    }

    if( me->conn == NULL )
    {
        if( !db_connect( me ) )
        {
            _log( LOG_LEVEL_ERROR, "Failed to reconnect" );
            return NULL;
        }
        _log( LOG_LEVEL_DEBUG, "Process %d reconnected to DB", getpid() );
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
#endif // DEBUG
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
        last_backoff_time = (int) ( 10 * ( ( double ) rand() / ( double ) RAND_MAX ) )
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
        db_connect( me );
    }

    _log(
        LOG_LEVEL_DEBUG,
        "Connection OK"
    );

    if( me->type == WORKER_TYPE_PARENT )
        _get_advisory_lock( me );

    while(
             (
                 last_sql_state == NULL // No state (first pass)
              || strncmp(
                     last_sql_state,
                     SQL_STATE_TERMINATED_BY_ADMINISTRATOR,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CANCELED_BY_ADMINISTRATOR,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CONNECTION_FAILURE,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CONNECTION_DOES_NOT_EXIST,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CONNECTION_EXCEPTION,
                     strlen( last_sql_state )
                 ) == 0
             )
          && retry_counter < MAX_CONN_RETRIES
         )
    {
        /* Handle case where we passed the first connection check but the connection
         * was interrupted mid-transaction. In this case, our transaction is aborted
         * but we still want to re-establish a connection
         */
        if(
               last_sql_state != NULL
            && (
                  strncmp(
                     last_sql_state,
                     SQL_STATE_CONNECTION_FAILURE,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CONNECTION_DOES_NOT_EXIST,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CONNECTION_EXCEPTION,
                     strlen( last_sql_state )
                 ) == 0
               )
          )
        {
            // Connection interrupted
            if( me->tx_in_progress == true )
            {
                me->tx_in_progress = false;
            }

            if( me->conn != NULL )
                PQfinish( me->conn );

            me->conn = NULL;
            db_connect( me );

            last_backoff_time = 1;
            return NULL;
        }

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
               result != NULL
            && !(
                     PQresultStatus( result ) == PGRES_COMMAND_OK
                  || PQresultStatus( result ) == PGRES_TUPLES_OK
                )
          )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Query '%s' failed: %s",
                query,
                PQerrorMessage( me->conn )
            );

            temp_last_sql_state = PQresultErrorField(
                result,
                PG_DIAG_SQLSTATE
            );

            if( last_sql_state != NULL )
            {
                free( last_sql_state );
                last_sql_state = NULL;
            }

            last_sql_state = ( char * ) calloc(
                sizeof( char ),
                strlen( temp_last_sql_state ) + 1
            );

            if( last_sql_state == NULL )
            {
                _log(
                    LOG_LEVEL_ERROR,
                    "Failed to allocate string for SQL state"
                );
            }

            strncpy(
                last_sql_state,
                temp_last_sql_state,
                strlen( temp_last_sql_state )
            );

            last_sql_state[ strlen( temp_last_sql_state ) - 1] = '\0';
            if( result != NULL )
            {
                PQclear( result );
            }

            retry_counter++;
            sleep( last_backoff_time );
            last_backoff_time = (int) ( 10 * ( ( double ) rand() / ( double ) RAND_MAX ) )
                              + last_backoff_time;
        }
        else
        {
            return result;
        }
    }

    if(
           retry_counter == 1
        && last_sql_state != NULL
        && (
                 strncmp(
                     last_sql_state,
                     SQL_STATE_TERMINATED_BY_ADMINISTRATOR,
                     strlen( last_sql_state )
                 ) == 0
              || strncmp(
                     last_sql_state,
                     SQL_STATE_CANCELED_BY_ADMINISTRATOR,
                     strlen( last_sql_state )
                 ) == 0
           )
      )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Query failed with state %s",
            last_sql_state
        );
    }
    else
    {
        _log(
            LOG_LEVEL_ERROR,
            "Query failed after %d tries with final state %s",
            retry_counter,
            last_sql_state
        );
    }

    if( param_count > 0 )
    {
        for( i = 0; i < param_count; i++ )
        {
            _log(
                LOG_LEVEL_ERROR,
                "param[%d]: '%s'",
                i,
                params[i]
            );
        }
    }

    if( last_sql_state != NULL )
    {
        free( last_sql_state );
        last_sql_state = NULL;
    }

    return NULL;
}

static bool db_connect( struct worker * me )
{
    unsigned short retry_counter     = 0;
    unsigned int   last_backoff_time = 0;

    if( me->conn != NULL )
    {
        if( PQstatus( me->conn ) != CONNECTION_OK )
        {
            PQfinish( me->conn );
            me->conn           = NULL;
            me->tx_in_progress = false;
        }
        else
        {
            _get_advisory_lock( me ); // It doesn't hurt to keep getting advisory locks
            return true;
        }
    }

    me->conn = PQconnectdb( conninfo );

    while(
              me->conn != NULL
           && PQstatus( me->conn ) != CONNECTION_OK
           && retry_counter < MAX_CONN_RETRIES
         )
    {
        sleep( last_backoff_time );
        last_backoff_time = (unsigned int) ( 10 * ( ( double ) rand() / ( double ) RAND_MAX ) )
                          + last_backoff_time;
        if( me->conn != NULL )
            PQfinish( me->conn );

        me->conn = NULL;
        me->conn = PQconnectdb( conninfo );
        retry_counter++;
    }

    if( me->conn != NULL && PQstatus( me->conn  ) == CONNECTION_OK )
    {
        _get_advisory_lock( me );
        _set_application_name ( me );

        return true;
    }

    return false;
}

/*
 * bool _begin_transaction( struct worker * )
 *     Begins a SQL transaction, sets the global tx state flag in the process.
 *
 * Arguments:
 *     struct worker * me: PID slot of the process starting the transaction
 * Return:
 *     bool is_success: Indicates that the transaction was successfully begun.
 * Error Conditions:
 *     Emits error on failure to start transaction (one is already in progress.)
 */
static bool _begin_transaction( struct worker * me )
{
    PGresult * result = NULL;

    if( me == NULL )
    {
        return false;
    }

    if( me->tx_in_progress )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempt to issue BEGIN when a transaction is already in progress"
        );
        return false;
    }

    if( !db_connect( me ) )
    {
        return false;
    }

    result = PQexec(
        me->conn,
        "BEGIN"
    );

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start transaction: %s",
            PQerrorMessage( me->conn )
        );

        PQclear( result );
        return false;
    }

    PQclear( result );
    me->tx_in_progress = true;
    me->tx_start = time( NULL );
    return true;
}

/*
 * bool _commit_transaction( struct worker * )
 *     Commits a SQL transaction.
 *
 * Arguments:
 *    struct worker * me: PID slot of process commiting the transaction
 * Return:
 *    bool is_success: true indicates that the transaction was successfully
 *                     committed.
 * Error Conditions:
 *    Emits error on failure to commit transaction.
 */
static bool _commit_transaction( struct worker * me )
{
    PGresult * result = NULL;

    if( me == NULL )
    {
        return false;
    }

    if( !( me->tx_in_progress ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempted to issue COMMIT when not transaction was in progress"
        );
        return false;
    }

    if( !db_connect( me ) )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Failed to extablish database connection. All in-progress "\
            "transactions in this process were automatically aborted"
        );
        return false;
    }

    if( me->tx_in_progress == false )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Conenction to database was interrupted before it could be committed."\
            "The transaction was automatically aborted."
        );
        return false;
    }

    result = PQexec(
        me->conn,
        "COMMIT"
    );

    // Transaction will be either successfully commited or enter an aborted state
    // Regardless, it is completed
    me->tx_in_progress = false;

    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit transaction %s",
            PQerrorMessage( me->conn )
        );
        PQclear( result );
        return false;
    }

    PQclear( result );

    _update_stats(
        me,
        1,
        0,
        difftime( time( NULL ), me->tx_start )
    );

    me->tx_start = 0;

    return true;
}

/*
 * bool _rollback_transaction( struct worker * )
 *     rolls back a SQL transaction
 *
 * Arguments:
 *     struct worker * me: PID slot of the process rolling the transaction back
 * Return:
 *     bool is_success: true indicates the transaction was successfully rolled back
 * Error Conditions:
 *     Emits error on failure to rollback transaction
 */
static bool _rollback_transaction( struct worker * me )
{
    PGresult * result = NULL;

    if( me == NULL )
    {
        return false;
    }

    if( !( me->tx_in_progress ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempted to issue ROLLBACK when no transaction was in progress"
        );
        return false;
    }

    if( !db_connect( me ) )
    {
        return false;
    }

    if( me->tx_in_progress == false )
    {
        return false;
    }

    result = PQexec(
        me->conn,
        "ROLLBACK"
    );

    me->tx_in_progress = false;
    if( PQresultStatus( result ) != PGRES_COMMAND_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to rollback transaction: %s",
            PQerrorMessage( me->conn )
        );
        PQclear( result );
        return false;
    }

    PQclear( result );

    _update_stats(
        me,
        0,
        1,
        difftime( time( NULL ), me->tx_start )
    );

    me->tx_start = 0;
    return true;
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
static void _queue_loop( struct worker * me )
{
    PGnotify *     notify          = NULL;
    char *         listen_command  = NULL;
#ifdef ALLOW_QUEUE_CHECK_WITH_GUC
    PGresult *     qc_result       = NULL;
    char *         disable_queue   = NULL;
    bool           dq_bool_result  = false;
#endif // ALLOW_QUEUE_CHECK_WITH_GUC
    PGresult *     listen_result   = NULL;
    int            processed_count = 0;
    int            dequeue_result  = 0;
    int            select_rv       = 0;
    double         heartbeat_delta = 0.0;
    struct timeval timeout         = {0};

    // Check queue prior to entering main loop
    _log(
        LOG_LEVEL_DEBUG,
        "Processing queue entries prior to entering main loop"
    );

    if( single_step_only )
    {
#ifdef ALLOW_CONFIG_MANAGER
        if( me->type == WORKER_TYPE_CONFIG_MANAGER )
            return;
#endif // ALLOW_CONFIG_MANAGER
        _log( LOG_LEVEL_DEBUG, "Single stepping dequeue function." );
        me->dequeue_function( me );
        return;
    }

    // Dequeue result will be -1 for error, 0 for empty queue, 1 for success
#ifdef ALLOW_CONFIG_MANAGER
    if( me->type != WORKER_TYPE_CONFIG_MANAGER )
    {
#endif // ALLOW_CONFIG_MANAGER
#ifdef ALLOW_QUEUE_CHECK_WITH_GUC
        if( me->type == WORKER_TYPE_WORK_PROCESSOR )
        {
            _log( LOG_LEVEL_DEBUG, "WORK processor checking queue GUC" );
            qc_result = _execute_query(
                me,
                ( char * ) check_work_queue_guc,
                NULL,
                0
            );
        }
        else if( me->type == WORKER_TYPE_EVENT_PROCESSOR )
        {
            _log( LOG_LEVEL_DEBUG, "EVENT processor checking queue GUC" );
            qc_result = _execute_query(
                me,
                ( char * ) check_event_queue_guc,
                NULL,
                0
            );
        }

        if( qc_result != NULL )
        {
            disable_queue = get_column_value( 0, qc_result, "value" );
            if(
                    disable_queue != NULL
                 && (
                         strncmp( disable_queue, "t", 1 ) == 0
                      || strncmp( disable_queue, "T", 1 ) == 0
                    )
              )
            {
                dq_bool_result = true;
            }

            _log(
                LOG_LEVEL_DEBUG,
                "Queue for %s is %s (raw result %s)",
                me == NULL ? "NULL" :
                me->type == WORKER_TYPE_EVENT_PROCESSOR ? "EVENT" :
                me->type == WORKER_TYPE_WORK_PROCESSOR ? "WORK" :
                me->type == WORKER_TYPE_PARENT ? "PARENT" :
                "Unknown",
                dq_bool_result ? "DISABLED" : "ENABLED",
                disable_queue
            );

            PQclear( qc_result );
            disable_queue = NULL;
        }


        if( dq_bool_result == false )
        {
#endif // ALLOW_QUEUE_CHECK_WITH_GUC
            dequeue_result = me->dequeue_function( me );

            while( dequeue_result != 0 )
            {
                if( dequeue_result > 0 )
                {
                    processed_count++;
                }

                dequeue_result = me->dequeue_function( me );
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
#ifdef ALLOW_QUEUE_CHECK_WITH_GUC
        }
#endif // ALLOW_QUEUE_CHECK_WITH_GUC
#ifdef ALLOW_CONFIG_MANAGER
    }
#endif // ALLOW_CONFIG_MANAGER

    listen_command  = ( char * ) calloc(
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
    strncpy( listen_command, "LISTEN \"", 8 );
    strncat(
        listen_command,
        ( const char * ) me->channel,
        strlen( me->channel )
    );
    strncat( listen_command, "\"\0", 2 );

    listen_result = _execute_query(
        me,
        listen_command,
        NULL,
        0
    );

    free( listen_command );

    if( listen_result == NULL )
        return;

    PQclear( listen_result );

    while( 1 )
    {
#ifdef BLOCKING_SELECT
        sigset_t signal_set;
#endif // BLOCKING_SELECT
        int sock;
        fd_set input_mask;

        // Set the timeout here - select() clears it if timeout is hit
        timeout.tv_sec  = ( time_t ) SELECT_TIMEOUT_SECONDS;
        timeout.tv_usec = ( suseconds_t ) 0;

        if( got_sigterm )
            __term();

        if( got_sighup )
        {
            _child_handle_sighup();
            break;
        }

#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
        me = get_worker_by_pid();

        // See if we've been pruned
        if( me->commanded_shutdown )
            __term();
        /*
           check for updated workers[] pointer
           Note this is really sensitive to SIGSEGV - do not dynamically
           allocate the workers[] array without a better shm implementation
           - otherwise newly allocated and shared ram will be out of scope
           for processes post fork.
        */
        if( me->commanded_refresh )
            _child_update_pointers();
#endif // ALLOW_OVERRIDE_WORKER_COUNTS

        // Parent is dead! Long live SystemD!
        heartbeat_delta = difftime( time( NULL ), me->last_heartbeat );

        if( heartbeat_delta > MAX_HEARTBEAT_DURATION )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Haven't heard from parent in %f seconds, exiting...",
                heartbeat_delta
            );
            __term();
        }

#ifdef BLOCKING_SELECT
        sigaddset( &signal_set, SIGTERM );
        sigaddset( &signal_set, SIGINT );
        sigaddset( &signal_set, SIGHUP );
#endif // BLOCKING_SELECT
        sock = PQsocket( me->conn );

        if( sock < 0 )
            break;

        errno = 0;
        FD_ZERO( &input_mask );
        FD_SET( sock, &input_mask );
#ifdef BLOCKING_SELECT
        sigprocmask( SIG_BLOCK, &signal_set, NULL );
#endif // BLOCKING_SELECT
        // Note: from select(2) - hitting the timeout can return 0
        // Also, we cannot examine the timeout struct post run - it should
        // be treated as undefined due to various kernel implementations
        select_rv = select( sock + 1, &input_mask, NULL, NULL, &timeout );

        if( select_rv < 0 )
        { // ERROR STATE
#ifdef BLOCKING_SELECT
            sigprocmask( SIG_UNBLOCK, &signal_set, NULL );
#endif // BLOCKING_SELECT
            if( !got_sighup && errno == 4 ) // Interrupted System Call
            {
                _log(
                    LOG_LEVEL_WARNING,
                    "select() failed: %s",
                    strerror( errno )
                );
                return;
            }
            else if( got_sighup )
            {
                _child_handle_sighup();
                return;
            }
        }

        if( select_rv == 0 )
        { // TIMEOUT STATE
            // Timout reached or got a notify with no descriptor change
            // the latter /shouldnt/ happen
#ifdef BLOCKING_SELECT
            sigprocmask( SIGUNBLOCK, &signal_set, NULL );
#endif // BLOCKING_SELECT

            if( got_sighup )
            {
                _child_handle_sighup();
                return;
            }

            continue;
        }
        
        // NORMAL STATE
        select_rv = 0;
#ifdef BLOCKING_SELECT
        sigprocmask( SIG_UNBLOCK, &signal_set, NULL );
#endif // BLOCKING_SELECT
        errno = 0;

        // We will get dumped here on SIGHUP, and need to re-enter the
        // _queue_loop function to re-establish all handles
        if( me->conn != NULL )
        {
            _log(
                LOG_LEVEL_DEBUG,
                "(%s) Handling notify",
                me->type == WORKER_TYPE_WORK_PROCESSOR ? "work queue" :
                me->type == WORKER_TYPE_EVENT_PROCESSOR ? "event queue" :
#ifdef ALLOW_CONFIG_MANAGER
                me->type == WORKER_TYPE_CONFIG_MANAGER ? "config" :
#endif // ALLOW_CONFIG_MANAGER
                "INVALID"
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

                    if( got_sighup )
                    {
                        _child_handle_sighup();
                        return;
                    }
                }

                _log(
                    LOG_LEVEL_DEBUG,
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
                _log( LOG_LEVEL_DEBUG, "Child %d breaking loop after SIGTERM", getpid() );
                __term();
            }

            if( got_sighup )
            {
                _child_handle_sighup();
                return;
            }

            if( single_step_only )
            {
                _log(
                    LOG_LEVEL_INFO,
                    "exiting after single stepping..."
                );
                return;
            }
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
static int event_queue_handler( struct worker * me )
{
    PGresult *            result                 = NULL;
    PGresult *            work_item_result       = NULL;
    PGresult *            delete_result          = NULL;
    PGresult *            insert_result          = NULL;
    PGresult *            update_result          = NULL;
    struct query *        work_item_query_obj    = NULL;
    char *                uid                    = NULL;
    char *                recorded               = NULL;
    char *                transaction_label      = NULL;
    char *                execute_asynchronously = NULL;
    char *                action                 = NULL;
    char *                work_item_query        = NULL;
    char *                pk_value               = NULL;
    char *                op                     = NULL;
    char *                ctid                   = NULL;
    char *                event_table_work_item  = NULL;
    char *                old                    = NULL;
    char *                new                    = NULL;
    char *                session_values         = NULL;
    char *                parameters             = NULL;
    char *                params[9]              = {NULL};
    register unsigned int i                      = 0;

    if( !_begin_transaction( me ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to start event dequeue transaction"
        );

        if( me->tx_in_progress == true )
        {
            me->tx_in_progress = false;
        }

        return -1;
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
        return -1;
    }

    if( PQntuples( result ) <= 0 )
    {
        // This is not useful, especially with > 1 worker on the queue, as all
        // workers receive the notify but only one wins the dequeue race.
        // This is especially egregious when the queue is empty
        _log(
            LOG_LEVEL_DEBUG,
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
    _add_parameter_to_query( work_item_query_obj, "NEW",                   new                   );
    _add_parameter_to_query( work_item_query_obj, "OLD",                   old                   );

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
        return -1;
    }

#ifdef DEBUG
    _log( LOG_LEVEL_DEBUG, "WORK ITEM QUERY: " );
    _debug_struct( work_item_query_obj );
#endif // DEBUG

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

        params[0] = event_table_work_item;
        params[1] = uid;
        params[2] = recorded;
        params[3] = pk_value;
        params[4] = op;
        params[5] = old;
        params[6] = new;
        params[7] = session_values;
        params[8] = ctid;
        _rollback_transaction( me );
        // XXX There is a small race condition here - this item ~may~ be picked up by another worker
        // as the transaction is aborted, and we've explicitly rolled it back
        if( _begin_transaction( me ) )
        {
            update_result = _execute_query(
                me,
                ( char * ) update_event_queue_item_failed,
                params,
                9
            );

            if( update_result != NULL && _commit_transaction( me ) )
            {
                _log(
                    LOG_LEVEL_DEBUG,
                    "Successfully marked event item as failed"
                );
            }
            else
            {
                _log(
                    LOG_LEVEL_WARNING,
                    "Failed to mark failed event item"
                );
            }
            PQclear( update_result );
        }
        else
        {
            _log(
                LOG_LEVEL_WARNING,
                "Failed to begin transaction for failure marking of event item"
            );

            if( me->tx_in_progress == true )
            {
                me->tx_in_progress = false;
            }
        }

        PQclear( result );
        return -1;
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
            return -1;
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
        return -1;
    }

    PQclear( delete_result );

    if( _commit_transaction( me ) == false )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to commit event queue transaction"
        );

        return -1;
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
static int work_queue_handler( struct worker * me )
{
    PGresult *            result        = NULL;
    PGresult *            delete_result = NULL;
    PGresult *            update_result = NULL;
    bool                  action_result = false;
    int                   row_count     = 0;
    char *                params[7]     = {NULL};
    register unsigned int i             = 0;

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

        if( me->tx_in_progress == true )
        {
            me->tx_in_progress = false;
        }

        return -1;
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
        return -1;
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
            _rollback_transaction( me );

            if( _begin_transaction( me ) )
            {
                update_result = _execute_query(
                    me,
                    ( char * ) update_work_queue_item_failed,
                    params,
                    7
                );

                if( update_result != NULL && _commit_transaction( me ) )
                {
                    _log(
                        LOG_LEVEL_DEBUG,
                        "Marked work queue item as failed"
                    );
                }
                else
                {
                    _log(
                        LOG_LEVEL_WARNING,
                        "Failed to mark work queue item as failed"
                    );
                }
            }
            else
            {
                _log(
                    LOG_LEVEL_WARNING,
                    "Failed to start transaction for marking work queue item as failed"
                );

                if( me->tx_in_progress == true )
                {
                    me->tx_in_progress = false;
                }
            }
            PQclear( result );

            return -1;
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
            return -1;
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

#ifdef ALLOW_CONFIG_MANAGER
/*
 * static void _config_manager_loop( void * data )
 *     sleep loop that waits for config updates. Allows event manager to get
 *     SIGHUP'ed from the database whenever configuration settings are changed.
 *
 * Arguments:
 *     void * data: a struct worker * cast to void. This is expected to be of
 *                  the Config Manager type
 * Returns:
 *     none
 * Error Conditions:
 *
 */
static int _config_manager_loop( struct worker * me )
{
    // Validation boilerplate, make sure the right process in in this sub
    _log(
        LOG_LEVEL_DEBUG,
        "Config manager handling remote SIGHUP with pid %d, data %p",
        getpid(),
        me
    );

    if( me == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Invalid PID slot provided to config loop"
        );
    }

    if( me->pid != getpid() )
    {
        _log(
            LOG_LEVEL_FATAL,
            "PID mismatch: %d received workers entry belonging to %d",
            getpid(),
            me->pid
        );
    }

    if( me->type != WORKER_TYPE_CONFIG_MANAGER )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Worker of type %d entered config manager routine",
            me->type
        );
    }

    if( parent == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Parent PID slice is NULL"
        );
    }

    kill( parent->pid, SIGHUP );
    return 0;
}
#endif // ALLOW_CONFIG_MANAGER

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
static char * get_column_value( int row, PGresult * result, char * column_name )
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
static bool is_column_null( int row, PGresult * result, char * column_name )
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
static bool execute_remote_uri_call( struct worker * me, struct action_result * action )
{
    struct curl_response write_buffer  = {0};
    CURLcode             response      = {0};
    char *               remote_call   = NULL;
    char *               param_list    = NULL;
    unsigned int         malloc_size   = 2;
    long int             response_code = 0;
    unsigned short       retry_count   = 0;
    unsigned short       method_len    = 0;

    if( action == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "cannot execute remote API call on NULL action_result handle"
        );

        return false;
    }

#ifdef REDEF_CURL_HANDLE
    me->curl_handle = curl_easy_init();

    if( me->curl_handle != NULL )
    {
        me->enable_curl = true;
        curl_easy_setopt( me->curl_handle, CURLOPT_NOSIGNAL, 1 );
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
            "JIT cURL initialization failed. Aborting URI call." \
            " cURL will be disabled for this worker"
        );
        me->enable_curl = false;
        return false;
    }
#endif // REDEF_CURL_HANDLE

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

#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    method_len = strlen( action->method );
    if(
        strncmp( action->method, "GET", MIN( method_len, 3 ) ) == 0
     || strncmp( action->method, "PUT", MIN( method_len, 3 ) ) == 0
      )
    {
        strncpy( param_list, "?", 1 );
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
#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
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
#ifdef REDEF_CURL_HANDLE
            curl_easy_cleanup( me->curl_handle );
            me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
            return false;
        }

        strncat( param_list, "&", 1 );

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
#ifdef REDEF_CURL_HANDLE
            curl_easy_cleanup( me->curl_handle );
            me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
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
#ifdef REDEF_CURL_HANDLE
            curl_easy_cleanup( me->curl_handle );
            me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
            return false;
        }

        strncat( param_list, "&", 1 );

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
#ifdef REDEF_CURL_HANDLE
            curl_easy_cleanup( me->curl_handle );
            me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
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

    if( strncmp( action->method, "GET", MIN( method_len, 3 ) ) == 0 )
    {
        _log( LOG_LEVEL_DEBUG, "Setting GET method" );
        response = curl_easy_setopt( me->curl_handle, CURLOPT_HTTPGET, 1L );
    }
    else if( strncmp( action->method, "PUT", MIN( method_len, 3 ) ) == 0 )
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
    else if( strncmp( action->method, "POST", MIN( method_len, 4 ) ) == 0 )
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

#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    if( response != CURLE_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to set curl method: %s",
            curl_easy_strerror( response )
        );
#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    // Initialize buffer
    write_buffer.pointer = calloc( 1, sizeof( char ) );

    if( write_buffer.pointer == NULL )
    {
        //Really? You dont have 1 byte?
        _log( LOG_LEVEL_ERROR, "Failed to allocate memory for write buffer" );
#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    write_buffer.size = 0;

    if(
          strncmp( action->method, "GET", MIN( method_len, 3 ) ) == 0
       || strncmp( action->method, "PUT", MIN( method_len, 3 ) ) == 0
      )
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
#ifdef REDEF_CURL_HANDLE
            curl_easy_cleanup( me->curl_handle );
            me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
            return false;
        }

        strncpy( remote_call, action->uri, strlen( action->uri ) );
        strncat( remote_call, param_list, strlen( param_list ) );

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

    response = curl_easy_setopt( me->curl_handle, CURLOPT_URL,            remote_call              );
    response = curl_easy_setopt( me->curl_handle, CURLOPT_WRITEFUNCTION,  _curl_write_callback     );
    response = curl_easy_setopt( me->curl_handle, CURLOPT_WRITEDATA,      ( void * ) &write_buffer );
    response = curl_easy_setopt( me->curl_handle, CURLOPT_CONNECTTIMEOUT, CURL_CONNECT_TIMEOUT     );
    response = curl_easy_setopt( me->curl_handle, CURLOPT_TIMEOUT,        CURL_TIMEOUT             );

#ifdef DEBUG
    response = curl_easy_setopt( me->curl_handle, CURLOPT_VERBOSE, 1L );
#endif

    if( response != CURLE_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to set cURLopts"
        );
#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    _log(
        LOG_LEVEL_DEBUG,
        "Making %s call with param list %s",
        action->method,
        param_list
    );

    HTTP_RETRY:

    response = curl_easy_perform( me->curl_handle );
    _log( LOG_LEVEL_DEBUG, "Call finished, parsing response" );

    if( param_list != NULL )
    {
        free( param_list );
        param_list = NULL;
    }

    curl_easy_getinfo(
        me->curl_handle,
        CURLINFO_RESPONSE_CODE,
        &response_code
    );

    _log( LOG_LEVEL_DEBUG, "Got HTTP %d", (int) response_code );

    if( response != CURLE_OK )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed %s %s: %s (%s)",
            action->method,
            remote_call,
            curl_easy_strerror( response ),
            write_buffer.pointer == NULL ?
                "no additional information" :
                write_buffer.pointer
        );

        if(
                response == CURLE_OPERATION_TIMEDOUT
             && retry_count < TIMEOUT_RETRY_LIMIT
          )
        {
            _log( LOG_LEVEL_DEBUG, "Request failed with timeout, retrying..." );
            sleep( RETRY_BACKOFF );
            retry_count++;
            goto HTTP_RETRY;
        }


        // We should probably retry before this point as we'll need the buffer for response writeback
        if( write_buffer.pointer != NULL )
        {
            free( write_buffer.pointer );
        }

        if(
              strncmp( action->method, "GET", MIN( method_len, 3 ) ) == 0
           || strncmp( action->method, "PUT", MIN( method_len, 3 ) ) == 0
          )
        {
            if( remote_call != NULL )
            {
                free( remote_call );
                remote_call = NULL;
            }
        }

#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    if(
          ( response_code / 100 ) == 4
       || ( response_code / 100 ) == 5
      ) // We got a 4XX or 5XX HTTP error
    {
        _log(
            LOG_LEVEL_ERROR,
            "HTTP Request failed with code %d\n %s %s: %s (%s)",
            ( int ) response_code,
            action->method,
            remote_call,
            curl_easy_strerror( response ),
            write_buffer.pointer == NULL ?
                "No additional information" :
                write_buffer.pointer
        );

        // We could probably put a special handler for HTTP timeouts
#ifdef REDEF_CURL_HANDLE
        curl_easy_cleanup( me->curl_handle );
        me->curl_handle = NULL;
#endif // REDEF_CURL_HANDLE
        return false;
    }

    _log(
        LOG_LEVEL_DEBUG,
        "Got response: '%s'",
        write_buffer.pointer
    );

    if( retry_count > 0 )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "URI call succeeded after %d tries",
            ( int ) retry_count
        );
    }

    free( write_buffer.pointer );

    if(
            strncmp( action->method, "GET", MIN( method_len, 3 ) ) == 0
         || strncmp( action->method, "PUT", MIN( method_len, 3 ) ) == 0
      )
    {
        if( remote_call != NULL )
        {
            free( remote_call );
            remote_call = NULL;
        }
    }

#ifdef REDEF_CURL_HANDLE
    curl_easy_cleanup( me->curl_handle );
    me->curl_handle = NULL;
    // curl_global_cleanup();
#endif // REDEF_CURL_HANDLE
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
static bool execute_action_query( struct worker * me, struct action_result * action )
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

#ifdef DEBUG
    _log( LOG_LEVEL_DEBUG, "ACTION QUERY: " );
    _debug_struct( action_query );
#endif // DEBUG

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

        _rollback_transaction( me );
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
static bool execute_action( struct worker * me, PGresult * result, int row )
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

        strncpy( action.uri, uri, strlen( uri ) );
        action.uri[strlen(uri)] = '\0';
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

    if( strncmp( use_ssl, "t", 1 ) == 0 || strncmp( use_ssl, "T", 1 ) == 0 )
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
static void _cyanaudit_integration( struct worker * me, char * transaction_label )
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
static bool set_uid( struct worker * me, char * uid, char * session_values )
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

    strncpy( set_uid_query, "SELECT ", 7 );
    strncat(
        set_uid_query,
        uid_function_name,
        strlen( uid_function_name )
    );

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
    PGresult *            result           = NULL;
    PGresult *            cyanaudit_result = NULL;
    char *                params[2]        = {NULL};
    register unsigned int tid              = 0;
    int                   random_ind       = 4; // determined by dice roll
    int                   row_count        = 0;
    time_t                last_stat_update = 0;
    struct em_stat *      stats[2]         = {NULL};

    _parse_args( argc, argv );

    if( !parent_init( argc, argv ) )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Failed to initialize event_manager"
        );
    }

    //Crapily seed PRNG for backoff of connection attempts on DB failure
    // and mutex acquisition
    srand( random_ind * time(0) );

    params[0] = EXTENSION_NAME;
    params[1] = VERSION;

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
        2
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
            "Extension check failed. Is version %s of %s installed?",
            VERSION,
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

    if( cyanaudit_result != NULL )
    {
        PQclear( cyanaudit_result );
    }

    result = _execute_query(
        parent,
        ( char * ) test_stat_table,
        NULL,
        0
    );

    if(
           result != NULL
        && PQntuples( result ) > 0
      )
    {
        enable_stats = true;
    }
    else
    {
        _log(
            LOG_LEVEL_INFO,
            "Statistics collection is disabled - extension not up-to-date"
        );
    }

    if( result != NULL )
    {
        PQclear( result );
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
        new_worker(
            WORKER_TYPE_EVENT_PROCESSOR,
            tid,
            &_queue_loop_wrapper,
            argc,
            argv,
            NULL
        );
    }

    for( tid = event_jobs; tid < ( work_jobs + event_jobs ); tid++ )
    {
        _log( LOG_LEVEL_DEBUG, "WL: %d", tid );
        new_worker(
            WORKER_TYPE_WORK_PROCESSOR,
            tid,
            &_queue_loop_wrapper,
            argc,
            argv,
            NULL
        );
    }

#ifdef ALLOW_CONFIG_MANAGER
    _log( LOG_LEVEL_DEBUG, "Starting config manager" );
    new_worker(
        WORKER_TYPE_CONFIG_MANAGER,
        0,
        &_queue_loop_wrapper,
        argc,
        argv,
        NULL
    );
#endif // ALLOW_CONFIG_MANAGER

    last_stat_update = time( NULL );

    while( 1 )
    {
        // Main loop for parent
        if(
                enable_stats
             && difftime(
                    time( NULL ),
                    last_stat_update
                ) > STAT_UPDATE_INTERVAL
          )
        {
            _log( LOG_LEVEL_DEBUG, "Updating stats..." );
            _gather_and_update_stats( parent, stats );
            last_stat_update = time( NULL );
        }

        sleep( 10 );
#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
        _get_child_counts_from_db();

        if(
              ( override_event_jobs != 0 && override_event_jobs != event_jobs )
           || ( override_work_jobs != 0 && override_work_jobs != work_jobs )
          )
        {
            _resize_pid_table( &_queue_loop_wrapper );
            override_event_jobs = 0;
            override_work_jobs  = 0;
        }
#endif // ALLOW_OVERRIDE_WORKER_COUNTS
        _manage_children( &_queue_loop_wrapper );
    }

    return 0;
}

#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
static void _get_child_counts_from_db()
{ //XXX
    PGresult *      worker_count_result = NULL;
    char *          worker_count        = NULL;
    struct worker * me                  = NULL;
    unsigned int    temp                = 0;

    me = get_worker_by_pid();

    if( me == NULL || me->type != WORKER_TYPE_PARENT )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Illegal entry into _get_child_counts_from_db"
        );
        return;
    }

    if( me->conn == NULL )
    {
        db_connect( me );
    }

    worker_count_result = _execute_query(
        me,
        ( char * ) get_work_processor_count,
        NULL,
        0
    );

    if( worker_count_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to check for override work queue processor counts"
        );
        return;
    }

    if( PQntuples( worker_count_result ) > 0 )
    {
        worker_count = get_column_value(
            0,
            worker_count_result,
            "work_count"
        );

        if( worker_count != NULL )
        {
            temp = atoi( worker_count );

            if( temp > 0 )
                override_work_jobs = temp;
            else
                _log( LOG_LEVEL_DEBUG, "invalid count %d", temp );
        }
        else
        {
            _log( LOG_LEVEL_DEBUG, "NULL response for WC" );
        }
    }
    else
    {
        _log( LOG_LEVEL_DEBUG, "Getting override work count got 0 rows" );
    }

    PQclear( worker_count_result );

    worker_count_result = _execute_query(
        me,
        ( char * ) get_event_processor_count,
        NULL,
        0
    );

    if( worker_count_result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to check for override event queue processor counts"
        );
        return;
    }

    if( PQntuples( worker_count_result ) > 0 )
    {
        worker_count = get_column_value(
            0,
            worker_count_result,
            "event_count"
        );

        if( worker_count != NULL )
        {
            temp = atoi( worker_count );

            if( temp > 0 )
                override_event_jobs = temp;
            else
                _log( LOG_LEVEL_DEBUG, "invalid count %d", temp );
        }
        else
        {
            _log( LOG_LEVEL_DEBUG, "NULL response for WC" );
        }
    }
    else
    {
        _log( LOG_LEVEL_DEBUG, "Getting override event count got 0 rows" );
    }

    PQclear( worker_count_result );
    PQfinish( me->conn );
    me->conn = NULL;
    return;
}
#endif // ALLOW_OVERRIDE_WORKER_COUNTS

static void _gather_and_update_stats( struct worker * me, struct em_stat ** stats )
{
    PGresult * stat_update         = NULL;
    char *     params[4]           = {NULL};
    char       tx_fail_buff[64]    = {0};
    char       tx_success_buff[64] = {0};
    char       tx_duration_buff[7] = {0};

    if( me == NULL ||  stats == NULL )
    {
        return;
    }

    if( stats[0] == NULL )
    {
        stats[0] = calloc(
            sizeof( struct em_stat ),
            1
        );
    }

    if( stats[1] == NULL )
    {
        stats[1] = calloc(
            sizeof( struct em_stat ),
            1
        );
    }

    if( stats[0] == NULL || stats[1] == NULL )
    {
        _log( LOG_LEVEL_ERROR, "Failed to allocate statistics rollup" );

        if( stats[0] != NULL )
        {
            free( stats[0] );
            stats[0] = NULL;
        }

        if( stats[1] != NULL )
        {
            free( stats[1] );
            stats[1] = NULL;
        }

        return;
    }

    _gather_child_stats_to_self( stats );

    // Dont perform the update if there's nothing _to_ update
    if(
           stats[0] != NULL
        && (
                stats[0]->tx_success  != 0
             || stats[0]->tx_fail     != 0
             || stats[0]->tx_duration != 0.0
           )
      )
    {
        db_connect( me );

        snprintf( tx_success_buff, 64, "%u", stats[0]->tx_success );
        snprintf( tx_fail_buff, 64, "%u", stats[0]->tx_fail );

        params[0] = tx_success_buff;
        params[1] = tx_fail_buff;
        params[2] = gcvt( stats[0]->tx_duration, 6, tx_duration_buff );
        params[3] = WORKER_TITLE_EVENT_PROCESSOR;

        stat_update = _execute_query(
            me,
            ( char * ) insert_stat_rollup,
            params,
            4
        );

        if( stat_update == NULL )
        {
            _log(
                LOG_LEVEL_DEBUG,
                "Failed to update event processor stats"
            );

            return;
        }

        free( stats[0] );
        stats[0] = NULL;
        PQclear( stat_update );
    }

    // Dont perform the update if there's nothing _to_ update
    if(
           stats[1] != NULL
        && (
               stats[1]->tx_success  != 0
            || stats[1]->tx_fail     != 0
            || stats[1]->tx_duration != 0.0
           )
      )
    {
        db_connect( me );

        snprintf( tx_success_buff, 64, "%u", stats[1]->tx_success );
        snprintf( tx_fail_buff, 64, "%u", stats[1]->tx_fail );

        params[0] = tx_success_buff;
        params[1] = tx_fail_buff;
        params[2] = gcvt( stats[1]->tx_duration, 6, tx_duration_buff );
        params[3] = WORKER_TITLE_WORK_PROCESSOR;

        stat_update = _execute_query(
            me,
            ( char * ) insert_stat_rollup,
            params,
            4
        );

        if( stat_update == NULL )
        {
            _log(
                LOG_LEVEL_DEBUG,
                "Failed to update work processor stats"
            );
            return;
        }

        free( stats[1] );
        stats[1] = NULL;
        PQclear( stat_update );
    }

    _log(
        LOG_LEVEL_DEBUG,
        "Stats updated"
    );

    return;
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
static void set_session_gucs( struct worker * me, char * session_gucs )
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

        if(
                strncmp( value, "null", MIN( strlen( value ), 4 ) ) == 0
             || strncmp( value, "NULL", MIN( strlen( value ), 4 ) ) == 0
          )
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

        _log(
            LOG_LEVEL_DEBUG,
            "Found session_guc kv pair: %s:%s",
            key,
            value
        );

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

static void clear_session_gucs( struct worker * me, char * session_gucs )
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
static void _queue_loop_wrapper( void * data )
{
    struct worker * me        = NULL;
    PGresult *      conn_test = NULL;

    _log( LOG_LEVEL_DEBUG, "Pid %d got data %p", getpid(), data );

    if( data == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "ERROR, Process %d started with empty pid table slice",
            getpid()
        );
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
#ifndef REDEF_CURL_HANDLE
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
#endif // REDEF_CURL_HANDLE
    }
#ifdef ALLOW_CONFIG_MANAGER
    else if( me->type == WORKER_TYPE_CONFIG_MANAGER )
    {
        me->channel          = CONFIG_MANAGER_CHANNEL;
        me->dequeue_function = &_config_manager_loop;
    }
#endif // ALLOW_CONFIG_MANAGER
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
    }

    PQclear( conn_test );
    _set_application_name( me );
    // Start main loop
    me->status = STATUS_WORKING;

    while( 1 )
    {
        if( me->conn == NULL )
        {
            db_connect( me );
        }

        _log(
            LOG_LEVEL_DEBUG,
            "Child (type %s) entering main loop",
            me == NULL ? "NULL" :
                me->type == WORKER_TYPE_PARENT ? "PARENT" :
                me->type == WORKER_TYPE_WORK_PROCESSOR ? "WORK" :
                me->type == WORKER_TYPE_EVENT_PROCESSOR ? "EVENT" :
#ifdef ALLOW_CONFIG_MANAGER
                me->type == WORKER_TYPE_CONFIG_MANAGER ? "CONFIG" :
#endif // ALLOW_CONFIG_MANAGER
                "Unknown"
        );
        _queue_loop( me );
        _log(
            LOG_LEVEL_DEBUG,
            "Child (type %s) escaped from main loop",
            me == NULL ? "NULL" :
                me->type == WORKER_TYPE_PARENT ? "PARENT" :
                me->type == WORKER_TYPE_WORK_PROCESSOR ? "WORK" :
                me->type == WORKER_TYPE_EVENT_PROCESSOR ? "EVENT" :
#ifdef ALLOW_CONFIG_MANAGER
                me->type == WORKER_TYPE_CONFIG_MANAGER ? "CONFIG" :
#endif // ALLOW_CONFIG_MANAGER
                "Unknown"
        );

        if( single_step_only )
        {
            break;
        }

        // Rebound from SIGHUPs, as that may interrupt the select() in
        // _queue_loop, which will dump us out here. Instead of calling
        // queue_loop recursively, we let it back out and call from here
    }

    // We should not get here but ehh
    me->status = STATUS_DEAD;
    exit( 0 );
}

static void _set_application_name( struct worker * me )
{
    char *       application_name_command = NULL;
    PGresult *   result                   = NULL;
    unsigned int malloc_size              = 0;

    if( me == NULL || me->conn == NULL )
    {
        return;
    }

    malloc_size = strlen( set_application_name ) + 2;

    if( me->type == WORKER_TYPE_PARENT )
    {
        malloc_size += strlen( WORKER_TITLE_PARENT );
    }
    else if( me->type == WORKER_TYPE_EVENT_PROCESSOR )
    {
        malloc_size += strlen( WORKER_TITLE_EVENT_PROCESSOR );
    }
    else if( me->type == WORKER_TYPE_WORK_PROCESSOR )
    {
        malloc_size += strlen( WORKER_TITLE_WORK_PROCESSOR );
    }
#ifdef ALLOW_CONFIG_MANAGER
    else if( me->type == WORKER_TYPE_CONFIG_MANAGER )
    {
        malloc_size += strlen( WORKER_TITLE_CONFIG_MANAGER );
    }
#endif // ALLOW_CONFIG_MANAGER
    else
    {
        return;
    }

    application_name_command = ( char * ) calloc(
        sizeof( char ),
        malloc_size
    );

    if( application_name_command == NULL )
    {
        return;
    }

    strncpy(
        application_name_command,
        set_application_name,
        strlen( set_application_name )
    );

    if( me->type == WORKER_TYPE_PARENT )
    {
        strncat(
            application_name_command,
            WORKER_TITLE_PARENT,
            strlen( WORKER_TITLE_PARENT )
        );
    }
    else if( me->type == WORKER_TYPE_EVENT_PROCESSOR )
    {
        strncat(
            application_name_command,
            WORKER_TITLE_EVENT_PROCESSOR,
            strlen( WORKER_TITLE_EVENT_PROCESSOR )
        );
    }
    else if( me->type == WORKER_TYPE_WORK_PROCESSOR )
    {
        strncat(
            application_name_command,
            WORKER_TITLE_WORK_PROCESSOR,
            strlen( WORKER_TITLE_WORK_PROCESSOR )
        );
    }
#ifdef ALLOW_CONFIG_MANAGER
    else if( me->type == WORKER_TYPE_CONFIG_MANAGER )
    {
        strncat(
            application_name_command,
            WORKER_TITLE_CONFIG_MANAGER,
            strlen( WORKER_TITLE_CONFIG_MANAGER )
        );
    }
#endif // ALLOW_CONFIG_MANAGER
    else
    {
        return;
    }

    strncat( application_name_command, "'\0", 2 );

    result = PQexec(
        me->conn,
        application_name_command
    );

    if(
            PQresultStatus( result ) != PGRES_COMMAND_OK
         && PQresultStatus( result ) != PGRES_TUPLES_OK
      )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Failed to set application_name"
        );
    }

    PQclear( result );
    free( application_name_command );
    return;
}

static bool _get_advisory_lock( struct worker * me )
{
    char *     params[2]   = {NULL};
    PGresult * result      = NULL;
    char       pid[64]     = {0};
    char *     adv_result  = NULL;

    if( me->conn == NULL )
    {
        return false;
    }

    if( me->type == WORKER_TYPE_WORK_PROCESSOR )
    {
        params[0] = "event_manager.tb_work_queue";
    }
    else if( me->type == WORKER_TYPE_EVENT_PROCESSOR )
    {
        params[0] = "event_manager.tb_event_queue";
    }
#ifdef ALLOW_CONFIG_MANAGER
    else if( me->type == WORKER_TYPE_CONFIG_MANAGER )
    {
        params[0] = "event_manager.tb_setting";
    }
#endif // ALLOW_CONFIG_MANAGER
    else if( me->type == WORKER_TYPE_PARENT )
    {
        parent_get_advisory_lock();
        return true;
    }
    else
    {
        return true;
    }

    snprintf( pid, 64, "%d", me->pid );

    params[1] = pid;

    result = PQexecParams(
        me->conn,
        pid_lock,
        2,
        NULL,
        ( const char * const * ) params,
        NULL,
        NULL,
        0
    );

    if(
            result != NULL
        && !(
                PQresultStatus( result ) == PGRES_COMMAND_OK
             || PQresultStatus( result ) == PGRES_TUPLES_OK
            )
      )
    {
        if( result != NULL )
        {
            PQclear( result );
        }

        _log( LOG_LEVEL_ERROR, "Failed to get advisory lock: %s", PQerrorMessage( me->conn ) );
        return false;
    }

    adv_result = get_column_value( 0, result, "result" );

    if( adv_result == NULL )
    {
        if( result != NULL )
        {
            PQclear( result );
        }

        return false;
    }

    if( strncmp( adv_result, "t", 1 ) == 0 || strncmp( adv_result, "T", 1 ) == 0 )
    {
        PQclear( result );
        return true;
    }

    PQclear( result );
    return false;
}

static bool parent_get_advisory_lock( void )
{
    struct worker * me           = NULL;
    PGresult *      result       = NULL;
    char *          lock_result  = NULL;

    _log( LOG_LEVEL_DEBUG, "PARENT ATTEMPTING TO GET ADVISORY LOCK" );
    me = get_worker_by_pid();

    if( me == NULL )
        return false;

    if( me->type != WORKER_TYPE_PARENT )
        return false;

    result = PQexecParams(
        me->conn,
        get_event_manager_running,
        0,
        NULL,
        NULL,
        NULL,
        NULL,
        0
    );

    if(
            result != NULL
        && !(
                PQresultStatus( result ) == PGRES_COMMAND_OK
             || PQresultStatus( result ) == PGRES_TUPLES_OK
            )
      )
    {
        if( result != NULL )
        {
            PQclear( result );
        }

        _log( LOG_LEVEL_FATAL, "Failed to get parent's advisory lock: %s", PQerrorMessage( me->conn ) );
        return false;
    }

    lock_result = get_column_value( 0, result, "result" );

    if( strncmp( lock_result, "t", 1 ) == 0 || strncmp( lock_result, "T", 1 ) == 0 )
    {
        PQclear( result );
        _log( LOG_LEVEL_DEBUG, "Parent (re)acquired advisory lock" );
        return true;
    }

    PQclear( result );

    _log( LOG_LEVEL_FATAL, "There appears to be another instance of event_manager running on this database" );
    __term();
}
