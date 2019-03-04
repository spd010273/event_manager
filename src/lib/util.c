/*------------------------------------------------------------------------
 *
 * util.c
 *     Utility and process management functions
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        util.c
 *
 *------------------------------------------------------------------------
 */

#include "util.h"

#define VERSION 0.1

struct worker ** workers          = NULL;
struct worker *  parent           = NULL;
char *           conninfo         = NULL;
bool             single_step_only = false;

extern char ** envorion; // Declared in unistd.h

unsigned int work_jobs     = 0;
unsigned int event_jobs    = 0;
unsigned int max_argv_size = 0;

// Flags
sig_atomic_t got_sighup  = false;
sig_atomic_t got_sigterm = false;
sig_atomic_t got_sigint  = false;

static const char * usage_string = "\
Usage: event_manager\n \
    -U DB User (default: postgres)\n \
    -p DB Port (default: 5432)\n \
    -h DB Host (default: localhost)\n \
    -d DB name (default: DB User)\n \
    -E worker_count: Number of Event Queue workers to spawn\n \
    -W worker_count: Number of Work Queue workers to spawn\n \
  [ -D debug mode\n \
    -S single step\n \
    -v VERSION\n \
    -? HELP ]\n";

/*
 * void _parse_args( int argc, char ** argv )
 *     Argument parser for main()
 *
 * Arguments:
 *     - int argc:     Count of arguments.
 *     - char ** argv: String array of CLI arguments.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to allocate string memory.
 *     - Emits error on receipt of invalid arguments.
 *     - Emits error on receipt of conflicting arguments.
 */
void _parse_args( int argc, char ** argv )
{
    int    c               = 0;
    char * username        = NULL;
    char * dbname          = NULL;
    char * port            = NULL;
    char * hostname        = NULL;
    char * event_job_count = NULL;
    char * work_job_count  = NULL;

    opterr = 0;

    while( ( c = getopt( argc, argv, "U:p:d:h:E:W:Sv?" ) ) != -1 )
    {
        switch( c )
        {
            case 'U':
                username = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'd':
                dbname = optarg;
                break;
            case 'h':
                hostname = optarg;
                break;
            case '?':
                _usage( NULL );
            case 'v':
                printf( "Event Manager, version %f\n", (float) VERSION );
                exit( 0 );
            case 'E':
                event_job_count = optarg;
                _log( LOG_LEVEL_DEBUG, "Got EJ: %s", event_job_count );
                break;
            case 'W':
                work_job_count = optarg;
                _log( LOG_LEVEL_DEBUG, "Got WJ: %s", work_job_count );
                break;
            case 'S':
                single_step_only = true;
                break;
            default:
                _usage( "Invalid argument." );
        }
    }

    if( event_job_count == NULL )
    {
        event_jobs = 0;
    }
    else
    {
        event_jobs = (unsigned int) atoi( event_job_count );

        if( event_jobs > MAX_WORKERS )
        {
            _log(
                LOG_LEVEL_WARNING,
                "Event Queue worker count %s out of bound, defaulting to 1",
                event_job_count
            );

            event_jobs = 1;
        }
    }

    if( work_job_count == NULL )
    {
        work_jobs = 0;
    }
    else
    {
        work_jobs = (unsigned int) atoi( work_job_count );

        if( work_jobs > MAX_WORKERS )
        {
            _log(
                LOG_LEVEL_WARNING,
                "Work Queue worker count %s out of bounds, defaulting to 1",
                work_job_count
            );

            work_jobs = 1;
        }
    }

    if( work_jobs == 0 && event_jobs == 0 )
    {
        _usage( "Must specify at least one event or one work processor" );
    }

    if( ( work_jobs > 1 || event_jobs > 1 ) && single_step_only )
    {
        _log(
            LOG_LEVEL_WARNING,
            "Forcing process counts to 1, it's suggested to run a single copy"\
            " of each queue processor while in single step mode"
        );

        if( work_jobs > 1 )
        {
            work_jobs = 1;
        }

        if( event_jobs > 1 )
        {
            event_jobs = 1;
        }
    }

    if( port == NULL )
        port = "5432";

    if( username == NULL )
        username = "postgres";

    if( hostname == NULL )
        hostname = "localhost";

    if( dbname == NULL )
        dbname = username;

    conninfo = ( char * ) calloc(
        (
            strlen( username ) +
            strlen( port ) +
            strlen( dbname ) +
            strlen( hostname ) +
            26
        ),
        sizeof( char )
    );

    if( conninfo == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Failed to allocate memory for connection string :("
        );
    }

    strcpy( conninfo, "user=" );
    strcat( conninfo, username );
    strcat( conninfo, " host=" );
    strcat( conninfo, hostname );
    strcat( conninfo, " port=" );
    strcat( conninfo, port );
    strcat( conninfo, " dbname=" );
    strcat( conninfo, dbname );

    _log(
        LOG_LEVEL_DEBUG,
        "Parsed args: %s",
        conninfo
    );

    return;
}

/*
 * void _usage( char * message )
 *     Emits basic usage and argument tips
 *
 * Arguments:
 *     char * message: Message containing tips
 *                     (directing user to fix arguments.)
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
void _usage( char * message )
{
    if( message != NULL )
    {
        printf( "%s\n", message );
    }

    printf( "%s", usage_string );

    exit( 1 );
}

/*
 * void _log( char * log_level, char * message, va_list )
 *     Custom logger implementing log levels:
 *        LOG_LEVEL_WARNING: Emitted on STDERR (non fatal)
 *        LOG_LEVEL_ERROR: Emitted on STDERR (non fatal)
 *        LOG_LEVEL_FATAL: Emitted on STDERR (fatal)
 *        LOG_LEVEL_DEBUG: Emitted on STDOUT (non fatal)
 *        LOG_LEVEL_INFO: Emitted on STDOUT (non fatal)
 *
 * Arguments:
 *     - char * log_level: Level at which to emit the log message.
 *     - char * message:   Message to emit.
 *     - va_list:          List of variable arguments which are to be
 *                         substituted into the message string
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
void _log( char * log_level, char * message, ... )
{
    va_list args = {{0}};
    FILE *  output_handle = NULL;

    if( message == NULL )
    {
        return;
    }

    va_start( args, message );

    if(
        strcmp( log_level, LOG_LEVEL_WARNING ) == 0 ||
        strcmp( log_level, LOG_LEVEL_ERROR ) == 0 ||
        strcmp( log_level, LOG_LEVEL_FATAL ) == 0
      )
    {
        output_handle = stderr;
    }
    else
    {
        output_handle = stdout;
    }

#ifndef DEBUG
    if( strcmp( log_level, LOG_LEVEL_DEBUG ) != 0 )
    {
#endif
        fprintf(
            output_handle,
            "%s: ",
            log_level
        );

        fprintf(
            output_handle,
            "(%d) ",
            getpid()
        );

        vfprintf(
            output_handle,
            message,
            args
        );

        fprintf(
            output_handle,
            "\n"
        );
#ifndef DEBUG
    }
#endif

    va_end( args );
    fflush( output_handle );

    if( strcmp( log_level, LOG_LEVEL_FATAL ) == 0 )
    {
        //free( conninfo );
        __term();
    }

    return;
}

/*
 * void free_worker( struct worker * worker )
 *     Deallocates memory used by a process for handles and objects
 *
 * Arguments:
 *     struct worker * worker: workers[] array slice for the child process
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
void free_worker( struct worker * worker )
{
    if( worker == NULL )
    {
        return;
    }

    if( worker->conn != NULL )
    {
        if( worker->tx_in_progress )
        {
            _rollback_transaction( worker );
        }

        PQfinish( worker->conn );
        worker->conn = NULL;
    }

    if( worker->curl_handle != NULL )
    {
        curl_easy_cleanup( worker->curl_handle );
        worker->curl_handle = NULL;
        //curl_global_cleanup();
    }

    munmap( worker, sizeof( struct worker ) );
    worker = NULL;
    return;
}

/*
 *  bool parent_init( void )
 *      Initial special (initial) call to new_worker for parent process
 *
 *   Arguments:
 *      None
 *   Return:
 *      true on success, false on error
 *   Error Conditions:
 *      Same failure scenarios as new_worker()
 */
bool parent_init( int argc, char ** argv )
{
    parent = new_worker( WORKER_TYPE_PARENT, 0, NULL, argc, argv, NULL );

    if( parent == NULL )
    {
        return false;
    }

    return true;
}

/*
 * struct worker * new_worker(
 *     unsigned short type,
 *     unsigned int id,
 *     void (*function)( void * )
 * )
 * Sets up workers[] array for parent process or spawns a worker process
 *
 * Arguments:
 *     unsigned_short type: Worker type, either:
 *              WORKER_TYPE_PARENT,
 *              WORKER_TYPE_WORK_PROCESSOR
 *           or WORKER_TYPE_EVENT_PROCESSOR
 *                          which determines what type of table entry is
 *                          created, and whether a fork() should happen.
 *     int id:              Array index for the process in the workers array
 *     void (*function)( void * ): Routine pointer to the function
 *                                 the forked child will run. This function
 *                                 is passed the child's workers[] entry
 *  Return:
 *     struct worker * worker - The worker structure of the process
 *                              (either parent, or forked child)
 *  Error Conditions:
 *      Emits error on invalid arguments, failure to allocate memory
 */
struct worker * new_worker(
    unsigned short  type,
    unsigned int    id,
    void (*function)( void * ),
    int             argc,
    char **         argv,
    struct worker * workerslot
)
{
    struct worker * result = NULL;
    pid_t           pid    = 0;
    size_t          size   = 0;

    // Iff workerslot is provided, we reuse that SHM space
    if( workerslot == NULL )
    {
        result = ( struct worker * ) create_shared_memory(
            sizeof( struct worker )
        );
    }
    else
    {
        result = workerslot;
    }

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Could not allocate memory for worker"
        );

        return NULL;
    }

    result->type           = type;
    result->tx_in_progress = false;
    result->enable_curl    = true;
    result->pid            = 0;
    result->conn           = NULL;
    result->curl_handle    = NULL;
    result->status         = STATUS_STARTUP;

    if(
            type != WORKER_TYPE_PARENT
         && type != WORKER_TYPE_WORK_PROCESSOR
         && type != WORKER_TYPE_EVENT_PROCESSOR
      )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Invalid worker type %d",
            type
        );

        free( result );
        return NULL;
    }

    if( type == WORKER_TYPE_PARENT )
    {
        result->pid = getpid();

        if( workers == NULL )
        {
            size = ( work_jobs + event_jobs ) * sizeof( struct worker * );
            _log(
                LOG_LEVEL_DEBUG,
                "Creating SHM with size %lu: WJ %d, EJ: %d",
                size,
                work_jobs,
                event_jobs
            );

            workers = ( struct worker ** ) create_shared_memory( size );

            if( workers == NULL )
            {
                _log(
                    LOG_LEVEL_FATAL,
                    "Could not allocate child process table"
                );
            }

            _log(
                LOG_LEVEL_DEBUG,
                "PID table allocated at %p", workers
            );
        }

        result->my_argv = argv;
        result->my_argc = argc;
        _log( LOG_LEVEL_DEBUG, "Parent argv: %p argc: %d", argv, argc );

        // Register signal handlers
        signal( SIGHUP, __sighup );
        signal( SIGTERM, __sigterm );
        signal( SIGINT, __sigint );
        _set_process_title( argv, argc, WORKER_TITLE_PARENT, &max_argv_size );
        return result;
    }

    _log( LOG_LEVEL_DEBUG, "Remapped PID table slot %u to %p", id, result );
    workers[id] = result;

    pid = fork();

    if( pid == 0 ) // child
    {
        void * data;

        // Get shm struct
        data = ( void * ) get_worker_by_pid();

        if( data != NULL )
        {
            ( ( struct worker * ) data )->my_argc = argc;
            ( ( struct worker * ) data )->my_argv = argv;
        }

        _log( LOG_LEVEL_DEBUG, "child argv: %p argc: %d", argv, argc );
        _log( LOG_LEVEL_DEBUG, "Post fork, got data %p", data );

        // Register signal handlers
        _set_process_title(
            argv,
            argc,
            ( ( ( struct worker * ) data )->type == WORKER_TYPE_EVENT_PROCESSOR ) ? WORKER_TITLE_EVENT_PROCESSOR : WORKER_TITLE_WORK_PROCESSOR,
            &max_argv_size
        );

        signal( SIGHUP, __sighup );
        signal( SIGTERM, __sigterm );
        signal( SIGINT, __sigint );

        function( data ); // Child main() equiv

        return NULL;
    }
    else if( pid < 0 )
    {
        _log( LOG_LEVEL_FATAL, "Fork Failed" );
        return NULL;
    }

    result->pid = pid;

    return result;
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
bool _rollback_transaction( struct worker * me )
{
    PGresult * result = NULL;

    if( !( me->tx_in_progress ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempted to issue ROLLBACK when no transaction was in progress"
        );
        return false;
    }

    result = PQexec(
        me->conn,
        "ROLLBACK"
    );

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
    me->tx_in_progress = false;
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
bool _commit_transaction( struct worker * me )
{
    PGresult * result = NULL;

    if( !( me->tx_in_progress ) )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempted to issue COMMIT when not transaction was in progress"
        );
        return false;
    }

    result = PQexec(
        me->conn,
        "COMMIT"
    );

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
    me->tx_in_progress = false;
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
bool _begin_transaction( struct worker * me )
{
    PGresult * result = NULL;

    if( me->tx_in_progress )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Attempt to issue BEGIN when a transaction is already in progress"
        );
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
    // TODO, verify that all processes receive this, and that the children cleanup, and the parent reaps
    _log(
        LOG_LEVEL_DEBUG,
        "Got SIGTERM. Completing current transaction..."
    );

    __term();
}

/*
 * void __sighub( int sig )
 *     SIGHUP handler
 *
 * Arguments:
 *     int sig: Signal number for SIGHUP
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
void __sighup( int sig )
{
    struct worker * me      = NULL;
    unsigned int    i       = 0;
    bool            all_ack = false;
    int             wstatus = 0;
    me = get_worker_by_pid();

    if( me == NULL )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Could not handle sighup, got null pid slice"
        );
        return;
    }

    if( me->type == WORKER_TYPE_PARENT )
    {
        _log(
            LOG_LEVEL_INFO,
            "Parent received reload command (SIGHUP)"
        );

        got_sighup = true;
        // spread sighup to all workers so they can join the party
        for( i = 0; i < ( work_jobs + event_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                _log(
                    LOG_LEVEL_DEBUG,
                    "PID table slice prior to sighup:"
                );
                _debug_worker_slot( workers[i] );
                kill( workers[i]->pid, SIGHUP );
            }
            else
            {
                _log(
                    LOG_LEVEL_WARNING,
                    "Acking SIGHUP: tid slot %u is empty!",
                    i
                );
            }
        }

        // Verify that all children have acked the sighup
        // and re-entered the working state
        while( all_ack == false )
        {
            sleep( 1 );
            all_ack = true;

            for( i = 0; i < ( work_jobs + event_jobs ); i++ )
            {
                if( workers[i] != NULL )
                {
                    if( workers[i]->status != STATUS_WORKING )
                    {
                        all_ack = false;
                        _log(
                            LOG_LEVEL_DEBUG,
                            "The following worker has failed to re-enter working state following SIGHUP"
                         );
                         _debug_worker_slot( workers[i] );
                         waitpid( workers[i]->pid, &wstatus, WNOHANG );

                         if( WIFSIGNALED( wstatus ) )
                         {
                             _log(
                                 LOG_LEVEL_DEBUG,
                                 "FYI worker exited with status %d",
                                 WTERMSIG( wstatus )
                             );
                         }
                    }
                }
                else
                {
                    _log(
                        LOG_LEVEL_WARNING,
                        "Worker check post SIGHUP: tid slot %u is empty!",
                        i
                    );
                }
            }

            if( all_ack == false )
            {
                _log(
                    LOG_LEVEL_WARNING,
                    "Not all workers have ACK'd the SIGUP"
                );
            }
        }

        got_sighup = false;
        return;
    }

    // Worker section
    me->status = STATUS_RELOAD;

    _log(
        LOG_LEVEL_INFO,
        "Pid %u got SIGHUP, reloading config",
        ( unsigned int ) getpid()
    );

    if( me->tx_in_progress )
    {
        _rollback_transaction( me );
    }

    /*
     *  Note: some connection poolers *cough* pgbouncer, pgpool will cache
     *  GUC values. We will force a reconnect (and hopefully open up a new pool
     *  in the process such that we get the latest value of our GUCs for tasks
     *  such as get_uid / set_uid and REST calls
     */
    PQfinish( me->conn );
    me->conn           = NULL;
    me->tx_in_progress = false;

    signal ( sig, __sighup );

    me->status = STATUS_WORKING;
    _log(
        LOG_LEVEL_DEBUG,
        "Child reset status to working"
    );

    return;
}

/*
 * void __sigint( int sig )
 *     SIGINT handler
 *
 * Arguments:
 *     int sig: Signal number for SIGINT
 * Return:
 *     None
 * Error Conditions:
 *     Emits error upon receiving SIGTERM
 */
void __sigint( int sig )
{
    got_sigint = true;

    _log(
        LOG_LEVEL_DEBUG,
        "Got SIGINT, Completing current transaction..."
    );

    __term();
}

/*
 * void __term( void )
 *     Termination handler for __sigint or fatal errors
 * Arguments:
 *     None
 * Return:
 *     None
 * Error Conditions:
 */
void __term( void )
{
    struct worker * me = NULL;
    unsigned int    i  = 0;

    me = get_worker_by_pid();

    if( me == NULL )
    {
        // Could not find PID entry, just exit
        _log(
            LOG_LEVEL_DEBUG,
            "No PID table entry for %d", getpid()
        );

        exit(0);
    }

    if( me->type != WORKER_TYPE_PARENT )
    {
        // just exit, let parent cleanup
        me = get_worker_by_pid();

        _log(
            LOG_LEVEL_DEBUG,
            "Child %d exiting", getpid()
        );

        if( me == NULL )
        {
            exit(0);
        }

        if( me->conn != NULL )
        {
            if( me->tx_in_progress )
            {
                _rollback_transaction( me );
            }

            PQfinish( me->conn );
            me->conn = NULL;
        }

        if( me->enable_curl || me->curl_handle != NULL )
        {
            curl_easy_cleanup( me->curl_handle );
            me->curl_handle = NULL;
            me->enable_curl = false;
        }

        me->status = STATUS_DEAD;
        exit(0);
    }
    else
    {
        // TODO: Check got SIGCHLD
        // I'm the parent
        for( i = 0; i < ( work_jobs + event_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                kill( workers[i]->pid, SIGTERM );
                waitpid( workers[i]->pid, NULL, WNOHANG );

                // Allow workers to clean up their mess
                //free_worker( workers[i] );
                workers[i] = NULL;
            }
        }

        munmap( workers, sizeof( struct worker * ) * ( work_jobs + event_jobs ) );
        workers = NULL;

        if( parent != NULL )
        {
            free_worker( parent );
        }
    }

    exit(1);
}

/*
 * struct worker * get_worker_by_pid()
 *    Call getpid() and search the process table for the worker struct
 *
 * Arguments:
 *     None
 * Return:
 *     struct worker * on successful location
 *     NULL on error
 * Error condition:
 *     Returns NULL and emits warning on failure to locate worker entry
 */
struct worker * get_worker_by_pid()
{
    struct worker * me  = NULL;
    unsigned int    i   = 0;
    pid_t           pid = 0;

    pid = getpid();

    if( parent != NULL )
    {
        if( pid == parent->pid )
        {
            me = parent;
        }
    }
    else
    {
        _log( LOG_LEVEL_DEBUG, "parent process entry is NULL" );
    }

    // Search workers array
    if( me == NULL && workers != NULL )
    {
        for( i = 0; i < ( event_jobs + work_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                if( workers[i]->pid == pid )
                {
                    me = workers[i];
                    break;
                }
            }
        }
    }

    if( workers == NULL )
    {
        _log( LOG_LEVEL_DEBUG, "PID table is NULL" );
    }

    return me;
}

/*
 * void * create_shared_memory( size_t size )
 *     Allocates a memory pointer of size_t using mmap
 *
 * Arguments:
 *     size_t size: Number of bytes to allocate
 * Return:
 *     void * ptr: Pointer to allocated memory region
 *     NULL on error
 * Error Conditions:
 *     Emits error and returns NULL on allocation failure
 */

void * create_shared_memory( size_t size )
{
    void * ptr = NULL;

    int protection = PROT_READ | PROT_WRITE;
    int visibility = MAP_ANONYMOUS | MAP_SHARED;

    _log(
        LOG_LEVEL_DEBUG,
        "Attempting to map shared memory of size %lu",
        size
    );

    ptr = mmap( NULL, size, protection, visibility, 0, 0 );

    if( ptr == MAP_FAILED )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate shared memory: %s",
            strerror( errno )
        );

        return NULL;
    }

    return ptr;
}

/*
 * void _manage_children( void (*function)( void * )
 *     Loop for parent process to run, monitors child processes for
 *     unexpected termination, and if so inclined, attempts to restart them.
 *
 * Arguments:
 *     void (*function)(void * ) Function pointer to child process routine
 *
 * Return:
 *     None
 *
 * Error Conditions:
 *     - Emits error when a child is found dead
 *     - Emits error when a child cannot be restarted
 */
void _manage_children( void (*function)( void * ) )
{
    struct worker * worker   = NULL;
    unsigned short  type     = 0;
    pid_t           pid      = 0;
    bool            alive    = true; // Indicates at least one child is alive
    unsigned int    tid      = 0;
    int             wstatus  = 0;
    //trap parent here
    // TODO: Add child monitoring, config reload support

    while( alive )
    {
        sleep( 10 );
        alive = false;
        _log( LOG_LEVEL_DEBUG, "Parent entering maintenance loop" );
        for( tid = 0; tid < ( event_jobs + work_jobs ); tid++ )
        {
            if( workers[tid] == NULL )
            {
                _log(
                    LOG_LEVEL_DEBUG,
                    "Skipping dead worker at index %u",
                    tid
                );
                continue;
            }

            worker = workers[tid];
            type = worker->type;
            pid  = worker->pid;
            waitpid( pid, &wstatus, WNOHANG );

            if( WIFSIGNALED( wstatus ) ) // Detect abnormal exit
            {
                worker->status = STATUS_DEAD;

                _log(
                    LOG_LEVEL_WARNING,
                    "Worker process %d found dead from signal %d",
                    pid,
                    WTERMSIG( wstatus )
                );
            }
           
            wstatus = 0;
 
            if( worker->status == STATUS_DEAD )
            {
                _log(
                    LOG_LEVEL_DEBUG,
                    "Found dead worker (%s queue, pid %d), (sigt flag: %s) restarting...",
                    type == WORKER_TYPE_EVENT_PROCESSOR ? "Event" : "Work",
                    pid,
                    got_sigterm ? "T" : "F"
                );

                sleep( 5 );

                if( worker->status == STATUS_DEAD && !single_step_only )
                {
                    // We ignore single stepping as all shm will be cleaned up
                    // as the parent exits
                    int status;
                    waitpid( pid, &status, WNOHANG );

                    if( status != 0 )
                    {
                        // Child terminated abnormally or is in a stopped state
                        kill( pid, SIGTERM );
                        waitpid( pid, NULL, WNOHANG );
                    }

                    if( ALLOW_WORKER_RESTART && function != NULL )
                    {
                        _log( LOG_LEVEL_DEBUG, "In restart block" );
                        if( got_sigterm || got_sigint )
                        {
                            __term();
                        }

                        worker = new_worker( type, tid, function, parent->my_argc, parent->my_argv, worker );

                        if( worker == NULL )
                        {
                            _log(
                                LOG_LEVEL_ERROR,
                                "Failed to restart %s queue worker",
                                type == WORKER_TYPE_EVENT_PROCESSOR ? "Event" : "Work"
                            );

                            continue;
                        }

                        sleep( 2 );

                        pid = worker->pid;

                        if( worker->status == STATUS_DEAD )
                        {
                            kill( pid, SIGTERM );
                            waitpid( pid, NULL, WNOHANG );

                            if( munmap( worker, sizeof( struct worker ) ) != 0 )
                            {
                                _log(
                                    LOG_LEVEL_ERROR,
                                    "Worker pid %d failed to start, marking PID table entry as dead",
                                    pid
                                );
                            }

                            worker = NULL;
                            pid = 0;
                        }

                        workers[tid] = worker;
                        alive = true;
                    }
                    else
                    {
                        worker->pid = 0;
                        worker->type = 0;
                        worker->status = STATUS_DEAD;
                        
                        if( munmap( worker, sizeof( struct worker ) ) != 0 )
                        {
                            _log(
                                LOG_LEVEL_ERROR,
                                "Failed to free worker shared memory"
                            );
                        }

                        workers[tid] = NULL;
                        worker = NULL;
                    }
                }
            }
            else
            {
                alive = true;
            }
        }
    }

    __term();
}

/*
 *  void _set_process_title(
 *      char **        argv,
 *      int            argc,
 *      char *         title,
 *      unsigned int * max_size
 *  )
 *
 *  Set the title of the invoking process. Uses the parent process to determine
 *  the size of the argv array and store in max_size pointer such that later
 *  invokers do not overflow the bounds of argv[]
 *
 *  Arguments:
 *      char ** argv: Pointer to the argument array
 *      int     argc: Number of arguments in the argv array
 *      char *  title: The name of the process, will be written to argv[0]
 *      unsigned int * max_size: set when the parent calls this routine, used
 *                               to determine the bounds of the write for later
 *                               callers.
 *
 *  Return:
 *      None
 *
 *  Error Conditions:
 *      Throws an error when the process title is null or argv is null
 */
void _set_process_title(
    char **        argv,
    int            argc,
    char *         title,
    unsigned int * max_size
)
{
    unsigned int i    = 0;
    unsigned int size = 0;

    if( title == NULL )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "NULL process title provided"
        );
        return;
    }

    if( argv == NULL )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "ARGV is null :("
        );

        return;
    }

    // Compute ARGV size on first run
    if( max_size == NULL || *max_size == 0 )
    {
        size = 0;
        // Get total size of argv[][]
        for( i = 0; i < argc; i++ )
        {
            if( argv[i] == NULL )
            {
                continue;
            }
            else
            {
                size += ( unsigned int ) ( strlen( argv[i] ) + 1 );
            }
        }

        _log(
            LOG_LEVEL_DEBUG,
            "Argv total size: %u",
            size
        );

        *max_size = size;
    }

    size = *max_size;
    memset( argv[0], '\0', size );
    strncpy( argv[0], title, strlen( title ) );
    return;
}

void _debug_worker_slot( struct worker * worker )
{
    if( worker == NULL )
    {
        return;
    }

    _log(
        LOG_LEVEL_DEBUG,
        "\nWorker struct %p\n"\
        "dequeue_function: %p,\n"\
        "channel: %s,\n"\
        "connection handle: %p,\n"\
        "curl handle: %p,\n"\
        "pid: %d,\n"\
        "type: %s,\n"\
        "tx_in_progress: %s,\n"\
        "curl enabled: %s,\n"\
        "status: %s,\n"\
        "ARGC: %d,\n"\
        "ARGV: %p,\n",
        worker,
        worker->dequeue_function,
        worker->channel,
        worker->conn,
        worker->curl_handle,
        (int) worker->pid,
        worker->type == WORKER_TYPE_PARENT ? "PARENT" : worker->type == WORKER_TYPE_EVENT_PROCESSOR ? "EVENT" : "WORK",
        worker->tx_in_progress == true ? "YES" : "NO",
        worker->enable_curl == true ? "YES" : "NO",
        worker->status == STATUS_DEAD ? "DEAD" : worker->status == STATUS_STARTUP ? "STARTUP" : worker->status == STATUS_WORKING ? "WORKING" : "RELOAD",
        worker->my_argc,
        worker->my_argv
    );

    return;
}
