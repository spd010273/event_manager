/*------------------------------------------------------------------------
 *
 * util.c
 *     Utility functions
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

struct worker ** workers = NULL;
struct worker * parent   = NULL;
char          * conninfo = NULL;

unsigned int work_jobs  = 0;
unsigned int event_jobs = 0;

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

    while( ( c = getopt( argc, argv, "U:p:d:h:E:W:v?" ) ) != -1 )
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
                break;
            case 'W':
                work_job_count = optarg;
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
 *
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
bool parent_init( void )
{
    parent = new_worker( WORKER_TYPE_PARENT, 0, NULL );

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
    unsigned short type,
    unsigned int   id,
    void (*function)( void * )
)
{
    struct worker * result = NULL;
    pid_t           pid    = 0;
    size_t          size   = 0;

    result = ( struct worker * ) create_shared_memory(
        sizeof( struct worker )
    );

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
            _log( LOG_LEVEL_DEBUG, "Creating SHM with size %lu: WJ %d, EJ: %d", size, work_jobs, event_jobs );
            workers = ( struct worker ** ) create_shared_memory( size );
            /*
            workers = ( struct worker ** ) calloc(
                work_jobs + event_jobs,
                sizeof( struct worker * )
            );
            */
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

        return result;
    }

    workers[id] = result;

    pid = fork();

    if( pid == 0 ) // child
    {
        void * data;

        data = ( void * ) get_worker_by_pid();
        _log( LOG_LEVEL_DEBUG, "Post fork, got data %p", data );
        function( data );
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
        LOG_LEVEL_ERROR,
        "Got SIGTERM. Completing current transaction..."
    );

    __term();
}

void __sighup( int sig )
{
    // TODO: reload config?
    got_sighup = true;
    signal ( sig, __sighup );
    return;
}

void __sigint( int sig )
{
    got_sigint = true;

    _log(
        LOG_LEVEL_ERROR,
        "Got SIGINT, Completing current transaction..."
    );

    __term();
}

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
                free_worker( workers[i] );
                workers[i] = NULL;
            }
        }

        munmap( workers, sizeof( struct worker * ) * ( work_jobs + event_jobs ) );
        workers = NULL;
        free_worker( parent );
    }

    exit(1);
}

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
        _log( LOG_LEVEL_ERROR, "parent process entry is NULL" );
    }

    // Search worker table
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

void _manage_children( void (*function)( void * ) )
{
    struct worker * worker   = NULL;
    unsigned short  type     = 0;
    pid_t           pid      = 0;
    bool            alive    = true; // Indicates at least one child is alive
    unsigned int    tid      = 0;

    //trap parent here
    // TODO: Add child monitoring, config reload support

    while( alive )
    {
        alive = false;
        for( tid = 0; tid < ( event_jobs + work_jobs ); tid++ )
        {
            worker = workers[tid];

            if( worker == NULL )
            {
                _log(
                    LOG_LEVEL_DEBUG,
                    "Skipping dead worker slot at index %u",
                    tid
                );
                continue;
            }

            type = worker->type;
            pid  = worker->pid;

            if( worker->status == STATUS_DEAD )
            {
                _log(
                    LOG_LEVEL_DEBUG,
                    "Found dead worker (%s queue, pid %d), restarting...",
                    type == WORKER_TYPE_EVENT_PROCESSOR ? "Event" : "Work",
                    pid
                );

                sleep( 5 );

                if( worker->status == STATUS_DEAD )
                {
                    int status;
                    waitpid( pid, &status, WNOHANG );

                    if( status != 0 )
                    {
                        // Child terminated abnormally or is in a stopped state
                        kill( pid, SIGTERM );
                        waitpid( pid, NULL, WNOHANG );
                    }

                    if( munmap( worker, sizeof( struct worker ) ) != 0 )
                    {
                        _log( LOG_LEVEL_ERROR, "Failed to free worker shared memory" );
                    }

                    if( ALLOW_WORKER_RESTART && function != NULL )
                    {
                        worker = new_worker( type, tid, function );

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
                    }
                }
            }
            else
            {
                alive = true;
            }
        }
    }

    // All children dead, exit
    if( munmap( workers, sizeof( struct worker * ) * ( event_jobs + work_jobs ) ) != 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to free PID table"
        );
    }
    
    if( parent != NULL )
    {
        free( parent );
    }

    free( conninfo );
    wait( NULL );
    exit(0);
}
