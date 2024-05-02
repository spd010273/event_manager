/*------------------------------------------------------------------------
 *
 * util.c
 *     Utility and process management functions
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        src/lib/util.c
 *
 *------------------------------------------------------------------------
 */

#include "util.h"

struct worker ** workers          = NULL;
struct worker *  parent           = NULL;
char *           conninfo         = NULL;
bool             single_step_only = false;
bool             daemonize        = false;
FILE *           log_file         = NULL;

extern char ** envorion; // Declared in unistd.h

unsigned int work_jobs     = 0;
unsigned int event_jobs    = 0;
unsigned int max_argv_size = 0;

// Flags
volatile sig_atomic_t got_sighup  = false;
volatile sig_atomic_t got_sigterm = false;
volatile sig_atomic_t got_sigint  = false;

static const char * usage_string = "\
Usage: event_manager\n \
    -U DB User (default: postgres)\n \
    -p DB Port (default: 5432)\n \
    -h DB Host (default: localhost)\n \
    -d DB name (default: DB User)\n \
    -E worker_count: Number of Event Queue workers to spawn\n \
    -W worker_count: Number of Work Queue workers to spawn\n \
  [ -D daemonize\n \
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
    int          c               = 0;
    unsigned int len             = 0;
    char *       username        = NULL;
    char *       dbname          = NULL;
    char *       port            = NULL;
    char *       hostname        = NULL;
    char *       event_job_count = NULL;
    char *       work_job_count  = NULL;

    opterr = 0;

    while( ( c = getopt( argc, argv, "U:p:d:h:E:W:Sv?D" ) ) != -1 )
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
                printf( "Event Manager, version %s\n", VERSION );
                exit( 0 );
            case 'E':
                event_job_count = optarg;
                break;
            case 'W':
                work_job_count = optarg;
                break;
            case 'S':
                single_step_only = true;
                break;
            case 'D':
                daemonize = true;
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

#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
    override_event_jobs = 0;
    override_work_jobs  = 0;
#endif // ALLOW_OVERRIDE_WORKER_COUNTS
    if( port == NULL )
        port = "5432";

    if( username == NULL )
        username = "postgres";

    if( hostname == NULL )
        hostname = "localhost";

    if( dbname == NULL )
        dbname = username;

    len = (
            strlen( username ) +
            strlen( port ) +
            strlen( dbname ) +
            strlen( hostname ) +
            26
          );

    conninfo = ( char * ) calloc( len, sizeof( char ) );

    if( conninfo == NULL )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Failed to allocate memory for connection string :("
        );
    }

    strcpy( conninfo, "user="    );
    strcat( conninfo, username   );
    strcat( conninfo, " host="   );
    strcat( conninfo, hostname   );
    strcat( conninfo, " port="   );
    strcat( conninfo, port       );
    strcat( conninfo, " dbname=" );
    strcat( conninfo, dbname     );
    conninfo[len - 1] = '\0';

#ifdef C_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "Parsed args: %s",
        conninfo
    );
#endif // C_DEBUG

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
    va_list        args          = {{0}};
    FILE *         output_handle = NULL;
    struct timeval tv            = {0};
    char           buff_time[28] = {0}; // Time gon' give it to ya
    unsigned short ll_len        = 0;

    if( message == NULL )
    {
        return;
    }

    gettimeofday( &tv, NULL );
    strftime(
        buff_time,
        sizeof( buff_time ) / sizeof( *buff_time ),
        "%Y-%m-%d %H:%M:%S",
        gmtime( &tv.tv_sec )
    );

    ll_len = strlen( log_level );

    // Setup logfile iff we're daemonizing and the parent's worker slot has
    // been inited
    if( daemonize )
    {
        if( log_file != NULL )
        {
            output_handle = log_file;
        }
        else
        {
            fprintf(
                stderr,
                "we're daemonized but the log file is not inited :(, message was %s\n",
                message
            );
        }
    }

    if(
           output_handle == NULL
        && (
                strncmp(
                    log_level,
                    LOG_LEVEL_WARNING,
                    MIN(
                        ll_len,
                        strlen( LOG_LEVEL_WARNING )
                    )
                ) == 0
             || strncmp(
                    log_level,
                    LOG_LEVEL_ERROR,
                    MIN(
                        ll_len,
                        strlen( LOG_LEVEL_ERROR )
                    )
                ) == 0
             || strncmp(
                    log_level,
                    LOG_LEVEL_FATAL,
                    MIN(
                        ll_len,
                        strlen( LOG_LEVEL_FATAL )
                    )
                ) == 0
           )
      )
    {
        output_handle = stderr;
    }
    else if( output_handle == NULL )
    {
        output_handle = stdout;
    }

    va_start( args, message );

#ifndef DEBUG
    if(
        strncmp(
            log_level,
            LOG_LEVEL_DEBUG,
            MIN(
                ll_len,
                strlen( LOG_LEVEL_DEBUG )
            )
        ) != 0
      )
    {
#endif // DEBUG
        fprintf(
            output_handle,
            "%s.%03d ",
            buff_time,
            ( int ) ( tv.tv_usec / 1000 )
        );

        fprintf(
            output_handle,
            "[%d] ",
            getpid()
        );

        fprintf(
            output_handle,
            "%s: ",
            log_level
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
#endif // DEBUG

    va_end( args );
    fflush( output_handle );

    if(
        strncmp(
            log_level,
            LOG_LEVEL_FATAL,
            MIN(
                ll_len,
                strlen( LOG_LEVEL_FATAL )
            )
        ) == 0
      )
    {
        __term();
    }

    return;
}

/*
 * void free_worker( struct worker * worker, bool free_only )
 *     Deallocates memory used by a process for handles and objects
 *
 * Arguments:
 *     struct worker * worker: workers[] array slice for the child process
 *     bool free_only: Only free memory, do not close connections
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
void free_worker( struct worker * worker, bool free_only )
{
    if( worker == NULL )
    {
        return;
    }

    if( !free_only )
    {
        if( worker->conn != NULL && PQstatus( worker->conn ) == CONNECTION_OK )
        {
            if( worker->tx_in_progress )
            {
                PQexec( worker->conn, "ROLLBACK" );
                worker->tx_in_progress = false;
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
    }

    munmap( worker, sizeof( struct worker ) );
    worker = NULL;
    return;
}

// Releases the file handle for the log once a HUP happens
bool logrotate( struct worker * me )
{
    if( me == NULL )
        return false;

    if( me->type != WORKER_TYPE_PARENT )
        return false;

    if( daemonize == false )
        return true;

    if( log_file == NULL )
        return false;

    fclose( log_file );
    log_file = NULL;

    errno = 0;
    log_file = fopen( LOG_FILE_NAME, "a" );

    if( log_file == NULL )
    {
        fprintf(
            stderr,
            "Failed to rotate logfile: %s",
            strerror( errno )
        );
        return false;
    }

    return true;
}

/*
 *  bool parent_init( void )
 *      Initial special (initial) call to new_worker for parent process,
 *      Then drop PID file for systemctl to read
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
    DIR *           dir           = NULL;
    FILE *          pfh           = NULL;
    char *          pid_path      = NULL;
    char *          my_pid        = NULL;
    char *          pid_file      = "event_manager.pid";
    struct stat     statbuffer    = {0};
    unsigned int    string_offset = 0;
    unsigned int    total_size    = 0;
    char            path[PATH_MAX] = {0};
    struct passwd * pw            = NULL;

    // Do we need to daemonize?
    if( daemonize )
    {
        // daemonize with nochdir != 0 and noclose == 0
        if( daemon( 1, 1 ) != 0 )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Daemonization failed"
            );

            return false;
        }

        errno = 0;
        log_file = fopen( LOG_FILE_NAME, "a" );

        if( log_file == NULL )
        {
            fprintf(
                stderr,
                "\nFailed to open log file " LOG_FILE_NAME ": %s\n\
Please verify that this directory is writable by \
the current user\n",
                strerror( errno )
            );
            errno = 0;
            return false;
        }
    }

    // Create parent struct
    parent = new_worker( WORKER_TYPE_PARENT, 0, NULL, argc, argv, NULL );

    if( parent == NULL )
    {
        return false;
    }

    // Find where we should place PID file
    //     /var/run/<file>
    //     ~/<file>
    //     cwd/<file>
    //     cry

    dir = opendir( "/var/run" );

    if( dir != NULL )
    {
        closedir( dir );
        pfh = fopen( "/var/run/__test.pid", "w" );

        // Jankily test that we can actually write to this dir
        if( pfh != NULL )
        {
            fclose( pfh );
            remove( "/var/run/__test.pid" );

            pfh = NULL;
            strncpy( path, "/var/run", 8 );
        }
    }

    // See if above failed
    if( strlen( path ) == 0 )
    {
        // either /var/run doesn't exist (!!!) or we cannot write to it,
        // get homedir
        if( getenv("HOME") != NULL )
        {
            strncpy( path, getenv( "HOME" ), sizeof( path ) );
        }

        if( strlen( path ) == 0 )
        {
            pw = getpwuid( geteuid() );

            if( pw == NULL )
            {
                // Give up and get CWD
                if( getcwd( path, sizeof( path ) ) == NULL )
                {
                    // Really give up
                    _log(
                        LOG_LEVEL_ERROR,
                        "Failed to determine where to place PID file"
                    );

                    return false;
                }
            }
            else
            {
                strncpy( path, pw->pw_dir, sizeof( path ) );
            }
        }
    }

    if( path[strlen(path) - 1] != '/' )
    {
        string_offset = 1;
    }

    total_size = strlen( path )
               + strlen( pid_file )
               + string_offset + 1;

    // Valgrind will complain about a leak here - we copy the pointer and
    // free it when the parent exits :|
    pid_path = ( char * ) calloc(
        sizeof( char ),
        total_size
    );

    if( pid_path == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for PID file path :("
        );

        return false;
    }

    strncpy( pid_path, path, strlen( path ) );

    if( string_offset == 1 )
    {
        strncat( pid_path, "/", 1 );
    }

    strncat( pid_path, pid_file, strlen( pid_file ) );
    pid_path[total_size - 1] = '\0';

#ifdef C_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "Opening pid file at %s",
        pid_path
    );
#endif // C_DEBUG

    // Does our PID file exist (we're already running)
    if( stat( pid_path, &statbuffer ) >= 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "PID file %s already exists, is event_manager already running?",
            pid_path
        );
        free( pid_path );
        return false;
    }

    // Include space for \n\0 at the end of the string
    total_size = ( unsigned int ) ceil( log10( (int) getpid() ) + 3 );

    my_pid = ( char * ) calloc(
        sizeof( char ),
        total_size
    );

    if( my_pid == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for PID string"
        );

        free( pid_path );
        return false;
    }

    snprintf( my_pid, total_size, "%d\n", ( int ) getpid()  );
#ifdef C_DEBUG
    _log( LOG_LEVEL_DEBUG, "my_pid: %s", my_pid );
#endif // C_DEBUG
    pfh = fopen( pid_path, "w" );

    if( pfh == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to open file '%s' for writing",
            pid_path
        );

        free( pid_path );
        free( my_pid );
        return false;
    }


    fprintf( pfh, "%s", my_pid );
    parent->pidfile = pid_path;
    fclose( pfh );
    free( my_pid );

    return true;
}

// Register the signal handlers in the current context
void _register_signal_handlers( void )
{
    signal( SIGHUP, __sighup );
    signal( SIGINT, __sigint );
    signal( SIGTERM, __sigterm );
    return;
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
 *              WORKER_TYPE_EVENT_PROCESSOR
 *           or WORKER_TYPE_CONFIG_MANAGER
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
    void *          data   = NULL;
    struct worker * worker = NULL;
    unsigned int    tid    = 0;

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
    result->tx_start       = 0;
    result->tx_success     = 0;
    result->tx_fail        = 0;
    result->tx_duration    = 0.0;
    result->last_heartbeat = time( NULL );
#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
    result->commanded_shutdown = false;
    result->commanded_refresh  = false;
    result->new_event_jobs     = 0;
    result->new_work_jobs      = 0;
#endif // ALLOW_OVERRIDE_WORKER_COUNTS

    if(
            type != WORKER_TYPE_PARENT
         && type != WORKER_TYPE_WORK_PROCESSOR
         && type != WORKER_TYPE_EVENT_PROCESSOR
#ifdef ALLOW_CONFIG_MANAGER
         && type != WORKER_TYPE_CONFIG_MANAGER
#endif // ALLOW_CONFIG_MANAGER
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
#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
            // We cannot properly reallocate a shared memory mapping without
            // restarting the processes we've forked after this point,
            // otherwise we'll get a SIGSEGV. To circumvent this, we
            // statically allocate the maximum size (8 bytes * MAX_WORKERS).
            size = MAX_WORKERS * sizeof( struct worker * );
#else
            size = ( work_jobs + event_jobs ) * sizeof( struct worker * );
#endif // ALLOW_OVERRIDE_WORKER_COUNTS

#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Creating SHM with size %lu: WJ %d, EJ: %d",
                size,
                work_jobs,
                event_jobs
            );
#endif // C_DEBUG
            workers = ( struct worker ** ) create_shared_memory( size );

            if( workers == NULL )
            {
                _log(
                    LOG_LEVEL_FATAL,
                    "Could not allocate child process table"
                );
            }
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "PID table allocated at %p", workers
            );
#endif // C_DEBUG
#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
            for( tid = 0; tid < MAX_WORKERS; tid++ )
#else
            for( tid = 0; tid < ( work_jobs + event_jobs ); tid++ )
#endif
            {
                workers[tid] = NULL;
            }
        }

        result->my_argv = argv;
        result->my_argc = argc;
#ifdef C_DEBUG
        _log( LOG_LEVEL_DEBUG, "Parent argv: %p argc: %d", argv, argc );
#endif // C_DEBUG
        // Register signal handlers
        _register_signal_handlers();
        return result;
    }

    // prep to copy FH
#ifdef ALLOW_CONFIG_MANAGER
    if( type == WORKER_TYPE_CONFIG_MANAGER )
    {
        config = result;
    }
    else
    {
#endif // ALLOW_CONFIG_MANAGER
#ifdef C_DEBUG
    _log( LOG_LEVEL_DEBUG, "Remapped PID table slot %u to %p", id, result );
#endif // C_DEBUG
    workers[id] = result;
#ifdef ALLOW_CONFIG_MANAGER
    }
#endif // ALLOW_CONFIG_MANAGER

    pid = fork();

    if( pid == 0 ) // child
    {
        // Get shm struct
        data = ( void * ) get_worker_by_pid();
        worker = ( struct worker * ) data;

        if( data != NULL )
        {
            worker->my_argc = argc;
            worker->my_argv = argv;
        }

#ifdef C_DEBUG
        _log( LOG_LEVEL_DEBUG, "child argv: %p argc: %d", argv, argc );
        _log( LOG_LEVEL_DEBUG, "Post fork, got data %p", data );
        _debug_worker_slot( worker );
#endif // C_DEBUG

        _set_process_title(
            argv,
            argc,
            worker->type == WORKER_TYPE_EVENT_PROCESSOR ? WORKER_TITLE_EVENT_PROCESSOR :
            worker->type == WORKER_TYPE_WORK_PROCESSOR  ? WORKER_TITLE_WORK_PROCESSOR :
#ifdef ALLOW_CONFIG_MANAGER
            worker->type == WORKER_TYPE_CONFIG_MANAGER  ? WORKER_TITLE_CONFIG_MANAGER :
#endif // ALLOW_CONFIG_MANAGER
            "UNKNOWN",
            &max_argv_size
        );

        _register_signal_handlers();

        function( data ); // Child main() equiv

        return NULL;
    }
    else if( pid < 0 )
    {
        _log( LOG_LEVEL_FATAL, "Fork Failed" );
    }

    result->pid = pid;

    return result;
}

void _gather_child_stats_to_self( struct em_stat ** stats )
{
    struct worker *  me = NULL;
    unsigned int     i  = 0;
    struct em_stat * w  = NULL;
    struct em_stat * e  = NULL;

    if( stats == NULL )
    {
        return;
    }

    e = stats[0];
    w = stats[1];

    if( w == NULL || e == NULL )
    {
        return;
    }

    me = get_worker_by_pid();

    if( me == NULL || me->type != WORKER_TYPE_PARENT )
    {
        _log( LOG_LEVEL_ERROR, "Failed to get parent process slot" );
        return;
    }

    if( workers == NULL )
    {
        _log( LOG_LEVEL_ERROR, "PID table empty!" );
        return;
    }

    for( i = 0; i < ( work_jobs + event_jobs ); i++ )
    {
        if( workers[i] == NULL )
        {
            _log( LOG_LEVEL_WARNING, "Stat collector skipping empty worker slot" );
            continue;
        }

        // check mutex
        if( _wait_and_set_mutex( workers[i] ) == false )
        {
            continue;
        }

        if( workers[i]->type == WORKER_TYPE_EVENT_PROCESSOR )
        {
            e->tx_success  += workers[i]->tx_success;
            e->tx_fail     += workers[i]->tx_fail;
            e->tx_duration += workers[i]->tx_duration;
        }
        else if( workers[i]->type == WORKER_TYPE_WORK_PROCESSOR )
        {
            w->tx_success  += workers[i]->tx_success;
            w->tx_fail     += workers[i]->tx_fail;
            w->tx_duration += workers[i]->tx_duration;
        }
        else
        {
            continue;
        }

        workers[i]->tx_success  = 0;
        workers[i]->tx_fail     = 0;
        workers[i]->tx_duration = 0.0;
        workers[i]->stat_update = false;
    }

    return;
}

void _update_stats(
    struct worker * me,
    unsigned int    tx_success,
    unsigned int    tx_fail,
    double          tx_duration
)
{
    if( me == NULL )
    {
        return;
    }

    if( _wait_and_set_mutex( me ) == false )
    {
        return;
    }

    if( tx_success > 0 )
    {
        me->tx_success += tx_success;
    }

    if( tx_fail > 0 )
    {
        me->tx_fail += tx_fail;
    }

    if( tx_duration > 0.0 )
    {
        me->tx_duration += tx_duration;
    }

    me->stat_update = false;

    return;
}

bool _wait_and_set_mutex( struct worker * me )
{
    time_t lock_acquire_start = 0;
    double random_backoff     = 0.0;
    double last_backoff       = 0.0;

    if( me == NULL )
    {
        return false;
    }

    lock_acquire_start = time( NULL );

    /*
     *  Attempt to acquire the sig_atomic_t lock in SHM located at me->stat_update
     *  note this is done in two parts using short circuit logic because
     *      - me->stat_update may reside in the L3/L2/L1 cache due to frequent access
     *      - this reduces cache writeback when the parent and worker are simultaneously
     *        trying to clear / set the workers stats, respectively
     *
     *  This is not a super critical operation, hence the backoff and timeout failure modes
     *  We _would_ like to update stats, but in the case we cannot, we'd rather continue doing
     *  our actual job.
     */
    last_backoff = 1.0;

    while( me->stat_update == true || __test_and_set( me ) == true )
    {
        if( difftime( time( NULL ), lock_acquire_start ) > MAX_LOCK_WAIT )
        {
            _log(
                LOG_LEVEL_WARNING,
                "Max lock wait time %d exceeded",
                MAX_LOCK_WAIT
            );
            return false;
        }

        sleep( last_backoff + random_backoff );
        last_backoff   = last_backoff + random_backoff;
        random_backoff = 2 * ( ( double ) rand() / ( double ) RAND_MAX );
    }

    me->stat_update = true;
    return true;
}

bool __test_and_set( struct worker * me )
{
    bool initial = true;
    initial = me->stat_update;
    me->stat_update = true;
    return initial;
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
    got_sigterm = true;
    _log(
        LOG_LEVEL_INFO,
        "Got SIGTERM. Completing current transaction..."
    );
    __term();
}

/*
 * void __sighup( int sig )
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
    struct worker * me = NULL;
    int             i  = 0;

    me = get_worker_by_pid();

    got_sighup = true;

    if( me == NULL )
    {
        return;
    }

    // Parent can immediately handle SIGHUP, children may be mid-tx
    if( me->type == WORKER_TYPE_PARENT )
    {
        if( logrotate( me ) == false )
        {
            _log( LOG_LEVEL_ERROR, "log rotation failed" );
        }

        // spread sighup to all workers so they can join the party
        for( i = 0; i < ( work_jobs + event_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                kill( workers[i]->pid, SIGHUP );
            }
        }

        // Verify that all children have acked the sighup
        // and re-entered the working state
        got_sighup = false;
    }

    // Worker section
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
        LOG_LEVEL_INFO,
        "Got SIGINT, Completing current transaction..."
    );
    __term();
}

/*
 * void __term( void )
 *     Termination handler for SIGINT, SIGTERM or fatal errors
 * Arguments:
 *     None
 * Return:
 *     None
 * Error Conditions:
 */
void __term( void )
{
    struct worker * me       = NULL;
    unsigned int    i        = 0;
    unsigned int    j        = 0;
    struct stat     filestat = {0};

    me = get_worker_by_pid();

    if( me == NULL )
    {
        // Could not find PID entry, just exit
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "No PID table entry for %d", getpid()
        );
#endif // C_DEBUG
        exit(0);
    }

#ifdef C_DEBUG
    _log( LOG_LEVEL_DEBUG, "__term() invoked for PID %d", getpid() );
#endif // C_DEBUG

    // Immediately disconnect and terminate any open TXs in the DB.
    if( me->conn != NULL )
    {
        if( me->tx_in_progress )
        {
            PQexec( me->conn, "ROLLBACK" );
            me->tx_in_progress = false;
        }

        PQfinish( me->conn );
        me->conn = NULL;
    }

    if( me->type != WORKER_TYPE_PARENT )
    {
        // just exit, let parent cleanup
        me = get_worker_by_pid();
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Child %d exiting", getpid()
        );
#endif // C_DEBUG
        if( me == NULL )
        {
            exit(0);
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
        _log(
            LOG_LEVEL_INFO,
            "Parent disconnected from DB, EM may be restarted"
        );

        _log(
            LOG_LEVEL_INFO,
            "Event Manager is exiting..."
        );

        sleep( 1 );

        for( i = 0; i < ( work_jobs + event_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                kill( workers[i]->pid, SIGTERM );
                waitpid( workers[i]->pid, NULL, WNOHANG );

                for( j = 0; j < WORKER_EXIT_TIMEOUT; j++ )
                {
                    if( workers[i]->status != STATUS_DEAD )
                    {
                        sleep( 1 );
                    }
                    else
                    {
                        free_worker( workers[i], true );
                        workers[i] = NULL;
                        break;
                    }
                }
            }
        }

        munmap( workers, sizeof( struct worker * ) * ( work_jobs + event_jobs ) );
        workers = NULL;

#ifdef ALLOW_CONFIG_MANAGER
        if( config != NULL )
        {
            kill( config->pid, SIGTERM );
            waitpid( config->pid, NULL, WNOHANG );
            free_worker( config, true );
            config = NULL;
        }
#endif // ALLOW_CONFIG_MANAGER

        // Remove PID file prior to exit

        if( parent != NULL )
        {
            if( parent->pidfile != NULL )
            {
                if( stat( parent->pidfile, &filestat ) >= 0 )
                {
                    if( remove( parent->pidfile ) != 0 )
                    {
                        _log(
                            LOG_LEVEL_ERROR,
                            "Failed to remove PID file '%s'",
                            parent->pidfile
                        );
                    }
                }

                free( parent->pidfile );
                parent->pidfile = NULL;
            }

            if( log_file != NULL )
            {
                fclose( log_file );
            }

            free_worker( parent, false );
        }
    }

    exit(1);
}

/*
 * struct worker * get_worker_by_pid()
 *    Call getpid() and search the process table for the worker struct
 *    NOTE: Calling _log from within this function will cause a stack
 *    overflow
 *
 *    TODO: I'll need to remove this to parent-only scope and have a
 *    new_worker set a self pointer that's available between the parent
 *    and worker. The only reason this doesn't SIGSEGV is because the workers
 *    are spawned linearly, so n workers prior to me are in my memory map.
 *    If we were to traverse the array backwards everything would explode
 *    with no survivors
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
    unsigned int i   = 0;
    pid_t        pid = 0;

    pid = getpid();

    // Check if we're the parent first
    // NOTE: parent will be init'd for children due to the fork() call,
    // so we compare using pid to verify
    if( parent != NULL && pid == parent->pid )
    {
        return parent;
    }

#ifdef ALLOW_CONFIG_MANAGER
    if( config != NULL && pid == config->pid )
    {
        return config;
    }
#endif // ALLOW_CONFIG_MANAGER

    // Search workers array
    if( workers != NULL )
    {
        for( i = 0; i < ( event_jobs + work_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                if( workers[i]->pid == pid )
                {
                    return workers[i];
                }
            }
        }
    }

    return NULL;
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
#ifdef C_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "Attempting to map shared memory of size %lu",
        size
    );
#endif // C_DEBUG
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

#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
void _resize_pid_table( void (*function)( void * ) )
{ //XXX
    struct worker *  me               = NULL;
    struct worker *  temp_worker      = NULL;
    struct worker ** temp             = NULL;
    struct worker ** delta_array      = NULL;
    struct worker ** old_workers      = NULL;
    unsigned int *   dead_worker_ind  = NULL;
    unsigned int     delta_ind        = 0;
    unsigned int     new_worker_total = 0;
    unsigned int     tid              = 0;
    unsigned int     t_work_count     = 0;
    unsigned int     t_event_count    = 0;
    unsigned int     old_work_jobs    = 0;
    unsigned int     old_event_jobs   = 0;
    unsigned int     t_index          = 0;
    unsigned int     target_work_cnt  = 0;
    unsigned int     target_event_cnt = 0;
    int              w_delta          = 0;
    int              e_delta          = 0;
    bool             flag_ptr_refresh = false;

    me = get_worker_by_pid();
#ifdef C_DEBUG
    _log( LOG_LEVEL_DEBUG, "_resize_pid_table entry" );
#endif // C_DEBUG

    if( me == NULL || me->type != WORKER_TYPE_PARENT )
    {
        _log( LOG_LEVEL_WARNING, "Illegal entry into _resize_pid_table" );
        return;
    }

    old_workers    = workers;
    old_event_jobs = event_jobs;
    old_work_jobs  = work_jobs;

    if( override_work_jobs != 0 && override_work_jobs != work_jobs )
    {
        new_worker_total += override_work_jobs;
        w_delta           = override_work_jobs - work_jobs;
        target_work_cnt   = override_work_jobs;
    }
    else
    {
        new_worker_total += work_jobs;
        target_work_cnt   = work_jobs;
    }

    if( override_event_jobs != 0 && override_event_jobs != event_jobs )
    {
        new_worker_total += override_event_jobs;
        e_delta           = override_event_jobs - event_jobs;
        target_event_cnt  = override_event_jobs;
    }
    else
    {
        new_worker_total += event_jobs;
        target_event_cnt  = event_jobs;
    }

#ifdef C_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "\nREMAP OF PID TABLE COMMANDED\n" \
        " Old WC: %d, New WC: %d\n" \
        " Old EC: %d, New EC: %d\n",
        work_jobs,
        target_work_cnt,
        event_jobs,
        target_event_cnt
    );
#endif // C_DEBUG

    if( new_worker_total > MAX_WORKERS )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot create more than %d workers (%d commanded)",
            MAX_WORKERS,
            new_worker_total
        );
        override_event_jobs = 0;
        override_work_jobs = 0;
        return;
    }

    if( new_worker_total != ( work_jobs + event_jobs ) )
    {
        // We'll actually need to resize the PID table
        temp = ( struct worker ** ) calloc(
            MAX_WORKERS,
            sizeof( struct worker * )
        );

#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "\nPID table resize occuring\n"\
            "old PID table %p (size %d)\n" \
            "new PID table %p (size %d)\n",
            workers,
            ( work_jobs + event_jobs ),
            temp,
            new_worker_total
        );
#endif // C_DEBUG

        if( temp == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate new PID table, disregarding update"
            );
            override_event_jobs = 0;
            override_work_jobs = 0;
            return;
        }

        flag_ptr_refresh = true;
    }
    else
    {
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Reusing old PID table as it's the correct size"
        );
#endif // C_DEBUG
    }

    // PRUNE STAGE
    if( w_delta < 0 || e_delta < 0 )
    {
#ifdef C_DEBUG
        _log( LOG_LEVEL_DEBUG, "Entering prune stage, WD %d, ED %d", w_delta, e_delta );
#endif // C_DEBUG
        delta_array = ( struct worker ** ) calloc(
            abs( w_delta ) + abs( e_delta ),
            sizeof( struct worker * )
        );

        dead_worker_ind = ( unsigned int * ) calloc(
            abs( w_delta ) + abs( e_delta ),
            sizeof( unsigned int )
        );

        if( delta_array == NULL || dead_worker_ind == NULL )
        {
            if( delta_array != NULL )
                free( delta_array );

            if( dead_worker_ind != NULL )
                free( dead_worker_ind );

            if( temp != NULL )
                free( temp );

            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory during PID table remap"
            );
            override_work_jobs = 0;
            override_event_jobs = 0;
            return;
        }

        for( tid = 0; tid < ( work_jobs + event_jobs ); tid++ )
        {
            temp_worker = workers[tid];

            if( temp_worker == NULL )
            {
                _log(
                    LOG_LEVEL_ERROR,
                    "Existing PID table is invalid"
                );
                if( temp != NULL )
                    free( temp );
                if( delta_array != NULL )
                    free( delta_array );
                if( dead_worker_ind != NULL )
                    free( dead_worker_ind );
                return;
            }

            if( temp_worker->type == WORKER_TYPE_EVENT_PROCESSOR )
            {
                t_event_count++;

                if( t_event_count > target_event_cnt )
                {
                    delta_array[delta_ind]          = temp_worker;
                    dead_worker_ind[delta_ind]      = tid;
                    temp_worker->commanded_shutdown = true;
                    delta_ind++;
#ifdef C_DEBUG
                    _log(
                        LOG_LEVEL_DEBUG,
                        "event processor %d has been selected for termination",
                        temp_worker->pid
                    );
#endif // C_DEBUG
                }
                else
                {
#ifdef C_DEBUG
                    _log(
                        LOG_LEVEL_DEBUG,
                        "event processor (%d) workers[%d] remaped to temp[%d]",
                        temp_worker->pid,
                        tid,
                        t_index
                    );
#endif // C_DEBUG
                    temp[t_index] = temp_worker;
                    t_index++;
                }
            }
            else if( temp_worker->type == WORKER_TYPE_WORK_PROCESSOR )
            {
                t_work_count++;

                if( t_work_count > target_work_cnt )
                {
                    delta_array[delta_ind]          = temp_worker;
                    dead_worker_ind[delta_ind]      = tid;
                    temp_worker->commanded_shutdown = true;
                    delta_ind++;
#ifdef C_DEBUG
                    _log(
                        LOG_LEVEL_DEBUG,
                        "work processor %d has been selected for termination",
                        temp_worker->pid
                    );
#endif // C_DEBUG
                }
                else
                {
#ifdef C_DEBUG
                    _log(
                        LOG_LEVEL_DEBUG,
                        "work processor (%d) workers[%d] remaped to temp[%d]",
                        temp_worker->pid,
                        tid,
                        t_index
                    );
#endif // C_DEBUG
                    temp[t_index] = temp_worker;
                    t_index++;
                }
            }
        }
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "shutting down %d workers",
            delta_ind
        );
#endif // C_DEBUG
        for( tid = 0; tid < delta_ind; tid++ )
        {
            temp_worker = delta_array[tid];
            while( !((delta_array[tid])->status == STATUS_DEAD ) )
            {
#ifdef C_DEBUG
                _log(
                    LOG_LEVEL_DEBUG,
                    "parent waiting for %d to exit (status %d)...",
                    temp_worker->pid,
                    (delta_array[tid])->status
                );
#endif // C_DEBUG
                sleep( 1 );
                temp_worker->commanded_shutdown = true;
                kill( temp_worker->pid, SIGHUP );
            }
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Child %d has shutdown",
                (delta_array[tid])->pid
            );
#endif // C_DEBUG
        }

#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "All delta workers shutdown"
        );
#endif // C_DEBUG
        for( tid = 0; tid < delta_ind; tid++ )
        {
            temp_worker = workers[dead_worker_ind[tid]];
            workers[dead_worker_ind[tid]] = NULL;
            munmap( temp_worker, sizeof( struct worker ) );
        }

#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Dead worker PID table entries pruned"
        );
#endif // C_DEBUG
        free( dead_worker_ind );
        free( delta_array );
    }
    else
    {
        // temp is larger, remap into temp
        for( tid = 0; tid < ( work_jobs + event_jobs ); tid++ )
        {
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Remapped worker %d workers[%d] to temp[%d]",
                (workers[tid])->pid,
                tid,
                t_index
            );
#endif // C_DEBUG
            temp[t_index] = workers[tid];
            t_index++;
        }
    }

    // Set vals and stash them prior to forking new workers
    work_jobs  = target_work_cnt;
    event_jobs = target_event_cnt;

    // Remap done, pruning done, need to spawn new workers if necessary. t_index will point into the new temp array
    if( w_delta > 0 )
    {
        for( tid = 0; tid < w_delta; tid++ )
        {
            temp_worker = new_worker(
                WORKER_TYPE_WORK_PROCESSOR,
                t_index,
                function,
                me->my_argc,
                me->my_argv,
                NULL
            );

            if( temp_worker == NULL )
            {
#ifdef C_DEBUG
                _log(
                    LOG_LEVEL_ERROR,
                    "Spawning new work queue worker for PID table index %d failed",
                    t_index
                );
#endif // C_DEBUG
                return;
            }

#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Spawned new work queue worker (%d) at temp[%d]",
                temp_worker->pid,
                t_index
            );
#endif // C_DEBUG
            temp[t_index] = temp_worker;
            t_index++;
        }
    }

    if( e_delta > 0 )
    {
        for( tid = 0; tid < e_delta; tid++ )
        {
            temp_worker = new_worker(
                WORKER_TYPE_EVENT_PROCESSOR,
                t_index,
                function,
                me->my_argc,
                me->my_argv,
                NULL
            );

            if( temp_worker == NULL )
            {
#ifdef C_DEBUG
                _log(
                    LOG_LEVEL_ERROR,
                    "Spawning new event queue worker for PID table index %d failed",
                    t_index
                );
#endif // C_DEBUG
                return;
            }

#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Spawned new event queue worker (%d) at temp[%d]",
                temp_worker->pid,
                t_index
            );
#endif // C_DEBUG
            temp[t_index] = temp_worker;
            t_index++;
        }
    }

    if( flag_ptr_refresh )
    {
        for( tid = 0; tid < MAX_WORKERS; tid++ )
        {
            workers[tid] = temp[tid];
        }

        // Need to signal workers that the PID table is updated
        for( tid = 0; tid < ( old_work_jobs + old_event_jobs ); tid++ )
        {
            temp_worker = workers[tid];
            if( temp_worker == NULL )
                continue;
#ifdef C_DEBUG
            _log( LOG_LEVEL_DEBUG, "Entering pointer reload state for %d", temp_worker->pid );
#endif // C_DEBUG
            temp_worker->new_event_jobs    = target_event_cnt;
            temp_worker->new_work_jobs     = target_work_cnt;
            temp_worker->status            = STATUS_REFRESH;
            temp_worker->commanded_refresh = true;
#ifdef C_DEBUG
            _log( LOG_LEVEL_DEBUG, "Entering wait state for %d reload", temp_worker->pid );
#endif // C_DEBUG
            // Indicates it has loaded new ptrs and discarded old pid table entry
            while( !( (old_workers[tid])->status == STATUS_WORKING ) )
            {
#ifdef C_DEBUG
                _log( LOG_LEVEL_DEBUG, "Parent waiting for worker %d to reload", temp_worker->pid );
#endif // C_DEBUG
                temp_worker->commanded_refresh = true;
                kill( temp_worker->pid, SIGHUP );
                sleep( 1 );
            }
#ifdef C_DEBUG
            _log( LOG_LEVEL_DEBUG, "Worker %d has reloaded pointers", temp_worker->pid );
#endif // C_DEBUG
        }

        free( temp );
    }

    return;
}

void _child_update_pointers( void )
{
    struct worker * me = NULL;

    me = get_worker_by_pid();

    if( me == NULL )
        return;

    if( !me->commanded_refresh )
        return;
#ifdef C_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "\nPID %d updating PID table:\n"\
        " old %p \n"\
        " old_wc: %d, new %d \n"\
        " old_ec: %d, new %d \n",
        getpid(),
        workers,
        work_jobs,
        me->new_work_jobs,
        event_jobs,
        me->new_event_jobs
    );
#endif // C_DEBUG
    work_jobs  = me->new_work_jobs;
    event_jobs = me->new_event_jobs;

    me->new_work_jobs     = 0;
    me->new_event_jobs    = 0;
    me->commanded_refresh = false;
    me->status            = STATUS_WORKING;

    // Just sanity check that the PID table lookup works
    me = get_worker_by_pid();

    if( me == NULL || me->pid != getpid() )
    {
        _log(
            LOG_LEVEL_FATAL,
            "Worker %d failed to get updated PID entry",
            getpid()
        );
    }

    return;
}
#endif // ALLOW_OVERRIDE_WORKER_COUNTS

/*
 * void _manage_children( void (*function)( void * )
 *     Task for parent process to run, monitors child processes for
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
    unsigned int    tid      = 0;
    int             wstatus  = 0;
#ifdef C_DEBUG
    _log( LOG_LEVEL_DEBUG, "Parent entering maintenance loop" );
#endif // C_DEBUG
    for( tid = 0; tid < ( event_jobs + work_jobs ); tid++ )
    {
        if( got_sighup )
        {
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Parent received sighup, entering handler"
            );
#endif // C_DEBUG
            _parent_handle_sighup();
        }

#ifdef C_DEBUG
        _log( LOG_LEVEL_DEBUG, "Checking TID %d of workers %p (%p)", tid, workers, workers[tid] );
#endif // C_DEBUG
        if( workers[tid] == NULL )
        {
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Skipping dead worker at index %u",
                tid
            );
#endif // C_DEBUG
            continue;
        }

        worker = workers[tid];
        type   = worker->type;
        pid    = worker->pid;

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
                LOG_LEVEL_WARNING,
                "Found dead worker (%s, pid %d),"\
                " (sigt flag: %s) restarting...",
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
                    if( got_sigterm || got_sigint )
                    {
                        __term();
                    }

                    worker = new_worker(
                        type,
                        tid,
                        function,
                        parent->my_argc,
                        parent->my_argv,
                        worker
                    );

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
                                "Worker pid %d failed to start, "\
                                "marking PID table entry as dead",
                                pid
                            );
                        }

                        worker = NULL;
                        pid    = 0;
                    }

                    workers[tid] = worker;
                }
                else
                {
                    worker->pid    = 0;
                    worker->type   = 0;
                    worker->status = STATUS_DEAD;

                    if( munmap( worker, sizeof( struct worker ) ) != 0 )
                    {
                        _log(
                            LOG_LEVEL_ERROR,
                            "Failed to free worker shared memory"
                        );
                    }

                    workers[tid] = NULL;
                    worker       = NULL;
                }
            }
        }
        else
        {
            // Send a pulse
            worker->last_heartbeat = time( NULL );
        }
    }

#ifdef ALLOW_CONFIG_MANAGER
    pid = config->pid;
    waitpid( pid, &wstatus, WNOHANG );

    if( WIFSIGNALED( wstatus ) ) // Detect abnormal exit
    {
        config->status = STATUS_DEAD;

        _log(
            LOG_LEVEL_WARNING,
            "Worker process %d found dead from signal %d",
            pid,
            WTERMSIG( wstatus )
        );
    }

    wstatus = 0;

    if( config->status == STATUS_DEAD )
    {
        int status;

        _log(
            LOG_LEVEL_WARNING,
            "Config Manager found dead, restarting..."
        );

        waitpid( pid, &status, WNOHANG );

        if( status != 0 )
        {
            // Child terminated abnormally or is in a stopped state
            kill( pid, SIGTERM );
            waitpid( pid, NULL, WNOHANG );
        }

        new_worker(
            WORKER_TYPE_CONFIG_MANAGER,
            0,
            function,
            parent->my_argc,
            parent->my_argv,
            config
        );

        if( config == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to restart config manager"
            );
        }

    }
    else
    {
        config->last_heartbeat = time( NULL );
    }
#endif // ALLOW_CONFIG_MANAGER

    return;
}

void _child_handle_sighup( void )
{
    struct worker * me = NULL;

    me = get_worker_by_pid();

    if( me == NULL )
    {
        _log(
            LOG_LEVEL_WARNING,
            "PID %d got invalid PID table entry",
            getpid()
        );
        got_sighup = false;
        return;
    }

#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
    if( me->status == STATUS_REFRESH )
    {
        _child_update_pointers();
        got_sighup = false;
        me->status = STATUS_WORKING;
        return;
    }
#endif // ALLOW_OVERRIDE_WORKER_COUNTS

    me->status = STATUS_RELOAD;
    _log(
        LOG_LEVEL_INFO,
        "Pid %u got SIGHUP, reloading config",
        ( unsigned int ) getpid()
    );

    if( me->tx_in_progress )
    {
        if( me->conn != NULL && PQstatus( me->conn ) == CONNECTION_OK )
        {
            PQexec( me->conn, "ROLLBACK" );
            me->tx_in_progress = false;

#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Successfully rolled back in progress transaction"
            );
#endif // C_DEBUG
        }
        else
        {
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "Transaction is in progress but conn handle is dead"
            );
#endif // C_DEBUG
        }
    }

    /*
     *  Note: some connection poolers *cough* pgbouncer, pgpool will cache
     *  GUC values. We will force a reconnect (and hopefully open up a new pool
     *  in the process such that we get the latest value of our GUCs for tasks
     *  such as get_uid / set_uid and REST calls
     */
    if( me->conn != NULL )
    {
        PQfinish( me->conn );
    }

    me->conn           = NULL;
    me->tx_in_progress = false;

    signal ( SIGHUP, __sighup );

    me->status = STATUS_WORKING;
#ifdef C_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "Child reset status to working"
    );
#endif // C_DEBUG
    got_sighup = false;
    return;
}

void _parent_handle_sighup( void )
{
    bool all_ack       = false;
    int  sleep_backoff = 0;
    int  busy_count    = 0;
    int  i             = 0;
    int  wstatus       = 0;

    while( all_ack == false )
    {
        sleep( 2 + sleep_backoff );
        busy_count = 0;
        all_ack = true;

        for( i = 0; i < ( work_jobs + event_jobs ); i++ )
        {
            if( workers[i] != NULL )
            {
                if( workers[i]->status != STATUS_WORKING )
                {
                    all_ack = false;

                    if( workers[i]->tx_in_progress )
                    {
                        busy_count++;
#ifdef C_DEBUG
                        _log(
                            LOG_LEVEL_DEBUG,
                            "Worker %d is in a transaction and cannot "\
                            "process SIGHUP",
                            workers[i]->pid
                        );
#endif // C_DEBUG
                        if( sleep_backoff == 0 )
                        {
                            // Use MAX_LOG_WAIT to extend the sleep time
                            sleep_backoff = MAX_LOCK_WAIT;
                        }
                    }
                    else
                    {
#ifdef C_DEBUG
                        _log(
                            LOG_LEVEL_DEBUG,
                            "The following worker has failed to re-enter "\
                            "working state following SIGHUP"
                         );
                        _debug_worker_slot( workers[i] );
#endif // C_DEBUG
                    }

                    waitpid( workers[i]->pid, &wstatus, WNOHANG );

                    if( WIFSIGNALED( wstatus ) )
                    {
#ifdef C_DEBUG
                        _log(
                            LOG_LEVEL_DEBUG,
                            "FYI worker exited with status %d",
                            WTERMSIG( wstatus )
                        );
#endif // C_DEBUG
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
            if( busy_count > 0 )
            {
#ifdef C_DEBUG
                _log(
                    LOG_LEVEL_DEBUG,
                    "There are %u worker(s) that are currently "\
                    "processing items",
                    busy_count
                );
#endif // C_DEBUG
            }
            else
            {
#ifdef C_DEBUG
                _log(
                    LOG_LEVEL_WARNING,
                    "Not all workers have ACK'd the SIGHUP"
                );
#endif // C_DEBUG
            }
        }
    }

    got_sighup = false;
    return;
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
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "NULL process title provided"
        );
#endif // C_DEBUG
        return;
    }

    if( argv == NULL )
    {
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "ARGV is null :("
        );
#endif // C_DEBUG
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

#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Argv total size: %u",
            size
        );
#endif // C_DEBUG
        *max_size = size;
    }

    size = *max_size;
    memset( argv[0], '\0', size );
    strncpy( argv[0], title, strlen( title ) );
    argv[0][strlen(title)] = '\0';
    return;
}

void _debug_worker_slot( struct worker * worker )
{
    if( worker == NULL )
    {
        return;
    }
#ifdef C_DEBUG
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
        "ARGV: %p,\n"\
        "pidfile: %s\n",
        worker,
        worker->dequeue_function,
        worker->channel,
        worker->conn,
        worker->curl_handle,
        (int) worker->pid,
        worker->type == WORKER_TYPE_PARENT ? "PARENT" :
            worker->type == WORKER_TYPE_EVENT_PROCESSOR ? "EVENT" :
            worker->type == WORKER_TYPE_WORK_PROCESSOR ? "WORK" :
#ifdef ALLOW_CONFIG_MANAGER
            worker->type == WORKER_TYPE_CONFIG_MANAGER ? "CONFIG " :
#endif // ALLOW_CONFIG_MANAGER
            "UNKNOWN",
        worker->tx_in_progress == true ? "YES" : "NO",
        worker->enable_curl == true ? "YES" : "NO",
        worker->status == STATUS_DEAD ? "DEAD" :
            worker->status == STATUS_STARTUP ? "STARTUP" :
#ifdef ALLOW_OVERRIDE_WORKER_COUNTS
            worker->status == STATUS_REFRESH ? "REFRESH" :
#endif // ALLOW_OVERRIDE_WORKER_COUNTS
            worker->status == STATUS_WORKING ? "WORKING" :
            worker->status == STATUS_RELOAD ? "RELOAD" :
            "UNKNOWN",
        worker->my_argc,
        worker->my_argv,
        worker->pidfile
    );
#endif // C_DEBUG
    return;
}
