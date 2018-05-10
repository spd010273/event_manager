#include "postgres.h"
#include "pg_config.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"

#if PG_VERSION_NUM >= 90600
#include "access/parallel.h"
#endif

#if PG_VERSION_NUM < 90300
    // Not supported :(
#endif

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1( events_launch );

/* Prototypes */
void _PG_init( void );
void _PG_fini( void );
static void events_sigterm( SIGNAL_ARGS );
static void events_sighup( SIGNAL_ARGS );
void events_bgw_event_main( Datum ) pg_attribute_noreturn();
void events_bgw_work_main( Datum ) pg_attribute_noreturn();
Datum event_manager_event_worker_main( PG_FUNCTION_ARGS );
Datum event_manager_work_worker_main( PG_FUNCTION_ARGS );

/* Latches */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUCs */
static char * event_manager_asynchronous = "on";
static char * event_manager_setuid_function = NULL;
static char * event_manager_getuid_function = NULL;
static int    event_manager_event_queue_worker_count = 1;
static int    event_manager_work_queue_worker_count = 1;

/* Misc Globals */
static int events_sleep_time   = 10;
char * events_schema = NULL;

#define PROCESS_NAME "events_bgw"
#define WORKER_NAME "events_bgw"

/* Signal Handlers */
static void events_sigterm( SIGNAL_ARGS )
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch( MyLatch );
    errno = save_errno;
}


static void events_sighup( SIGNAL_ARGS )
{
    int save_errno = errno;
    got_sighup = true;
    SetLatch( MyLatch );
    errno = save_errno;
}

/* Initializer */
static void initialize_events( void )
{
    StringInfoData buffer;
    int ret;
    bool isnull;

    initStringInfo( &buffer );

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect(); // Changes palloc context
    PushActiveSnapshot( GetTransactionSnapshot() );
    pgstat_report_activity( STATE_RUNNING, "Initializing Events Manager" );

    /* Before we start - ensure event_manager extension is installed */
    appendStringInfo(
        &buffer,
        "SELECT extname FROM pg_catalog.pg_extension WHERE extname = 'event_manager'"
    );

    pgstat_report_activity( STATE_RUNNING, buffer.data );

    ret = SPI_execute( buffer.data, true, 1 );

    if( ret != SPI_OK_SELECT )
    {
        elog(
            FATAL,
            "Cannot determine if event_manager is installed on database'. Got error code: %d",
            ret
        );
    }

    if( SPI_processed <= 0 )
    {
        elog(
            DEBUG1,
            "Event Manager is not installed on database. Exiting."
        );

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity( STATE_IDLE, NULL );

        proc_exit( 1 );
    }

    resetStringInfo( &buffer );
    appendStringInfo(
        &buffer,
        "    SELECT n.nspname "
        "      FROM pg_catalog.pg_extension e "
        "INNER JOIN pg_catalog.pg_namespace n "
        "        ON n.oid = e.extnamespace "
        "     WHERE e.extname = 'event_manager'"
    );

    pgstat_report_activity( STATE_RUNNING, buffer.data );

    ret = SPI_execute( buffer.data, true, 1 );

    if( ret != SPI_OK_SELECT )
    {
        elog(
            ERROR,
            "Cannot determine event_manager schema from catalog. Got error code: %d",
            ret
        );

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity( STATE_IDLE, NULL );

        proc_exit( 1 );
    }

    if( SPI_processed <= 0 )
    {
        elog(
            FATAL,
            "Could not determine the event_manager schema. Is it installed?"
        );

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity( STATE_IDLE, NULL );

        proc_exit( 1 );
    }

    events_schema = DatumGetCString(
        SPI_getbinval(
            SPI_tuptable->vals[0],
            SPI_tuptable->tupdesc,
            1,
            &isnull
        )
    );

    if( isnull )
    {
        elog(
            FATAL,
            "Query to determine event_manager schema returned NULL."
        );

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity( STATE_IDLE, NULL );

        proc_exit( 1 );
    }

    SPI_finish(); // Closes function context, frees pallocs
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity( STATE_IDLE, NULL );

    return;
}

/* Worker Mains */
void events_bgw_event_main( Datum main_arg )
{
    int index = DatumGetInt32( main_arg );

    StringInfoData buffer; // Used for strings n such

    /* Setup signal handlers */
    pqsignal( SIGHUP, events_sighup );
    pqsignal( SIGTERM, events_sigterm );

    initStringInfo( &buffer );
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection( "postgres", NULL );

    // probably should log that we're alive
    initialize_events();
    /* Main Loop */
    while( !got_sigterm )
    {
        int rc;

        #if ( PG_VERSION_NUM >= 100000 )
        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            events_sleep_time * 1000L,
            PG_WAIT_EXTENSION
        );
        #endif
        #if( PG_VERSION_NUM < 100000 )
        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            events_sleep_time * 1000L
        );
        #endif
        ResetLatch( MyLatch );

        if( rc & WL_POSTMASTER_DEATH )
        {
            // Something bad probably happened, let's GTFO
            proc_exit( 1 );
        }

        CHECK_FOR_INTERRUPTS();

        if( got_sighup )
        {
            got_sighup = false;
            ProcessConfigFile( PGC_SIGHUP );
        }

        elog(
            DEBUG1,
            "Event Manager event processor (%d) got SIGHUP: %d",
            index,
            got_sighup
        );

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot( GetTransactionSnapshot() );
        pgstat_report_activity( STATE_RUNNING, "Event Manager Event Processor" );

        /* D O   S O M E   S T U F F */

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat( false );
        pgstat_report_activity( STATE_IDLE, NULL );
    }

    if( got_sigterm )
    {
        elog(
            LOG,
            "Event Manager event processor (%d) got SIGTERM: %d. Exiting.",
            index,
            got_sigterm
        );
    }

    proc_exit( 1 );
}

void events_bgw_work_main( Datum main_arg )
{
    int index = DatumGetInt32( main_arg );

    StringInfoData buffer; // Used for strings n such

    /* Setup signal handlers */
    pqsignal( SIGHUP, events_sighup );
    pqsignal( SIGTERM, events_sigterm );

    initStringInfo( &buffer );
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection( "postgres", NULL );

    // probably should log that we're alive
    initialize_events();
    /* Main Loop */
    while( !got_sigterm )
    {
        int rc;

        #if ( PG_VERSION_NUM >= 100000 )
        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            events_sleep_time * 1000L,
            PG_WAIT_EXTENSION
        );
        #endif
        #if( PG_VERSION_NUM < 100000 )
        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            events_sleep_time * 1000L
        );
        #endif
        ResetLatch( MyLatch );

        if( rc & WL_POSTMASTER_DEATH )
        {
            // Something bad probably happened, let's GTFO
            proc_exit( 1 );
        }

        CHECK_FOR_INTERRUPTS();

        if( got_sighup )
        {
            got_sighup = false;
            ProcessConfigFile( PGC_SIGHUP );
        }

        elog(
            DEBUG1,
            "Event Manager work processor (%d) got SIGHUP: %d",
            index,
            got_sighup
        );

        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot( GetTransactionSnapshot() );
        pgstat_report_activity( STATE_RUNNING, "Event Manager Work Processor" );

        /* D O   S O M E   S T U F F */

        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat( false );
        pgstat_report_activity( STATE_IDLE, NULL );
    }

    if( got_sigterm )
    {
        elog(
            LOG,
            "Event Manager work processor (%d) got SIGTERM: %d. Exiting.",
            index,
            got_sigterm
        );
    }

    proc_exit( 1 );
}

/* Entrypoint of this module */
void _PG_init( void )
{
    BackgroundWorker event_worker;
    BackgroundWorker work_worker;
    unsigned int i;

    if( !process_shared_preload_libraries_in_progress )
    {
        return;
    }

    DefineCustomIntVariable(
        "events.event_processor_count",
        "Number of event_queue workers to start.",
        NULL,
        &event_manager_event_queue_worker_count,
        1,
        1,
        50,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomIntVariable(
        "events.work_processor_count",
        "Number of work_queue workers to start.",
        NULL,
        &event_manager_work_queue_worker_count,
        1,
        1,
        50,
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomStringVariable(
        "events.asynchronous",
        "Whether Event Manager operates in Asynchronous or Synchronous mode.",
        "Asynchronous mode = 'on', Synchronous mode = 'off'",
        &event_manager_asynchronous,
        "on",
        PGC_SIGHUP,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomStringVariable(
        "events.setuid_function",
        "Function call to set any session level UID (user ID) for the application",
        "Requires bindpoints ?uid? within the function parameters, ex: fn_set_uid( ?uid? )",
        &event_manager_setuid_function,
        NULL,
        PGC_SIGHUP,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomStringVariable(
        "events.getuid_function",
        "Function call to get the current session UID (user ID) for the application",
        NULL,
        &event_manager_getuid_function,
        NULL,
        PGC_SIGHUP,
        0,
        NULL,
        NULL,
        NULL
    );

    memset( &event_worker, 0, sizeof( event_worker ) );
    memset( &work_worker, 0, sizeof( work_worker ) );

    /* Setup our access flags - We'll need a connection and access to SHM */
    event_worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    work_worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

    event_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    event_worker.bgw_restart_time = BGW_NEVER_RESTART;

    work_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    work_worker.bgw_restart_time = BGW_NEVER_RESTART;

    sprintf( event_worker.bgw_library_name, "event_manager_event_worker" );
    sprintf( event_worker.bgw_function_name, "event_manager_event_worker_main" );

    sprintf( work_worker.bgw_library_name, "event_manager_work_worker" );
    sprintf( work_worker.bgw_function_name, "event_manager_work_worker_main" );

    event_worker.bgw_notify_pid = 0;
    work_worker.bgw_notify_pid = 0;

    for( i = 1; i <= event_manager_event_queue_worker_count; i++ )
    {
        snprintf( event_worker.bgw_name, BGW_MAXLEN, "event manager %d (event processor)", i );
        event_worker.bgw_main_arg = Int32GetDatum( i );

        RegisterBackgroundWorker( &event_worker );
    }

    for( i = 1; i <= event_manager_work_queue_worker_count; i++ )
    {
        snprintf( work_worker.bgw_name, BGW_MAXLEN, "event manager %d (work processor)", i );
        work_worker.bgw_main_arg = Int32GetDatum( i );

        RegisterBackgroundWorker( &work_worker );
    }
}

/* BGW setup */
Datum event_manager_event_worker_main( PG_FUNCTION_ARGS )
{
    int32 i = PG_GETARG_INT32( 0 );
    BackgroundWorker worker;
    BackgroundWorkerHandle * handle;
    BgwHandleStatus status;
    pid_t pid;

    memset( &worker, 0, sizeof( worker ) );
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf( worker.bgw_library_name, "events_bgw" );
    sprintf( worker.bgw_function_name, "events_bgw_event_main" );
    snprintf( worker.bgw_name, BGW_MAXLEN, "events_%d", i );
    worker.bgw_main_arg = Int32GetDatum( i );

    worker.bgw_notify_pid = MyProcPid;

    if( !RegisterDynamicBackgroundWorker( &worker, &handle ) )
    {
        PG_RETURN_NULL();
    }

    status = WaitForBackgroundWorkerStartup( handle, &pid );

    if( status == BGWH_STOPPED )
    {
        ereport(
            ERROR,
            (
                errcode( ERRCODE_INSUFFICIENT_RESOURCES ),
                errmsg( "Could not start background process" ),
                errhint( "Check server logs." )
            )
        );
    }

    if( status == BGWH_POSTMASTER_DIED )
    {
        /* R.I.P. Gone, but not forgotten. */
        ereport(
            ERROR,
            (
                errcode( ERRCODE_INSUFFICIENT_RESOURCES ),
                errmsg( "Cannot start background process without postmaster" ),
                errhint( "Kill all remaining database processes and restart the database" )
            )
        );
    }

    Assert( status == BGWH_STARTED );

    PG_RETURN_INT32( pid );
}

Datum event_manager_work_worker_main( PG_FUNCTION_ARGS )
{
    int32 i = PG_GETARG_INT32( 0 );
    BackgroundWorker worker;
    BackgroundWorkerHandle * handle;
    BgwHandleStatus status;
    pid_t pid;

    memset( &worker, 0, sizeof( worker ) );
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf( worker.bgw_library_name, "events_bgw" );
    sprintf( worker.bgw_function_name, "events_bgw_work_main" );
    snprintf( worker.bgw_name, BGW_MAXLEN, "events_%d", i );
    worker.bgw_main_arg = Int32GetDatum( i );

    worker.bgw_notify_pid = MyProcPid;

    if( !RegisterDynamicBackgroundWorker( &worker, &handle ) )
    {
        PG_RETURN_NULL();
    }

    status = WaitForBackgroundWorkerStartup( handle, &pid );

    if( status == BGWH_STOPPED )
    {
        ereport(
            ERROR,
            (
                errcode( ERRCODE_INSUFFICIENT_RESOURCES ),
                errmsg( "Could not start background process" ),
                errhint( "Check server logs." )
            )
        );
    }

    if( status == BGWH_POSTMASTER_DIED )
    {
        ereport(
            ERROR,
            (
                errcode( ERRCODE_INSUFFICIENT_RESOURCES ),
                errmsg( "Cannot start background process without postmaster" ),
                errhint( "Kill all remaining database processes and restart the database" )
            )
        );
    }

    Assert( status == BGWH_STARTED );

    PG_RETURN_INT32( pid );
}
