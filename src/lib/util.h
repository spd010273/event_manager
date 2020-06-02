/*------------------------------------------------------------------------
 *
 * util.h
 *     Utility function prototypes and process managment routines
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        util.h
 *
 *------------------------------------------------------------------------
 */

#ifndef UTIL_H
#define UTIL_H

#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <regex.h>
#include <curl/curl.h>
#include <signal.h>
#include <libpq-fe.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <limits.h>
#include <sys/user.h>
#include <pwd.h>
#include <dirent.h>
#include <time.h>
#include <sys/time.h>

#define LOG_LEVEL_WARNING "WARNING"
#define LOG_LEVEL_ERROR "ERROR"
#define LOG_LEVEL_FATAL "FATAL"
#define LOG_LEVEL_DEBUG "DEBUG"
#define LOG_LEVEL_INFO "INFO"

#define STATUS_DEAD 1
#define STATUS_STARTUP 2
#define STATUS_WORKING 3
#define STATUS_RELOAD 4

#define ALLOW_WORKER_RESTART true
#define MAX_WORKERS 16
#define MAX_LOCK_WAIT 5 // Seconds

// Worker Types
#define WORKER_TYPE_EVENT_PROCESSOR 1
#define WORKER_TYPE_WORK_PROCESSOR 2
#define WORKER_TYPE_PARENT 3

#define WORKER_TITLE_EVENT_PROCESSOR "event queue processor"
#define WORKER_TITLE_WORK_PROCESSOR "work queue processor"
#define WORKER_TITLE_PARENT "Event Manager parent process"

#define LOG_FILE_NAME "/var/log/event_manager/event_manager.log"

/*
 *  Structure used to store worker initialization at fork time,
 *  as well as store worker specific handles
 *
 *  Notes on access patterns:
 *      - Parent allocates worker slot
 *      - Children write to the worker slot once init'd
 *      - Parent only reads data from the worker slot
 *      - Children will attempt to do their own cleanup if term'd (see __term)
 *      - Parent will verify that worker slots are cleaned (see __term)
 *
 *  Signal flag rules:
 *      SIGINT:
 *          This is received by the parent as the children may be in a blocking
 *          select() operation. The parent sets the got_sigint flag ( so any
 *          non-blocked children can clean up ), then enters the __term loop
 *          where it will SIGTERM / SIGKILL child processes
 *      SIGTERM:
 *          Similar to above, the parent starts terminating processes once it
 *          enters __term()
 *      SIGHUP:
 *          We're expecting the parent to receive this, in which case the
 *          parent enters a short maintenance routing where it echos the
 *          SIGHUP to all children and verifies that they've acked the HUP.
 *          The children will rollback all transactions and terminate their
 *          database handles, then re-enter their main loops causing them to
 *          re-open their DB handles. The reconnect is required to be able to
 *          read changes in GUC values when behind a connection pooler such as
 *          pgbouncer of pgpool-II.
 *
 */

struct worker {
    int (*dequeue_function)( struct worker * );
    const char *   channel;
    PGconn *       conn;
    CURL *         curl_handle;
    pid_t          pid;
    unsigned short type;
    bool           tx_in_progress;
    bool           enable_curl;
    unsigned short status;
    int            my_argc;
    char **        my_argv;
    char *         pidfile; // used only by parent
    time_t         tx_start;
    unsigned int   tx_success;  // Number of successful tx since last update
    unsigned int   tx_fail;     // Number of failed tx since last update
    double         tx_duration; // Duration of all tx since last update, in seconds
    bool           stat_update; // Semaphore for stat collector routine
    time_t         last_heartbeat; // Workaround for systemd not reaping children
};

// for by-type rollup
struct em_stat {
    unsigned int tx_success;
    unsigned int tx_fail;
    double       tx_duration;
};

// Global variables
unsigned int event_jobs;
unsigned int work_jobs;

bool daemonize;
bool single_step_only;
char * conninfo;
FILE * log_file;

volatile sig_atomic_t got_sighup;
volatile sig_atomic_t got_sigterm;
volatile sig_atomic_t got_sigint;

struct worker ** workers;
struct worker * parent;

void _parse_args( int, char ** );
void _usage( char * ) __attribute__ ((noreturn));
void _log( char *, char *, ... ) __attribute__ ((format (gnu_printf, 2, 3)));

// Functions
struct worker * new_worker(
    unsigned short,
    unsigned int,
    void (*function)( void * ),
    int,
    char **,
    struct worker *
);

void free_worker( struct worker * worker );

struct worker * get_worker_by_pid( void );
bool logrotate( struct worker * );
bool parent_init( int, char ** );
void * create_shared_memory( size_t );
void _manage_children( void (*function)( void * ) );

void __sigterm( int ) __attribute__ ((noreturn));
void __sigint( int ) __attribute__ ((noreturn));
void __sighup( int );
void __term( void ) __attribute__ ((noreturn));
void __sighup_sigaction( int, siginfo_t *, void * );
void _register_signal_handlers( void );

void _gather_child_stats_to_self( struct em_stat ** );
void _update_stats(
    struct worker *,
    unsigned int,
    unsigned int,
    double
);

// Mutex helpers
bool _wait_and_set_mutex( struct worker * );
bool __test_and_set( struct worker * );

void _parent_handle_sighup( void );
void _child_handle_sighup( void );

void _set_process_title( char **, int, char *, unsigned int * );

void _debug_worker_slot( struct worker * );
#endif // UTIL_H
