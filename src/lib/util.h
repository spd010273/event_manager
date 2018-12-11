/*------------------------------------------------------------------------
 *
 * util.h
 *     Utility function prototypes
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
#include <errno.h>

#define LOG_LEVEL_WARNING "WARNING"
#define LOG_LEVEL_ERROR "ERROR"
#define LOG_LEVEL_FATAL "FATAL"
#define LOG_LEVEL_DEBUG "DEBUG"
#define LOG_LEVEL_INFO "INFO"

#define STATUS_DEAD 0
#define STATUS_STARTUP 1
#define STATUS_WORKING 2

#define ALLOW_WORKER_RESTART true
#define MAX_WORKERS 16

// Worker Types
#define WORKER_TYPE_EVENT_PROCESSOR 1
#define WORKER_TYPE_WORK_PROCESSOR 2
#define WORKER_TYPE_PARENT 3
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
};

unsigned int  event_jobs;
unsigned int  work_jobs;

char * conninfo;
sig_atomic_t got_sighup;
sig_atomic_t got_sigterm;
sig_atomic_t got_sigint;
struct worker ** workers;
struct worker * parent;

void _parse_args( int, char ** );
void _usage( char * ) __attribute__ ((noreturn));
void _log( char *, char *, ... ) __attribute__ ((format (gnu_printf, 2, 3)));
void free_worker( struct worker * worker );
struct worker * new_worker( unsigned short, unsigned int, void (*function)( void * ) );
struct worker * get_worker_by_pid( void );
bool parent_init( void );

void __sigterm( int ) __attribute__ ((noreturn));
void __sigint( int ) __attribute__ ((noreturn));
void __sighup( int );
void __term( void ) __attribute__ ((noreturn));

bool _rollback_transaction( struct worker * );
bool _commit_transaction( struct worker * );
bool _begin_transaction( struct worker * );
void * create_shared_memory( size_t );
void _manage_children( void (*function)( void * ) ) __attribute__ ((noreturn));
#endif
