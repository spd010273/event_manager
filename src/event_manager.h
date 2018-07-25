/*------------------------------------------------------------------------
 *
 * event_manager.h
 *     Prototypes for main event / work handlers and helper functions
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        event_manager.h
 *
 *------------------------------------------------------------------------
 */

#ifndef EVENT_MANAGER_H
#define EVENT_MANAGER_H
#include <libpq-fe.h>

// Structures
struct curl_response {
    char * pointer;
    size_t size;
};

struct action_result {
    char * query;
    char * uri;
    char * method;
    bool use_ssl;
    char * parameters;
    char * static_parameters;
    char * session_values;
    char * uid;
    char * transaction_label;
    char * recorded;
};

/* Function Prototypes */
// Main functions
void _queue_loop( const char *, int (*)(void) );
int work_queue_handler( void );
int event_queue_handler( void );
bool execute_action( PGresult *, int );
bool execute_action_query( struct action_result * );
bool execute_remote_uri_call( struct action_result * );
bool set_uid( char *, char * );
static size_t _curl_write_callback( void *, size_t, size_t, void * );

// Helper functions
PGresult * _execute_query( char *, char **, int );
char * get_column_value( int, PGresult *, char * );
bool is_column_null( int, PGresult *, char * );
bool _rollback_transaction( void );
bool _commit_transaction( void );
bool _begin_transaction( void );
void set_session_gucs( char * );
void clear_session_gucs( char * );

// Integration functions
void _cyanaudit_integration( char * );

// Signal Handlers
void __sigterm( int ) __attribute__ ((noreturn));
void __sighup( int );

// Program Entry
int main( int, char ** );

#endif
