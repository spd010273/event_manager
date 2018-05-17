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

sig_atomic_t got_sighup;
sig_atomic_t got_sigterm;

bool event_listener;
bool work_listener;

char * conninfo;

void _parse_args( int, char ** );
void _usage( char * ) __attribute__ ((noreturn));
void _log( char *, char *, ... ) __attribute__ ((format (gnu_printf, 2, 3)));
void __sigterm( int );
void __sighup( int );

#endif
