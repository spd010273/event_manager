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

#define LOG_LEVEL_WARNING "WARNING"
#define LOG_LEVEL_ERROR "ERROR"
#define LOG_LEVEL_FATAL "FATAL"
#define LOG_LEVEL_DEBUG "DEBUG"
#define LOG_LEVEL_INFO "INFO"

bool event_listener;
bool work_listener;

char * conninfo;

void _parse_args( int, char ** );
void _usage( char * ) __attribute__ ((noreturn));
void _log( char *, char *, ... ) __attribute__ ((format (gnu_printf, 2, 3)));

#endif
