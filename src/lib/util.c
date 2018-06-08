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

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include "util.h"

#define VERSION 0.1

bool event_listener = false;
bool work_listener  = false;

char * conninfo = NULL;

static const char * usage_string = "\
Usage: event_manager \
    -U DB User (default: postgres) \
    -p DB Port (default: 5432) \
    -h DB Host (default: localhost) \
    -d DB name (default: DB User) \
    -E | -W Start Event or Work Queue Processor, respectively \
  [ -D debug mode \
    -v VERSION \
    -? HELP ]";

void _parse_args( int argc, char ** argv )
{
    int    c        = 0;
    char * username = NULL;
    char * dbname   = NULL;
    char * port     = NULL;
    char * hostname = NULL;

    opterr = 0;

    while( ( c = getopt( argc, argv, "U:p:d:h:v?EW" ) ) != -1 )
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
                event_listener = true;
                break;
            case 'W':
                work_listener = true;
                break;
            default:
                _usage( "Invalid argument." );
        }
    }

    if( event_listener == true && work_listener == true )
    {
        _usage( "Event and Work queue processing modes are mutually exclusive" );
    }

    if( event_listener == false && work_listener == false )
    {
        _usage( "Need to instruct program to listen to events (-E) or work (-W)" );
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

void _usage( char * message )
{
    if( message != NULL )
    {
        printf( "%s\n", message );
    }

    printf( "%s", usage_string );

    exit( 1 );
}

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
        free( conninfo );

        exit( 1 );
    }

    return;
}
