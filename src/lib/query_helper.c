/*------------------------------------------------------------------------
 *
 * query_helper.c
 *     Query parameterization functions and struct
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        query_helper.c
 *
 *------------------------------------------------------------------------
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <regex.h>
#include <signal.h>
#include <stdbool.h>
#include <math.h>

#include "util.h"

#define MAX_REGEX_GROUPS 1
#define MAX_REGEX_MATCHES 100

#define LOG_LEVEL_WARNING "WARNING"
#define LOG_LEVEL_ERROR "ERROR"
#define LOG_LEVEL_FATAL "FATAL"
#define LOG_LEVEL_DEBUG "DEBUG"
#define LOG_LEVEL_INFO "INFO"

struct query {
    char *  query_string;
    int     length;
    char ** _bind_list;
    int     _bind_count;
};

struct query * _new_query( char * );
struct query * _add_parameter_to_query( struct query *, char *, char * );
void _free_query( struct query * );
void _debug_struct( struct query * );

struct query * _new_query( char * query_string )
{
    struct query * query_object = NULL;

    query_object = ( struct query * ) malloc( sizeof( struct query ) );

    if( query_object == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for query parameterization structure"
        );
        return NULL;
    }

    query_object->length = strlen( query_string );

    query_object->query_string = ( char * ) malloc(
        sizeof( char )
      * ( query_object->length + 1 )
    );

    if( query_object->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for query string"
        );

        free( query_object );
        return NULL;
    }

    strcpy( (char * )( query_object->query_string ), query_string );
    query_object->_bind_count = 0;
    query_object->_bind_list = NULL;

    return query_object;
}

struct query * _add_parameter_to_query( struct query * query_object, char * key, char * value )
{
    regex_t    regex;
    regmatch_t matches[MAX_REGEX_GROUPS + 1];
    char *     temp_query;
    int        reg_result = 0;
    int        i;

    int    bind_length       = 0;
    int    bind_counter      = 0;
    char * bindpoint_search  = NULL;
    char * bindpoint_replace = NULL;

    if( query_object->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot parameterize NULL query string"
        );
        _free_query( query_object );
        return NULL;
    }

    bindpoint_search = ( char * ) malloc(
        sizeof( char )
      * ( strlen( key ) + 7 )
    );

    if( bindpoint_search == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for regular expression"
        );
        _free_query( query_object );
        return NULL;
    }

    strcpy( bindpoint_search, "[?]" );
    strcat( bindpoint_search, key );
    strcat( bindpoint_search, "[?]" );

    reg_result = regcomp( &regex, bindpoint_search, REG_EXTENDED );

    if( reg_result != 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to compile bindpoint search regular expression"
        );
        free( bindpoint_search );
        _free_query( query_object );
        return NULL;
    }

    bindpoint_replace = ( char * ) malloc(
        sizeof( char )
      * (int) ( floor( log10( abs( query_object->_bind_count + 1 ) ) ) + 3 )
    );

    if( bindpoint_replace == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for replacement string in regular expression"
        );
        free( bindpoint_search );
        _free_query( query_object );
        regfree( &regex );
        return NULL;
    }

    sprintf( bindpoint_replace, "$%d", query_object->_bind_count + 1 );

    for( i = 0; i < MAX_REGEX_MATCHES; i++ )
    {
        reg_result = regexec(
            &regex,
            query_object->query_string,
            MAX_REGEX_GROUPS,
            matches,
            0
        );

        if( matches[0].rm_so == -1 || reg_result == REG_NOMATCH )
        {
            break;
        }
        else if( reg_result != 0 )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to execute regular expression search"
            );
            free( bindpoint_search );
            _free_query( query_object );
            free( bindpoint_replace );
            regfree( &regex );
            return NULL;
        }

        bind_counter++;

        bind_length = matches[0].rm_eo - matches[0].rm_so;
        temp_query = ( char * ) malloc(
            sizeof( char )
          * (
                strlen( query_object->query_string )
              - bind_length
              + strlen( bindpoint_replace )
              + 1
            )
        );

        if( temp_query == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for string resize operation"
            );
            free( bindpoint_search );
            free( query_object );
            free( bindpoint_replace );
            regfree( &regex );
            return NULL;
        }

        strncpy( temp_query, query_object->query_string, matches[0].rm_so );
        strcat( temp_query, bindpoint_replace );
        strcat( temp_query, ( char * ) ( query_object->query_string + matches[0].rm_eo ) );

        free( query_object->query_string );

        query_object->query_string = ( char * ) malloc(
            sizeof( char )
          * ( strlen( temp_query ) + 1 )
        );

        if( query_object->query_string == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for resized string"
            );
            free( bindpoint_search );
            _free_query( query_object );
            free( bindpoint_replace );
            regfree( &regex );
            free( temp_query );
            return NULL;
        }

        strcpy( query_object->query_string, temp_query );
        query_object->length = strlen( temp_query );
        free( temp_query );
    }

    free( bindpoint_search );
    free( bindpoint_replace );
    regfree( &regex );

    if( bind_counter > 0 )
    {
        if( query_object->_bind_count == 0 )
        {
            query_object->_bind_list = ( char ** ) malloc(
                sizeof( char * )
            );
        }
        else
        {
            query_object->_bind_list = ( char ** ) realloc(
                query_object->_bind_list,
                sizeof( char * ) * ( query_object->_bind_count + 1 )
            );
        }

        if( query_object->_bind_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for query parameter"
            );

            _free_query( query_object );
            return NULL;
        }

        query_object->_bind_list[query_object->_bind_count] = ( char * ) malloc(
            sizeof( char )
          * ( strlen( value ) + 1 )
        );
        strcpy( query_object->_bind_list[query_object->_bind_count], value );
        query_object->_bind_count = query_object->_bind_count + 1;
    }

    return query_object;
}

void _debug_struct( struct query * obj )
{
    int i;
    _log( LOG_LEVEL_DEBUG, "Query object: " );
    _log( LOG_LEVEL_DEBUG, "==============" );
    _log( LOG_LEVEL_DEBUG, "query_string: '%s'", obj->query_string );
    _log( LOG_LEVEL_DEBUG, "length: %d", obj->length );
    _log( LOG_LEVEL_DEBUG, "_bind_count: %d", obj->_bind_count );
    _log( LOG_LEVEL_DEBUG, "_bind_list: " );

    for( i = 0; i < obj->_bind_count; i++ )
    {
        _log( LOG_LEVEL_DEBUG, "%d: '%s'", i, obj->_bind_list[i] );
    }
    return;
}

void _free_query( struct query * query_object )
{
    int i;
    if( query_object == NULL )
    {
        return;
    }

    if( query_object->query_string != NULL )
    {
        free( query_object->query_string );
    }

    if( query_object->_bind_list != NULL )
    {
        if( query_object->_bind_count > 0 )
        {
            for( i = 0; i < query_object->_bind_count; i++ )
            {
                if( query_object->_bind_list[i] != NULL )
                {
                    free( query_object->_bind_list[i] );
                }
            }
        }

        free( query_object->_bind_list );
    }

    return;
}
