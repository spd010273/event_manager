/*------------------------------------------------------------------------
 *
 * query_helper.h
 *     Prototypes for query parameterization functionality
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        query_helper.h
 *
 *------------------------------------------------------------------------
 */

#ifndef QUERY_HELPER_H
#define QUERY_HELPER_H
#include <curl/curl.h>
#include "jsmn/jsmn.h"
struct query {
    char *  query_string;
    int     length;
    char ** _bind_list;
    int     _bind_count;
};

struct query * _new_query( char * );
struct query * _finalize_query( struct query * );
struct query * _add_parameter_to_query( struct query *, char *, char * );
struct query * _add_json_parameter_to_query( struct query *, char *, char * );
void _free_query( struct query * );
void _debug_struct( struct query * );
jsmntok_t * json_tokenise( char *, int * );
char * _add_json_parameters_to_param_list( CURL *, char *, char *, int * );

#endif
