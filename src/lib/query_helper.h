/*------------------------------------------------------------------------
 *
 * query_helper.h
 *     Prototypes for query parameterization functionality
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        src/lib/query_helper.h
 *
 *------------------------------------------------------------------------
 */

#ifndef QUERY_HELPER_H
#define QUERY_HELPER_H
#include <curl/curl.h>
#include "jsmn/jsmn.h"

struct query {
    char *       query_string;
    unsigned int length;
    char **      _bind_list;
    unsigned int _bind_count;
};

struct json_kv {
    char * key;
    char * value;
};

extern struct query * _new_query( char * );
extern void _finalize_query( struct query * );
extern void _add_parameter_to_query( struct query *, char *, char * );
extern void _add_json_parameter_to_query( struct query *, char *, char * );
extern void _free_query( struct query * );
extern void _debug_struct( struct query * );
extern jsmntok_t * json_tokenise( char *, unsigned int * );
extern char * _add_json_parameters_to_param_list( CURL *, char *, char *, unsigned int * );
extern void _bind_uri_arguments( char **, char *, char * );
#endif // QUERY_HELPER_H
