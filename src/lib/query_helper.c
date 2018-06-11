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
#include "query_helper.h"

#define MAX_REGEX_GROUPS 1
#define MAX_REGEX_MATCHES 100

#define JSON_TOKENS 16

struct query * _new_query( char * query_string )
{
    struct query * query_object = NULL;

    query_object = ( struct query * ) calloc( 1, sizeof( struct query ) );

    if( query_object == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for query parameterization structure"
        );
        return NULL;
    }

    query_object->length = strlen( query_string );

    query_object->query_string = ( char * ) calloc(
        ( query_object->length + 1 ),
        sizeof( char )
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

struct query * _finalize_query( struct query * query_object )
{
    regmatch_t matches[MAX_REGEX_GROUPS + 1] = {{0}};
    regex_t    regex                         = {0};
    char *     temp_query                    = NULL;
    int        reg_result                    = 0;
    int        i                             = 0;

    char * bind_search      = "[?][:word:]+[?]";
    char * bind_replace     = "NULL";
    int    bindpoint_length = 0;

    if( query_object == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "NULL query object passed"
        );
        return NULL;
    }

    if( query_object->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "NULL query string passed in object"
        );
        _free_query( query_object );
        return NULL;
    }

    reg_result = regcomp( &regex, bind_search, REG_EXTENDED );

    if( reg_result != 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to compile regular expression"
        );

        _free_query( query_object );
        return NULL;
    }

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
                "Failed to execute regular expression"
            );
            _free_query( query_object );
            regfree( &regex );
            return NULL;
        }

        bindpoint_length = matches[0].rm_eo - matches[0].rm_so;
        temp_query = ( char * ) calloc(
            (
                strlen( query_object->query_string )
             - bindpoint_length
             + strlen( bind_replace )
             + 1
            ),
            sizeof( char )
        );

        if( temp_query == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for string resize operation"
            );
            _free_query( query_object );
            regfree( &regex );
            return NULL;
        }

        strncpy( temp_query, query_object->query_string, matches[0].rm_so );
        strcat( temp_query, bind_replace );
        strcat( temp_query, ( char * ) ( query_object->query_string + matches[0].rm_eo ) );

        query_object->query_string = ( char * ) realloc(
            ( void * ) query_object->query_string,
            strlen( temp_query ) + 1
        );

        if( query_object->query_string == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to reallocate memory for query string"
            );
            free( temp_query );
            regfree( &regex );
        }
    }

    regfree( &regex );
    free( temp_query );
    return query_object;
}

struct query * _add_parameter_to_query( struct query * query_object, char * key, char * value )
{
    regmatch_t matches[MAX_REGEX_GROUPS + 1] = {{0}};
    regex_t    regex                         = {0};
    char *     temp_query                    = NULL;
    int        reg_result                    = 0;
    int        i                             = 0;

    int    bind_length       = 0;
    int    bind_counter      = 0;
    char * bindpoint_search  = NULL;
    char * bindpoint_replace = NULL;

    if( query_object == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "NULL query_object passed"
        );
        return NULL;
    }

    if( query_object->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot parameterize NULL query string"
        );
        _free_query( query_object );
        return NULL;
    }

    bindpoint_search = ( char * ) calloc(
        ( strlen( key ) + 7 ),
        sizeof( char )
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

    bindpoint_replace = ( char * ) calloc(
        (int) ( floor( log10( abs( query_object->_bind_count + 1 ) ) ) + 3 ),
        sizeof( char )
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
        temp_query = ( char * ) calloc(
            (
                strlen( query_object->query_string )
              - bind_length
              + strlen( bindpoint_replace )
              + 1
            ),
            sizeof( char )
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
            _free_query( query_object );
            return NULL;
        }

        strncpy( temp_query, query_object->query_string, matches[0].rm_so );
        strcat( temp_query, bindpoint_replace );
        strcat( temp_query, ( char * ) ( query_object->query_string + matches[0].rm_eo ) );

        free( query_object->query_string );

        query_object->query_string = ( char * ) calloc(
            ( strlen( temp_query ) + 1 ),
            sizeof( char )
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
            query_object->_bind_list = ( char ** ) calloc(
                1,
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

        query_object->_bind_list[query_object->_bind_count] = ( char * ) calloc(
            ( strlen( value ) + 1 ),
            sizeof( char )
        );
        strcpy( query_object->_bind_list[query_object->_bind_count], value );
        query_object->_bind_count = query_object->_bind_count + 1;
    }

    return query_object;
}

struct query * _add_json_parameter_to_query( struct query * query_obj, char * json_string, char * key_prefix )
{
    jsmntok_t * json_tokens      = NULL;
    jsmntok_t   json_key_token   = {0};
    jsmntok_t   json_value_token = {0};
    jsmntok_t   temp_token       = {0};
    char *      key              = NULL;
    char *      value            = NULL;
    int         i                = 0;
    int         j                = 0;
    int         end_index        = 0;
    int         max_tokens       = 0;
    int         key_size_offset  = 0;

    if( json_string == NULL )
    {
        _log(
            LOG_LEVEL_DEBUG,
            "Nothing to bind"
        );
        return query_obj;
    }

    if( query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot add parameter to NULL object"
        );
        return NULL;
    }

    if( query_obj->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot parameterize NULL query string"
        );
        _free_query( query_obj );
        return NULL;
    }

    json_tokens = json_tokenise( json_string, &max_tokens );

    if( json_tokens == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to tokenise JSON string for binding to query"
        );
        _free_query( query_obj );
        return NULL;
    }

    // JSMN returns OBJECT, KEY, VALUE, ...
    //  We're expecting at least an object with one key and one value
    if( json_tokens[0].type != JSMN_OBJECT )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Root element of JSON response is not an object"
        );
        _free_query( query_obj );
        return NULL;
    }

    if( max_tokens < 3 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "JSON response is empty"
        );

        _log(
            LOG_LEVEL_DEBUG,
            "Got '%s', (%d tokens )",
            json_string,
            max_tokens
        );
        _free_query( query_obj );
        return NULL;
    }

    i = 1;
    _log( LOG_LEVEL_DEBUG, "Parsing JSON '%s'", json_string );

    if( key_prefix != NULL )
    {
        key_size_offset = strlen( key_prefix );
    }

    for(;;)
    {
        json_key_token = json_tokens[i];

        if( json_key_token.type != JSMN_STRING )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Expected string key in JSON structure (got %d at index %d)",
                json_key_token.type,
                i
            );
            free( json_tokens );
            _free_query( query_obj );
            return NULL;
        }

        key = ( char * ) calloc(
            ( json_key_token.end - json_key_token.start + 1 + key_size_offset ),
            sizeof( char )
        );

        if( key == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for JSON key string"
            );
            _free_query( query_obj );
            free( json_tokens );
            return NULL;
        }

        if( key_prefix != NULL )
        {
            strcpy( key, key_prefix );
        }

        strncat(
            key,
            ( char * ) ( json_string + json_key_token.start ),
            json_key_token.end - json_key_token.start
        );

        key[key_size_offset + json_key_token.end - json_key_token.start] = '\0';

        i++;

        if( i >= ( max_tokens ) )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Reached unexpected end of JSON object"
            );
            free( key );
            free( json_tokens );
            _free_query( query_obj );
            return NULL;
        }

        json_value_token = json_tokens[i];

        value = ( char * ) calloc(
            ( json_value_token.end - json_value_token.start + 1 ),
            sizeof( char )
        );

        if( value == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to allocate memory for JSON value string"
            );
            _free_query( query_obj );
            free( json_tokens );
            free( key );
            return NULL;
        }

        strncpy(
            value,
            ( char * ) ( json_string + json_value_token.start ),
            json_value_token.end - json_value_token.start
        );

        value[json_value_token.end - json_value_token.start] = '\0';

        if( json_value_token.type == JSMN_OBJECT || json_value_token.type == JSMN_ARRAY )
        {
            // index i to point to the next key by finding the index of the
            //  first token that is >= our value's end index
            end_index = json_value_token.end;

            for( j = i; j < max_tokens; j++ )
            {
                temp_token = json_tokens[j];
                if( temp_token.start >= end_index )
                {
                    i = j;
                    break;
                }
            }

            // The value which is an object or key somehow was last thing in our json object
            i = max_tokens;
        }

        query_obj = _add_parameter_to_query(
            query_obj,
            key,
            value
        );

        _log( LOG_LEVEL_DEBUG, "Potentially bound KV: %s,%s", key, value );
        free( key );
        free( value );

        if( i >=  ( max_tokens - 1 ) )
        {
            break;
        }

        i++;
    }

    free( json_tokens );
    return query_obj;
}

void _debug_struct( struct query * obj )
{
    int i = 0;
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
    int i = 0;
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

    free( query_object );
    return;
}

char * _add_json_parameters_to_param_list( char * param_list, char * json_string, int * malloc_size )
{
    jsmntok_t * json_tokens      = NULL;
    jsmntok_t   json_key_token   = {0};
    jsmntok_t   json_value_token = {0};
    jsmntok_t   temp_token       = {0};
    int         end_index        = 0;
    int         j                = 0;
    int         i                = 0;
    int         max_tokens       = 0;
    bool        first_param_pass = true;

    if( param_list == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Received NULL URI parameter list"
        );

        return NULL;
    }

    if( json_string == NULL )
    {
        _log( LOG_LEVEL_DEBUG, "Nothing to bind" );
        return param_list;
    }

    json_tokens = json_tokenise( json_string, &max_tokens );

    if( json_tokens == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to tokenise JSON string for binding to parameter_list"
        );
        return NULL;
    }

    // JSMN returns OBJECT, KEY, VALUE, ...
    //  We're expecting at least an object with one key and one value
    if( json_tokens[0].type != JSMN_OBJECT )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Root element of JSON response is not an object"
        );
        return NULL;
    }

    if( max_tokens < 3 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "JSON response is empty"
        );

        _log(
            LOG_LEVEL_DEBUG,
            "Got '%s', (%d tokens )",
            json_string,
            max_tokens
        );
        return NULL;
    }

    i = 1;

    _log( LOG_LEVEL_DEBUG, "Parsing JSON '%s'", json_string );

    for(;;)
    {
        json_key_token = json_tokens[i];

        if( json_key_token.type != JSMN_STRING )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Expected string key in JSON structure (got %d at index %d)",
                json_key_token.type,
                i
            );
            free( json_tokens );
            free( param_list );
            return NULL;
        }

        *malloc_size = *malloc_size + json_key_token.end - json_key_token.start + 1;

        if( first_param_pass == true )
        {
            *malloc_size = *malloc_size - 1;
        }

        param_list = ( char * ) realloc(
            ( char * ) param_list,
            sizeof( char ) * (*malloc_size)
        );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to reallocate memory for parameter string key"
            );
            free( json_tokens );
            return NULL;
        }

        if( first_param_pass == false )
        {
            strcat( param_list, "&" );
        }

        first_param_pass = false;

        strncpy(
            param_list,
            ( char * ) ( json_string + json_key_token.start ),
            json_key_token.end - json_key_token.start
        );

        param_list[*malloc_size] = '\0';

        i++;

        if( i >= ( max_tokens ) )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Reached unexpected end of JSON object"
            );
            free( param_list );
            free( json_tokens );
            return NULL;
        }

        json_value_token = json_tokens[i];

        *malloc_size = *malloc_size + json_value_token.end - json_key_token.start + 1;
        param_list = ( char * ) realloc(
            ( char * ) param_list,
            *malloc_size
        );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to reallocate memory for parameter string value"
            );
            free( json_tokens );
            return NULL;
        }

        strcat( param_list, "=" );
        strncpy(
            param_list,
            ( char * ) ( json_string + json_value_token.start ),
            json_value_token.end - json_value_token.start
        );

        param_list[*malloc_size] = '\0';

        if( json_value_token.type == JSMN_OBJECT || json_value_token.type == JSMN_ARRAY )
        {
            // index i to point to the next key by finding the index of the
            //  first token that is >= our value's end index
            end_index = json_value_token.end;

            for( j = i; j < max_tokens; j++ )
            {
                temp_token = json_tokens[j];
                if( temp_token.start >= end_index )
                {
                    i = j;
                    break;
                }
            }

            // The value which is an object or key somehow was last thing in our json object
            i = max_tokens;
        }

        if( i >=  ( max_tokens - 1 ) )
        {
            break;
        }

        i++;
    }

    free( json_tokens );
    return param_list;
}

jsmntok_t * json_tokenise( char * json, int * token_count )
{
    jsmn_parser  parser  = {0};
    int          jsmn_rc = 0;
    jsmntok_t *  tokens  = NULL;
    unsigned int n       = JSON_TOKENS;

    jsmn_init( &parser );

    tokens = ( jsmntok_t * ) malloc(
        sizeof( jsmntok_t )
      * n
    );

    if( tokens == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for JSON tokenisation"
        );
        return NULL;
    }

    jsmn_rc = jsmn_parse( &parser, json, strlen( json ), tokens, n );

    // Keep reallocating memory in JSON_TOKENS chunks until
    // The entire JSON structure can fit

    while( jsmn_rc == JSMN_ERROR_NOMEM )
    {
        n = n + JSON_TOKENS;

        tokens = realloc(
            tokens,
            sizeof( jsmntok_t )
          * n
        );

        if( tokens == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to reallocate memory for JSON tokenisation"
            );
            return NULL;
        }

        jsmn_rc = jsmn_parse( &parser, json, strlen( json ), tokens, n );
    }

    if( jsmn_rc == JSMN_ERROR_INVAL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to parse JSON string: invalid or corrupted string"
        );

        free( tokens );
        return NULL;
    }

    if( jsmn_rc == JSMN_ERROR_PART )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to parse JSON string: received invalid partial string"
        );
        free( tokens );

        return NULL;
    }

    *token_count = jsmn_rc;
    return tokens;
}
