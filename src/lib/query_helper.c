/*------------------------------------------------------------------------
 *
 * query_helper.c
 *     Query parameterization functions and struct
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 * Copyright (c) 2021, MerchLogix Inc.
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
#include <curl/curl.h>

#include "util.h"
#include "query_helper.h"

#define MAX_REGEX_GROUPS 1
#define MAX_REGEX_MATCHES 100

#define JSON_TOKENS 16

static struct json_kv * get_next_json_kv_pair( char *, char *, bool * );
static struct json_kv * new_kv_pair(
    char *,         // JSON string
    jsmntok_t *,    // Key token
    jsmntok_t *,    // Value token
    jsmntok_t *,    // Token array
    char *,         // Key prefix
    bool *,         // Error flag
    unsigned int *, // State variable
    unsigned int *  // State variable
);
static void _free_json_kv( struct json_kv * );

/*
 * struct query * _new_query( char * query_string )
 *     Generates a query struct from a query string, allowing
 *     statements and their parameters to be passed around in one
 *     neat little package.
 *
 * Arguments:
 *     char * query_string: Statement to be copied into the structure.
 * Return:
 *     struct query *:      Pointer to the allocated query struct.
 * Error Conditions:
 *     - Emits error on failure to allocate memory.
 */
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

    strncpy(
        (char * )( query_object->query_string ),
        query_string,
        query_object->length
    );

    query_object->_bind_count = 0;
    query_object->_bind_list  = NULL;

    return query_object;
}

/*
 * void _finalize_query( struct query * query_object )
 *     Finalizes a query, replacing all remaining unbound placeholders with
 *     SQL NULL.
 *
 * Arguments:
 *     struct query * query_object: Query struct containing statement
 *                                  to finalize.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to allocate / reallocate string memory
 */
void _finalize_query( struct query * query_object )
{
    regmatch_t matches[MAX_REGEX_GROUPS + 1] = {{0}};
    regex_t    regex                         = {0};
    char *     temp_query                    = NULL;
    int        reg_result                    = 0;
    int        i                             = 0;

    char * bind_search      = "[?](((OLD)|(NEW))[[:punct:]])?[[:alpha:]_]+[?]";
    char * bind_replace     = "NULL";
    int    bindpoint_length = 0;

    if( query_object == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "NULL query object passed"
        );

        return;
    }

    if( query_object->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "NULL query string passed in object"
        );

        _free_query( query_object );

        return;
    }

    reg_result = regcomp( &regex, bind_search, REG_EXTENDED );

    if( reg_result != 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to compile regular expression (_finalize_query)"
        );

        _free_query( query_object );

        return;
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

            return;
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

            return;
        }

        strncpy(
            temp_query,
            query_object->query_string,
            matches[0].rm_so
        );

        strncat(
            temp_query,
            bind_replace,
            strlen( bind_replace )
        );

        strncat(
            temp_query,
            ( char * ) ( query_object->query_string + matches[0].rm_eo ),
            strlen( query_object->query_string + matches[0].rm_eo )
        );

        temp_query[
            matches[0].rm_so
          + strlen( bind_replace )
          + strlen( query_object->query_string + matches[0].rm_eo )
        ] = '\0';

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

            return;
        }

        strncpy(
            query_object->query_string,
            temp_query,
            strlen( temp_query )
        );
        query_object->query_string[strlen(temp_query)] = '\0';
        free( temp_query );
    }

    regfree( &regex );

    return;
}

/*
 * void _add_parameter_to_query(
 *     struct query * query_object,
 *     char * key,
 *     char * value
 * )
 *     Adds parameter to query structure. This performs several actions:
 *         - Creates a new indexed bindpoint for the named parameter
 *         - Replace all appearances of the named bindpoint with bindpoint index
 *              EX: ?some_parameter?  becomes $4
 *         - Pushes parameter onto the param_list at that index position
 *
 * Arguments:
 *     - struct query * query_object: Query struct to add the parameter to.
 *     - char * key:                  Named bindpoint for the following value.
 *     - char * value:                String representation of the value to be
 *                                    bound.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to allocate string memory
 *     - Emits error on regular expression compilation failure
 *     - Emits error on regular expression execution failure
 */
void _add_parameter_to_query(
    struct query * query_object,
    char * key,
    char * value
)
{
    regmatch_t matches[MAX_REGEX_GROUPS + 1] = {{0}};
    regex_t    regex                         = {0};
    char *     temp_query                    = NULL;
    int        reg_result                    = 0;
    int        i                             = 0;
    int        bind_length                   = 0;
    int        bind_counter                  = 0;
    char *     bindpoint_search              = NULL;
    char *     bindpoint_replace             = NULL;

    if( query_object == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "NULL query_object passed"
        );

        return;
    }

    if( query_object->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot parameterize NULL query string"
        );

        _free_query( query_object );

        return;
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
        return;
    }

    strncpy( bindpoint_search, "[?]", 3 );
    strncat( bindpoint_search, key, strlen( key ) );
    strncat( bindpoint_search, "[?]", 3 );
    bindpoint_search[ strlen(key) + 6 ] = '\0';
    reg_result = regcomp( &regex, bindpoint_search, REG_EXTENDED );

    if( reg_result != 0 )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to compile bindpoint search regular expression"
        );

        free( bindpoint_search );
        _free_query( query_object );

        return;
    }

    bindpoint_replace = ( char * ) calloc(
        (int) ( floor( log10( abs( query_object->_bind_count + 1 ) ) ) + 3 ),
        sizeof( char )
    );

    if( bindpoint_replace == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for replacement string in regular "
            "expression"
        );

        free( bindpoint_search );
        _free_query( query_object );
        regfree( &regex );

        return;
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
#ifdef EVENT_DEBUG
            _log( LOG_LEVEL_DEBUG, "No match for key '%s'", bindpoint_search );
#endif // EVENT_DEBUG
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

            return;
        }
#ifdef EVENT_DEBUG
        _log( LOG_LEVEL_DEBUG, "Found match for key '%s'", bindpoint_search );
#endif // EVENT_DEBUG
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

            return;
        }

        strncpy(
            temp_query,
            query_object->query_string,
            matches[0].rm_so
        );

        strncat(
            temp_query,
            bindpoint_replace,
            strlen( bindpoint_replace )
        );

        strncat(
            temp_query,
            ( char * ) ( query_object->query_string + matches[0].rm_eo ),
            strlen( query_object->query_string + matches[0].rm_eo )
        );

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

            return;
        }

        _log( LOG_LEVEL_DEBUG, "Temp query is '%s'", temp_query );
        strncpy(
            query_object->query_string,
            temp_query,
            strlen( temp_query )
        );

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

            return;
        }

        if( value == NULL )
        {
            query_object->_bind_list[query_object->_bind_count] = NULL;
        }
        else
        {
            query_object->_bind_list[query_object->_bind_count] = ( char * ) calloc(
                ( strlen( value ) + 1 ),
                sizeof( char )
            );

            strncpy(
                query_object->_bind_list[query_object->_bind_count],
                value,
                strlen( value )
            );
        }

        query_object->_bind_count = query_object->_bind_count + 1;
    }

    return;
}

/*
 * void _add_json_parameter_to_query(
 *     struct query * query_obj,
 *     char * json_string,
 *     char * key_prefix
 * )
 *     Adds the key-value pairs from a JSON object to a query
 *
 * Arguments:
 *     struct query * query_obj: Query struct to add the parameters to
 *     char * json_string:       JSON containing the key-value pairs to be
 *                               added.
 *     char * key_prefix:        Optional key prefix for the JSON strings
 *                               being added to the query. Allows support for
 *                               NEW and OLD psuedorecords to be added as
 *                               ?OLD.key?, for example.
 * Return:
 *     None
 * Error Conditions:
 *     - Emits error on failure to allocate string memory
 *     - Emits error on failure to parse JSON object
 *     - Emits error on invalid JSON structure (ARRAY / SCALAR )
 *     - Emits error on receipt of invalid arguments (NULL query object)
 */
void _add_json_parameter_to_query(
    struct query * query_obj,
    char *         json_string,
    char *         key_prefix
)
{
    bool             json_error     = false;
    struct json_kv * key_value_pair = NULL;

    if( json_string == NULL )
    {
#ifdef EVENT_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Nothing to bind"
        );
#endif // EVENT_DEBUG
        return;
    }
#ifdef EVENT_DEBUG
    _log(
        LOG_LEVEL_DEBUG,
        "Adding JSON parameters to query\n"\
        "string: '%s', prefix: '%s'",
        json_string,
        key_prefix
    );
#endif // EVENT_DEBUG
    if( query_obj == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot add parameter to NULL object"
        );
        return;
    }

    if( query_obj->query_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot parameterize NULL query string"
        );
        _free_query( query_obj );
        return;
    }

    for(
            key_value_pair = get_next_json_kv_pair(
                json_string, // Initialize JSON tokenization and setup state
                key_prefix,
                &json_error
            );
            json_error == false && key_value_pair != NULL;
            key_value_pair = get_next_json_kv_pair(
                json_string, // Continue parsing KV errors
                key_prefix,  // Until we get and error or hit the
                &json_error  // end of the string
            )
       )
    {
        if( json_error == true )
        {
            _free_query( query_obj );

            if( key_value_pair != NULL )
            {
                _free_json_kv( key_value_pair );
            }

            return;
        }

        if( key_value_pair == NULL )
        {
            break;
        }

        _add_parameter_to_query(
            query_obj,
            key_value_pair->key,
            key_value_pair->value
        );

#ifdef EVENT_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Potentially bound KV: %s,%s",
            key_value_pair->key,
            key_value_pair->value
        );
#endif // EVENT_DEBUG
        _free_json_kv( key_value_pair );
        key_value_pair = NULL;
    }

    return;
}

/*
 * void _debug_struct( struct query * obj )
 *     Emits the structure on STDOUT
 *
 * Arguments:
 *     struct query * obj: Struct to dump
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
void _debug_struct( struct query * obj )
{
    int i = 0;
#ifdef C_DEBUG
    _log( LOG_LEVEL_DEBUG, "Query object: " );
    _log( LOG_LEVEL_DEBUG, "==============" );
    _log( LOG_LEVEL_DEBUG, "query_string: '%s'", obj->query_string );
    _log( LOG_LEVEL_DEBUG, "length: %d", obj->length );
    _log( LOG_LEVEL_DEBUG, "_bind_count: %d", obj->_bind_count );
    _log( LOG_LEVEL_DEBUG, "_bind_list: " );
#endif // C_DEBUG
    for( i = 0; i < obj->_bind_count; i++ )
    {
        _log( LOG_LEVEL_DEBUG, "%d: '%s'", i, obj->_bind_list[i] );
    }

    return;
}

/*
 * void _free_query( struct query * query_object )
 *     Frees the memory allocated to internal query struct strings and
 *     parameter lists.
 *
 * Arguments:
 *     struct query * query_object: Struct to free
 * Return:
 *     None
 * Error Conditions:
 *     None
 */
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

/*
 * char * _add_json_parameters_to_param_list(
 *     CURL * curl_handle,
 *     char * param_list,
 *     char * json_string,
 *     unsigned int * malloc_size
 * )
 *    Adds the parameters from the json_string to a URI's parameter list.
 *    Example:
 *        {"key1":"val1","key2":"val2"}
 *    becomes:
 *        key1=val1&key2=val2

 * Arguments:
 *     CURL * curl_handle: Handle to the CuRL global state / handle.
 *     char * param_list:  URI parameter list.
 *     char * json_string: JSON object to get new parameters from.
 *     int * malloc_size:  Current size in sizeof( char ) of param_list,
 *                         may be adjusted as parameters are added
 *                         (pass by reference).
 * Return:
 *     char * param_list:  Modified parameter list
 * Error Conditions:
 *     - Emits error on failure to allocate string memory.
 *     - Emits error on failure to parse JSON object.
 *     - Emits error on receipt of invalid JSON structure (ARRAY / SCALAR ).
 */
char * _add_json_parameters_to_param_list(
    CURL *         curl_handle,
    char *         param_list,
    char *         json_string,
    unsigned int * malloc_size
)
{
    bool             first_param_pass = true;
    bool             json_error       = false;
    char *           encoded_value    = NULL;
    struct json_kv * key_value_pair   = NULL;

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
#ifdef EVENT_DEBUG
        _log( LOG_LEVEL_DEBUG, "Nothing to bind" );
#endif // EVENT_DEBUG
        return param_list;
    }

    for(
            key_value_pair = get_next_json_kv_pair(
                json_string,
                NULL,
                &json_error
            );
            json_error == false && key_value_pair != NULL;
            key_value_pair = get_next_json_kv_pair(
                json_string,
                NULL,
                &json_error
            )
       )
    {
        if( json_error == true )
        {
            if( key_value_pair != NULL )
            {
                _free_json_kv( key_value_pair );
            }

            if( param_list != NULL )
            {
                free( param_list );
            }

            return NULL;
        }

        if( key_value_pair == NULL )
        {
            break;
        }

        if( key_value_pair->value == NULL )
        {
            // \0 is fine for SQL, but will pre-terminate a URI string. We will replace
            // \0 with string NULL
            key_value_pair->value = ( char * ) calloc(
                5,
                sizeof( char )
            );

            if( key_value_pair->value == NULL )
            {
                _log(
                    LOG_LEVEL_ERROR,
                    "Failed to allocate string :("
                );
                _free_json_kv( key_value_pair );

                if( param_list != NULL )
                {
                    free( param_list );
                }
                return NULL;
            }

            strncpy( key_value_pair->value, "NULL", 4 );
        }

        *malloc_size = *malloc_size
                     + strlen( key_value_pair->key )
                     + 2;

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

            return NULL;
        }

        if( first_param_pass == false )
        {
            strncat( param_list, "&", 1 );
        }

        first_param_pass = false;

        strncat(
            param_list,
            ( char * ) ( key_value_pair->key ),
            strlen( key_value_pair->key )
        );

        param_list[*malloc_size - 1] = '\0';
#ifdef EVENT_DEBUG
        _log( LOG_LEVEL_DEBUG, "My key_value_pair->Value: '%s'", key_value_pair->value );
#endif // EVENT_DEBUG
        encoded_value = curl_easy_escape(
            curl_handle,
            ( const char * ) key_value_pair->value,
            strlen( key_value_pair->value )
        );

        if( encoded_value == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "URL Encoding operation failed"
            );

            _free_json_kv( key_value_pair );
            free( param_list );

            return NULL;
        }

        *malloc_size = *malloc_size + strlen( encoded_value ) + 2;

        param_list = ( char * ) realloc(
            ( char * ) param_list,
            sizeof( char ) * (*malloc_size)
        );

        if( param_list == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to reallocate memory for parameter string value"
            );

            free( param_list );
            curl_free( encoded_value );
            _free_json_kv( key_value_pair );
            return NULL;
        }

        strncat( param_list, "=", 1 );

        strncat(
            param_list,
            encoded_value,
            strlen( encoded_value )
        );

        curl_free( encoded_value );
        param_list[*malloc_size - 1] = '\0';
        _free_json_kv( key_value_pair );
    }

    return param_list;
}

/*
 * jsmntok_t * json_tokenise( char * json, unsigned int * token_count )
 *     Converts a JSON string into an array of JSMN tokens
 *
 * Arguments:
 *     char * json:                JSON string to tokenise.
 *     unsigned int * token_count: Count of tokens found within the JSON string.
 * Return:
 *     jsmntok_t * tokens: Array of JSMN tokens from JSON string.
 *                         NOTE: The caller is responsible for freeing this.
 * Error Conditions:
 *     - Emits error on failure to allocate memory for a token block.
 *     - Emits error on failure to parse JSON.
 */
jsmntok_t * json_tokenise( char * json, unsigned int * token_count )
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

void _bind_uri_arguments( char ** uri, char * parameters, char * key_prefix )
{
    struct json_kv * key_value_pair                = NULL;
    regmatch_t       matches[MAX_REGEX_GROUPS + 1] = {{0}};
    regex_t          regex                         = {0};
    char *           bindpoint_search              = NULL;
    char *           temp_string                   = NULL;
    char *           temp_value                    = NULL;
    unsigned int     bind_length                   = 0;
    unsigned int     i                             = 0;
    int              reg_result                    = 0;
    bool             json_error                    = false;

    if( uri == NULL || (*uri) == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot bind URI arguments to a NULL uri"
        );

        return;
    }

    if( parameters == NULL )
    {
        // No parameters to bind
        return;
    }

    for(
            key_value_pair = get_next_json_kv_pair(
                parameters,
                key_prefix,
                &json_error
            );
            json_error == false && key_value_pair != NULL;
            key_value_pair = get_next_json_kv_pair(
                parameters,
                key_prefix,
                &json_error
            )
       )
    {
        if( json_error == true )
        {
            return;
        }

        if( key_value_pair == NULL )
        {
            // nothing left to process
            break;
        }

        bindpoint_search = ( char * ) calloc(
            ( strlen( key_value_pair->key ) + 7 ),
            sizeof( char )
        );

        if( bindpoint_search == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Could not allocate memory for regular expression search string"
            );

            _free_json_kv( key_value_pair );
            return;
        }

        strncpy( bindpoint_search, "[?]", 3 );
        strncat(
            bindpoint_search,
            key_value_pair->key,
            strlen( key_value_pair->key )
        );
        strncat( bindpoint_search, "[?]", 3 );

        bindpoint_search[strlen( key_value_pair->key ) + 6] = '\0';

        reg_result = regcomp( &regex, bindpoint_search, REG_EXTENDED );

        if( reg_result != 0 )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to compile regular expression"
            );

            free( bindpoint_search );
            _free_json_kv( key_value_pair );
            return;
        }

        for( i = 0; i < MAX_REGEX_MATCHES; i++ )
        {
            reg_result = regexec(
                &regex,
                (*uri),
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

                free( bindpoint_search );
                _free_json_kv( key_value_pair );
                return;
            }

            bind_length = matches[0].rm_eo - matches[0].rm_so;

            if( key_value_pair->value == NULL )
            {
                temp_value = "''";
            }
            else
            {
                temp_value = key_value_pair->value;
            }

            temp_string = ( char * ) calloc(
                (
                    strlen( (*uri) )      // Original string
                  - bind_length           // What we are replacing
                  + strlen( temp_value )  // What is replacing ^
                  + 1                     // NULL terminator
                ),
                sizeof( char )
            );

            if( temp_string == NULL )
            {
                _log(
                    LOG_LEVEL_ERROR,
                    "Failed to allocate memory for string resize operation"
                );

                free( bindpoint_search );
                _free_json_kv( key_value_pair );
                return;
            }

            strncpy(
                temp_string,
                (*uri),
                matches[0].rm_so
            );

            strncat(
                temp_string,
                temp_value,
                strlen( temp_value )
            );

            strncat(
                temp_string,
                ( char * ) ( (*uri) + matches[0].rm_eo ),
                strlen( (*uri) + matches[0].rm_eo )
            );

            temp_string[
                matches[0].rm_so
              + strlen( temp_value )
              + strlen( (*uri) + matches[0].rm_eo )
//              - 1
            ] = '\0';

            free( (*uri) );

            // Copy temp_string -> action->uri so we can reuse pointer in next iter
            (*uri) = ( char * ) calloc(
                ( strlen( temp_string ) + 1 ),
                sizeof( char )
            );

            if( (*uri) == NULL )
            {
                _log(
                    LOG_LEVEL_ERROR,
                    "Failed to allocate memory for string resize operation"
                );

                free( temp_string );
                free( bindpoint_search );
                _free_json_kv( key_value_pair );

                return;
            }

            strncpy(
                (*uri),
                temp_string,
                strlen( temp_string )
            );
            (*uri)[strlen( temp_string )] = '\0';
            free( temp_string );
        }

        free( bindpoint_search );
        regfree( &regex );
#ifdef EVENT_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "Potentially bound KV %s,%s to %s",
            key_value_pair->key,
            temp_value,
            (*uri)
        );
#endif // EVENT_DEBUG
        _free_json_kv( key_value_pair );
    }

    return;
}

/*
 * static struct json_kv * get_next_json_kv_pair(
 *     char *,
 *     char *,
 *     bool *
 * )
 *
 * Iterator for loops that parse JSON strings using JSMN. This abstracts the
 * object / array lookahead and keeps the main logic nice and tidy.
 *
 * Arguments:
 *     char * json_string: The JSON string being parsed
 *     char * key_prefix:  An optional string to prefix keys with in
 *                         regular expression replacement operations
 *     bool * error:       Error flag thrown on parsing or memory
 *                         allocation errors
 *
 * Return:
 *     struct json_kv * result: The next key-value pair in the json string
 *
 * Error Conditions:
 *     Will set the error flag and throw a warning on JSON parse error or
 *     failure to allocate memory
 */
static struct json_kv * get_next_json_kv_pair(
    char * json_string,
    char * key_prefix,
    bool * error
)
{
    static jsmntok_t *  json_tokens      = NULL;
    static unsigned int max_tokens       = 0;
    static unsigned int i                = 0;
    jsmntok_t           json_key_token   = {0};
    jsmntok_t           json_value_token = {0};
    struct json_kv *    result           = NULL;

    (*error) = false;

    if( json_string == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Cannot parse NULL JSON string"
        );

        // Reset state
        if( json_tokens != NULL )
        {
            free( json_tokens );
            json_tokens = NULL;
        }

        i        = 0;
        (*error) = true;

        return NULL;
    }

    if( json_tokens == NULL )
    {
        // Parse a new JSON string into tokens, set up parser state
        json_tokens = json_tokenise( json_string, &max_tokens );

        if( json_tokens == NULL )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Failed to tokenise JSON string"
            );

            (*error) = true;
            i        = 0;

            return NULL;
        }

        if( json_tokens[0].type != JSMN_OBJECT )
        {
            _log(
                LOG_LEVEL_ERROR,
                "Root element of JSON response is not an object"
            );

            free( json_tokens );

            json_tokens = NULL;
            i           = 0;
            (*error)    = true;

            return NULL;
        }

        if( max_tokens < 3 )
        {
            _log(
                LOG_LEVEL_ERROR,
                "JSON to be parsed is empty"
            );

            free( json_tokens );

            json_tokens = NULL;
            i           = 0;
            (*error)    = true;

            return NULL;
        }

        i = 1;
#ifdef C_DEBUG
        _log( LOG_LEVEL_DEBUG, "Parsing JSON '%s'", json_string );
#endif // C_DEBUG
    }
    else
    {
        // ensure that we do not exceed the bounds of an already allocated
        // json_tokens array
        if( i >= ( max_tokens - 1 ) )
        {
            i = 0;
            max_tokens = 0;

            if( json_tokens != NULL )
            {
                free( json_tokens );
                json_tokens = NULL;
            }

            return NULL;
        }
    }

    // Handle the case where i is at max_tokens
    if( i >= max_tokens )
    {
        (*error) = false;
        i = 0;
        max_tokens = 0;
        if( json_tokens != NULL )
        {
            free( json_tokens );
            json_tokens = NULL;
        }
        return NULL;
    }

    json_key_token = json_tokens[i];

    if( json_key_token.type != JSMN_STRING )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Expected string key in JSON structure, got %d at index %d",
            json_key_token.type,
            i
        );

        free( json_tokens );

        json_tokens = NULL;
        i           = 0;
        (*error)    = true;

        return NULL;
    }

    i++;

    if( i >= ( max_tokens ) )
    {
        // Bounds checking
        _log(
            LOG_LEVEL_ERROR,
            "Reached unexpected end of JSON object"
        );

        free( json_tokens );

        json_tokens = NULL;
        i           = 0;
        (*error)    = true;

        return NULL;
    }

    json_value_token = json_tokens[i];
    i++;

    result = new_kv_pair(
        json_string,
        &json_key_token,
        &json_value_token,
        json_tokens,
        key_prefix,
        error,
        &i,
        &max_tokens
    );

    if( (*error) == true )
    {
        if( result != NULL )
        {
            _free_json_kv( result );
            result = NULL;
        }

        free( json_tokens );
        json_tokens = NULL;
        i           = 0;

        return NULL;
    }

    return result;
}

/*
 * static struct json_kv * new_kv_pair(
 *    char *         json_string,
 *    jsmntok_t *    json_key_token,
 *    jsmntok_t *    json_value_token,
 *    jsmntok_t *    json_tokens,
 *    char *         key_prefix,
 *    bool *         error,
 *    unsigned int * i,
 *    unsigned int * max_tokens
 * )
 *
 * Struct allocator to JSON key-value pairs. Keeps track of the iterator's
 * internal state as well as information about the string being parsed
 *
 * Arguments:
 *    char * json_string:           JSON string being parsed
 *    jsmntok_t * json_key_token:   Pointer to the JSON key token
 *    jsmntok_t * json_value_token: Pointer to the JSON value token
 *    jsmntok_t * json_tokens:      Array of json tokens.
 *    char * key_prefix:            Used to prefix keys during regex
 *                                  substitution
 *    bool * error:                 Error flag set on parse error or memory
 *                                  allocation error.
 *    unsigned int * i:             Internal iterator
 *    unsigned int * max_tokens:    Number of parsed tokens from the json
 *                                  string
 *
 * Return:
 *    struct json_kv * json_pair:   The newly allocated JSON key-value pair
 *
 * Error Conditions:
 *    Raises error flag and throws error on JSON parse error or failure to
 *    allocate memory.
 */
static struct json_kv * new_kv_pair(
    char *         json_string,
    jsmntok_t *    json_key_token,
    jsmntok_t *    json_value_token,
    jsmntok_t *    json_tokens,
    char *         key_prefix,
    bool *         error,
    unsigned int * i,
    unsigned int * max_tokens
)
{
    unsigned int     j               = 0;
    unsigned int     key_size_offset = 0;
    unsigned int     end_index       = 0;
    jsmntok_t        temp_token      = {0};
    char *           key             = NULL;
    char *           value           = NULL;
    struct json_kv * result          = NULL;
    bool             found_st_end    = false;
    unsigned short   val_len         = 0;

    if(
            json_string == NULL
         || json_key_token == NULL
         || json_value_token == NULL
         || json_tokens == NULL
      )
    {
        return NULL;
    }

    if( key_prefix != NULL )
    {
        key_size_offset = strlen( key_prefix );
    }

    key = ( char * ) calloc(
        ( json_key_token->end - json_key_token->start + 1 + key_size_offset ),
        sizeof( char )
    );

    if( key == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for JSON key"
        );

        (*error) = true;
        return NULL;
    }

    if( key_prefix != NULL )
    {
        strncpy( key, key_prefix, key_size_offset );
    }

    strncat(
        key,
        ( char * ) ( json_string + json_key_token->start ),
        json_key_token->end - json_key_token->start
    );

    key[key_size_offset + json_key_token->end - json_key_token->start] = '\0';

    value = ( char * ) calloc(
        ( json_value_token->end - json_value_token->start + 1 ),
        sizeof( char )
    );

    if( value == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Failed to allocate memory for JSON value"
        );
        free( key );
        (*error) = true;
        return NULL;
    }

    strncpy(
        value,
        ( char * ) ( json_string + json_value_token->start ),
        json_value_token->end - json_value_token->start
    );

    value[json_value_token->end - json_value_token->start] = '\0';

    if(
           json_value_token->type == JSMN_OBJECT
        || json_value_token->type == JSMN_ARRAY
      )
    {
        end_index = json_value_token->end;
#ifdef C_DEBUG
        _log(
            LOG_LEVEL_DEBUG,
            "JSON Iterator: Performing nested struct lookahead to index %d",
            end_index
        );
#endif // C_DEBUG
        // i has already been inremented to point to the next token
        // (the token that follows json_value_token in json_tokens[])
        for( j = (*i); j < (*max_tokens); j++ )
        {
            temp_token = json_tokens[j];
#ifdef C_DEBUG
            _log(
                LOG_LEVEL_DEBUG,
                "JSON Iterator: token T: %d, S: %d E: %d size: %d",
                temp_token.type,
                temp_token.start,
                temp_token.end,
                temp_token.size
            );
#endif // C_DEBUG
            if( temp_token.start >= end_index )
            {
                (*i) = j;
                found_st_end = true;

                break;
            }
        }

        if( !found_st_end )
        {
            (*i) = (*max_tokens);
        }
    }

    val_len = strlen( value );
    if(
           strncmp( value, "null", MIN( val_len, 4 ) ) == 0
        || strncmp( value, "NULL", MIN( val_len, 4 ) ) == 0
      )
    {
        free( value );
        value = NULL;
    }

    result = ( struct json_kv * ) calloc(
        1,
        sizeof( struct json_kv )
    );

    if( result == NULL )
    {
        _log(
            LOG_LEVEL_ERROR,
            "Could not allocate JSON key/value pair"
        );

        free( key );

        if( value != NULL )
        {
            free( value );
        }

        (*error) = true;
        return NULL;
    }

    result->key   = key;
    result->value = value;

    return result;
}

/*
 * static void _free_json_jv( struct json_kv * json_pair )
 *
 * Free the memory allocated for a JSON key-value pair.
 *
 * Arguments:
 *     struct json_kv * json_pair: The key_value pair to be freed.
 *
 * Return:
 *     None
 *
 * Error Conditions:
 *     None
 */
static void _free_json_kv( struct json_kv * json_pair )
{
    if( json_pair == NULL )
    {
        return;
    }

    if( json_pair->key != NULL )
    {
        free( json_pair->key );
        json_pair->key = NULL;
    }

    if( json_pair->value != NULL )
    {
        free( json_pair->value );
        json_pair->value = NULL;
    }

    free( json_pair );

    return;
}
