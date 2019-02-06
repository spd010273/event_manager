/*-----------------------------------------------------------------------------
 *
 * event_manager--0.1--0.2.sql
 *     Event Manager extension schema
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        event_manager--0.1--0.2.sql
 *
 *-----------------------------------------------------------------------------
 */

SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_action', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.sq_pk_event_table_work_item', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_setting', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_action', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table', '' );
SELECT pg_catalog.pg_extension_config_dump( '@extschema@.tb_event_table_work_item', '' );
