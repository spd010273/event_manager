/*------------------------------------------------------------------------
 *
 * event_manager--0.2--0.3.sql
 *     Upgrade script for transitioning from version 0.2 to 0.3
 *
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        event_manager--0.2--0.3.sql
 *
 *------------------------------------------------------------------------
 */

CREATE INDEX ix_work_queue_action_recorded
          ON @extschema@.tb_work_queue( action, recorded )
       WHERE execute_asynchronously IS TRUE;

CREATE INDEX ix_event_queue_event_table_work_item
          ON @extschema@.tb_event_queue( evnet_table_work_item, recorded )
       WHERE execute_asynchronously IS TRUE;

ALTER TABLE @extschema@.tb_action
    ADD COLUMN can_deduplicate BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN can_bulk_execute BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT CHECK( ( can_bulk_execute IS TRUE AND method IS NULL ) OR can_bulk_execute IS FALSE );

COMMENT ON COLUMN @extschema@.tb_action.can_deduplicate IS 'Indicates that this action is fully idempotent and can be deduplicated if multiple instances of this action are found with the same parameters';
COMMENT ON COLUMN @extschema@.tb_action.can_bulk_execute IS 'Indicates that work items of this action can be executed en-masse';

ALTER TABLE @extschema@.tb_event_table_work_item
    ADD COLUMN can_deduplicate BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN can_bulk_execute BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN @extschema@.tb_event_table_work_item.can_deduplicate IS 'Indicates that this ETWI is fully idempotent and can be deduplicated if multiple instances are found with the same parameters. Unlike actions, ETWIs are more reliant on database state and this flag should be more carefully considered';
COMMENT ON COLUMN @extschema@.tb_event_table_work_item.can_bulk_execute IS 'Indicates that this ETWI can be executed with other events of the same type en-masse';
