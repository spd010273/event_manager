
DROP TRIGGER tr_new_enqueue_trigger ON event_manager.tb_event_table_work_item;
ALTER TABLE event_manager.tb_event_table_work_item ALTER COLUMN source_column_name SET DATA TYPE VARCHAR;
CREATE TRIGGER tr_new_enqueue_trigger
    AFTER INSERT OR DELETE OR UPDATE OF source_event_table, source_column_name ON event_manager.tb_event_table_work_item
    FOR EACH ROW EXECUTE PROCEDURE event_manager.fn_manage_trigger_wrapper();
