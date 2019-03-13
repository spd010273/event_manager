-- perform an in-place upgrade, this is a one-off for removing the no-trigger column, as we handle that automagically now
SELECT COUNT(*) AS A FROM event_manager.tb_action;
SELECT COUNT(*) AS ET FROM event_manager.tb_event_table;
SELECT COUNT(*) AS ETWI FROM event_manager.tb_event_table_work_item;
SELECT t.tgname, c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE t.tgname = 'tr_event_enqueue';

COPY( SELECT * FROM event_manager.tb_action ) TO '/tmp/tb_action.csv' WITH CSV HEADER;
COPY( SELECT * FROM event_manager.tb_event_table ) TO '/tmp/tb_event_table.csv' WITH CSV HEADER;
COPY( SELECT * FROM event_manager.tb_event_table_work_item ) TO '/tmp/tb_event_table_work_item.csv' WITH CSV HEADER;
COPY( SELECT * FROM event_manager.tb_event_table_work_item_instance ) TO '/tmp/tb_event_table_work_item_instance.csv' WITH CSV HEADER;
COPY( SELECT * FROM event_manager.tb_event_queue ) TO '/tmp/tb_event_queue.csv' WITH CSV HEADER;
COPY( SELECT * FROM event_manager.tb_work_queue ) TO '/tmp/tb_work_queue.csv' WITH CSV HEADER;

DROP EXTENSION event_manager CASCADE;
CREATE EXTENSION event_manager;

CREATE TEMP TABLE tt_et
(
    event_table INTEGER,
    schema_name VARCHAR,
    table_name  VARCHAR,
    no_trigger  BOOLEAN
);

COPY event_manager.tb_action FROM '/tmp/tb_action.csv' WITH CSV HEADER;
COPY tt_et FROM '/tmp/tb_event_table.csv' WITH CSV HEADER;
INSERT INTO event_manager.tb_event_table
            (
                event_table,
                schema_name,
                table_name
            )
     SELECT event_table,
            schema_name,
            table_name
       FROM tt_et;
COPY event_manager.tb_event_table_work_item FROM '/tmp/tb_event_table_work_item.csv' WITH CSV HEADER;
COPY event_manager.tb_event_table_work_item_instance FROM '/tmp/tb_event_table_work_item_instance.csv' WITH CSV HEADER;
COPY event_manager.tb_event_queue FROM '/tmp/tb_event_queue.csv' WITH CSV HEADER;
COPY event_manager.tb_work_queue FROM '/tmp/tb_work_queue.csv' WITH CSV HEADER;

SELECT setval('event_manager.sq_pk_event_table_work_item_instance', MAX( event_table_work_item_instance ) ) FROM event_manager.tb_event_table_work_item_instance;
SELECT setval('event_manager.sq_pk_event_table_work_item', MAX( event_table_work_item ) ) FROM event_manager.tb_event_table_work_item;
SELECT setval('event_manager.sq_pk_event_table', MAX( event_table ) ) FROM event_manager.tb_event_table;
SELECT setval('event_manager.sq_pk_action', MAX( action ) ) FROM event_manager.tb_action;

SELECT COUNT(*) AS A FROM event_manager.tb_action;
SELECT COUNT(*) AS ET FROM event_manager.tb_event_table;
SELECT COUNT(*) AS ETWI FROM event_manager.tb_event_table_work_item;
SELECT t.tgname, c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE t.tgname = 'tr_event_enqueue';
NOTIFY new_event_queue_item;
NOTIFY new_work_queue_item;
