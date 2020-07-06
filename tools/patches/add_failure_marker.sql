ALTER TABLE event_manager.tb_event_queue ADD COLUMN failed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE event_manager.tb_work_queue ADD COLUMN failed BOOLEAN NOT NULL DEFAULT FALSE;
