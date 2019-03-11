DO
 $_$
BEGIN
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET event_manager.set_uid_function = ''fn_setup_entity_session( ?uid?, ?uid? )''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET event_manager.get_uid_function = ''fn_get_session_entity()''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET event_manager.session_gucs = ''xerp.effective_entity,xerp.entity,event_manager.base_url''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET event_manager.base_url = ''http://change_me/''';
END
 $_$
LANGUAGE plpgsql;

UPDATE event_manager.tb_setting
   SET value = 'fn_setup_entity_session( ?uid?, ?uid? )'
 WHERE key = 'event_manager.set_uid_function';
UPDATE event_manager.tb_setting
   SET value = 'fn_get_session_entity()'
 WHERE key = 'event_manager.get_uid_function';
UPDATE event_manager.tb_setting
   SET value = 'xerp.effective_entity,xerp.entity,event_manager.base_url'
 WHERE key = 'event_manager.session_gucs';
UPDATE event_manager.tb_setting
   SET value = 'https://change_me/'
 WHERE key = 'event_manager.base_url';
