DO
 $_$
DECLARE
    my_schema                       VARCHAR := 'event_manager';
    my_execute_asynchronously       VARCHAR := 'true';
    my_set_uid_function             VARCHAR := 'fn_setup_entity_session( ?uid?, ?uid? )';
    my_get_uid_function             VARCHAR := 'fn_get_session_entity()';
    my_default_when_function        VARCHAR := 'event_manager.fn_dummy_when_function';
    my_session_gucs                 VARCHAR := 'xerp.effective_entity,xerp.entity,event_manager.base_url';
    my_base_url                     VARCHAR := 'https://change_me/';
    my_disable_event_queue          VARCHAR := 'false';
    my_disable_work_queue           VARCHAR := 'false';
    my_event_manager_version        VARCHAR := '0.1';
    my_override_event_process_count VARCHAR := '';
    my_override_work_process_count  VARCHAR := '';
    my_query                        VARCHAR;
BEGIN
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.execute_asynchronously = ''' || my_execute_asynchronously || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.default_when_function = ''' || my_default_when_function || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.set_uid_function = ''' || my_set_uid_function || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.get_uid_function = ''' || my_get_uid_function || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.session_gucs = ''' || my_session_gucs || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.base_url = ''' || my_base_url || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.disable_event_queue = ''' || my_disable_event_queue || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.disable_work_queue = ''' || my_disable_work_queue || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.version = ''' || my_event_manager_version || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.override_event_process_count = ''' || my_override_event_process_count || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.override_work_process_count = ''' || my_override_work_process_count || '''';
    my_query := '
WITH tt_data AS
(
    SELECT unnest(
               ARRAY[
                   ''' || my_schema || '.execute_asynchronously'',
                   ''' || my_schema || '.default_when_function'',
                   ''' || my_schema || '.set_uid_function'',
                   ''' || my_schema || '.get_uid_function'',
                   ''' || my_schema || '.session_gucs'',
                   ''' || my_schema || '.base_url'',
                   ''' || my_schema || '.disable_event_queue'',
                   ''' || my_schema || '.disable_work_queue'',
                   ''' || my_schema || '.version'',
                   ''' || my_schema || '.override_event_process_count'',
                   ''' || my_schema || '.override_work_process_count''
               ]::VARCHAR[]
           ) AS key,
           unnest(
               ARRAY[
                   ''' || my_execute_asynchronously || ''',
                   ''' || my_default_when_function || ''',
                   ''' || my_set_uid_function || ''',
                   ''' || my_get_uid_function || ''',
                   ''' || my_session_gucs || ''',
                   ''' || my_base_url || ''',
                   ''' || my_disable_event_queue || ''',
                   ''' || my_disable_work_queue || ''',
                   ''' || my_event_manager_version || ''',
                   ''' || my_override_event_process_count || ''',
                   ''' || my_override_work_process_count || '''
               ]::VARCHAR[]
           ) AS value
)
INSERT INTO ' || my_schema || '.tb_setting
            (
                key,
                value
            )
     SELECT tt.key,
            tt.value
       FROM tt_data tt
  LEFT JOIN ' || my_schema || '.tb_setting s
         ON s.key = tt.key
      WHERE s.value IS NULL';

    EXECUTE my_query;

    my_query := '
WITH tt_data AS
(
    SELECT unnest(
               ARRAY[
                   ''' || my_schema || '.execute_asynchronously'',
                   ''' || my_schema || '.default_when_function'',
                   ''' || my_schema || '.set_uid_function'',
                   ''' || my_schema || '.get_uid_function'',
                   ''' || my_schema || '.session_gucs'',
                   ''' || my_schema || '.base_url'',
                   ''' || my_schema || '.disable_event_queue'',
                   ''' || my_schema || '.disable_work_queue'',
                   ''' || my_schema || '.version''
               ]::VARCHAR[]
           ) AS key,
           unnest(
               ARRAY[
                   ''' || my_execute_asynchronously || ''',
                   ''' || my_default_when_function || ''',
                   ''' || my_set_uid_function || ''',
                   ''' || my_get_uid_function || ''',
                   ''' || my_session_gucs || ''',
                   ''' || my_base_url || ''',
                   ''' || my_disable_event_queue || ''',
                   ''' || my_disable_work_queue || ''',
                   ''' || my_event_manager_version || '''
               ]::VARCHAR[]
           ) AS value
)
    UPDATE ' || my_schema || '.tb_setting s
       SET value = tt.value
      FROM tt_data tt
     WHERE tt.key = s.key';

    EXECUTE my_query;
END
 $_$
LANGUAGE plpgsql;
