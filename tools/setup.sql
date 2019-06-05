DO
 $_$
DECLARE
    my_schema                 VARCHAR := 'event_manager';
    my_execute_asynchronously VARCHAR := 'true';
    my_set_uid_function       VARCHAR := 'fn_setup_entity_session( ?uid?, ?uid? )';
    my_get_uid_function       VARCHAR := 'fn_get_session_entity()';
    my_default_when_function  VARCHAR := 'event_manager.fn_dummy_when_function';
    my_session_gucs           VARCHAR := 'xerp.effective_entity,xerp.entity,event_manager.base_url';
    my_base_url               VARCHAR := 'https://change_me/';
    my_query                  VARCHAR;
BEGIN
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.execute_asynchronously = ''' || my_execute_asynchronously || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.default_when_function = ''' || my_default_when_function || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.set_uid_function = ''' || my_set_uid_function || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.get_uid_function = ''' || my_get_uid_function || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.session_gucs = ''' || my_session_gucs || '''';
    EXECUTE 'ALTER DATABASE "' || current_database() || '" SET ' || my_schema || '.base_url = ''' || my_base_url || '''';

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
                   ''' || my_schema || '.base_url''
               ]::VARCHAR[]
           ) AS key,
           unnest(
               ARRAY[
                   ''' || my_execute_asynchronously || ''',
                   ''' || my_default_when_function || ''',
                   ''' || my_set_uid_function || ''',
                   ''' || my_get_uid_function || ''',
                   ''' || my_session_gucs || ''',
                   ''' || my_base_url || '''
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
                   ''' || my_schema || '.base_url''
               ]::VARCHAR[]
           ) AS key,
           unnest(
               ARRAY[
                   ''' || my_execute_asynchronously || ''',
                   ''' || my_default_when_function || ''',
                   ''' || my_set_uid_function || ''',
                   ''' || my_get_uid_function || ''',
                   ''' || my_session_gucs || ''',
                   ''' || my_base_url || '''
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
