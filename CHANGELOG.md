# Changelog

## 0.1
### Added:
- event_manager

## 0.2
### Added:
- Config Manager process that can remotely SIGHUP event_manager
- Changelog
- GUCs that can disable each queue
- make buildcheck (run event_manager tests w/ valgrind enabled)
- Poisons for unsafe C functions (incomplete)
- CURRENT_VERSION file specified the testing, build, and install version
- Worker scaling from startup values via override GUCs

### Updated:
- development, setup documentation
- automated valgrind arguments to provide more information and trace children
- Extension check now confirms correct version against compiled code

### Fixed:
- Possible SIGSEGV on parent __term() invokation on slow machines / with valgrind running (race condition)
- Possible SIGSEGV in transaction failure marking
- Test harness (run_tests.pl) improperly handling versions
- Usage of strcpy/strcat/strcmp (now slightly less dangerous strncpy/strncat/strncmp)
- Memory leak in stat update (not freeing result handle)


## SECURITY

### 2026-10-06:
- Fixed security issue with a local privelege escalation via abuse of implicit REFERENCE GRANT to tb_event_table_work_item_instance. Users are strongly encouraged to execute the following:
```REVOKE ALL ON event_manager.tb_event_queue FROM public;
REVOKE ALL ON event_manager.tb_work_queue FROM public;
GRANT SELECT, INSERT, UPDATE, DELETE ON event_manager.tb_event_queue TO public;
GRANT SELECT, INSERT, UPDATE, DELETE ON event_manager.tb_work_queue TO public;
REVOKE ALL ON event_manager.tb_statistic FROM public;
GRANT SELECT ON event_manager.tb_statistic TO public;
REVOKE ALL ON event_manager.tb_event_table_work_item FROM public;
GRANT SELECT, INSERT, UPDATE, DELETE ON event_manager.tb_event_table_work_item TO public;
REVOKE ALL ON event_manager.tb_event_table_work_item_instance FROM public;
GRANT SELECT, INSERT, UPDATE, DELETE ON event_manager.tb_event_table_work_item_instance TO public;```

- Fixed security issue where an low priviledge user could override the session_values in queue tables and modify server settings. This has been restricted to any GUC which does not appear in `pg_settings`

This impact both version 0.1 and 0.2 of the extension.
