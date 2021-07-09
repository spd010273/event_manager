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
- Possible SIGSEVG in transaction failure marking
- Test harness (run_tests.pl) improperly handling versions
- Usage of strcpy/strcat/strcmp (now slightly less dangerous strncpy/strncat/strncmp)
- Memory leak in stat update (not freeing result handle)
