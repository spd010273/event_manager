# Development

## Installation

Follow the guide in docs/setup.md to get the proper development packages for compilation

the program can be rebuilt easily with
```bash
make clean && make && sudo make install && psql -U postgres -h <host> -p 5432 -d <dbname> -c 'DROP EXTENSION IF EXISTS event_manager; CREATE EXTENSION event_manager;'
```
This rebuilds the code and reinstalls the extension files

The tools directory contains a rebuild script for event manager that updates the extension tables / functions via reinstallation, this file is in tools/update_em.sql

Additionally, there is a setup script in the tools directory that initializes event manager GUCs for your specific application.

## Debugging

To enable debug mode in the Event and Work queue processing daemons, uncomment the line in Makefile 'DEBUG = -DDEBUG -g'
Setting event_manager.debug = 'TRUE' GUC will enable SQL function debug messages. These will be emitted at level DEBUG, you may need to change client_min_messages or log_min_messages in postgresql.conf or with:
```sql
SET client_min_message = 'DEBUG';
```

The event manager daemon has a single-step mode in which it will execute a single event or action, then exit. This can be activated for the appropriate queue with:
```bash
./event_manager <connection_args> -S -E 1 -W 0
```
for debugging the event queue (event_table_work_items) or
```bash
./event_manager <connection_args> -S -E 0 -W 1
```
for debugging the work queue (actions)

Additionally. There are GUCs in version 0.2 which can selectively disable the queue processing. These GUCs are:
```sql
event_manager.disable_work_queue
```
and
```sql
event_manager.disable_event_queue
```
These GUCs are also stored in tb_setting.

## Testing

See docs/test.md for test prerequisites for and installation of tests. This tests the installation and some features of Event Manager, and is useful for validating basic behavior of the extension under development.

## Versioning

This extension uses PGXS. Modifications between versions require files for a clean install of said version, and files that upgrade older versions to the new version, for example. If you were to implement version X.Y, you would need:
* sql/event_manager--X.Y.sql
* sql/event_manager--<old_version>--X.Y.sql

Additionally, you will need to write tests for the new version in test/sql/vX.Y/, and add version X.Y to the version check script tools/valid_version_check.pl, since the CURRENT_VERSION file is expected to be modified by the user depending on their build target.

## Notes

* Remote calls will show as coming from User Agent 'EventManagerbot/0.1 (+https://bitbucket.org/neadwerx/event_manager/src/master/)'
