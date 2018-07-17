# Development

## Installation

Follow the guide in docs/setup.md to get the proper development packages for compilation

the program can be rebuild easily with
```bash
make clean && make && sudo make install && psql -U postgres -h <host> -p 5432 -d <dbname> -c 'DROP EXTENSION IF EXISTS event_manager; CREATE EXTENSION event_manager;'
```
This rebuilds the code and reinstalls the extension files

## Debugging

To enable debug mode in the Event and Work queue processing daemons, uncomment the line in Makefile 'PG_CPPFLAGS = -DDEBUG -g'
Setting event_manager.debug = 'TRUE' GUC will enable SQL function debug messages (these will be emitted at level DEBUG, you may need to change client_min_messages or log_min_messages in postgresql.conf)
 

## Testing

See docs/tests.md for test prerequisites for and installation of tests. This tests the installation and some features of Event Manager, and is useful for validating basic behavior of the extension under development.

## Notes

* Remote calls will show as coming from User Agent 'EventManagerbot/0.1 (+https://bitbucket.org/neadwerx/event_manager/src/master/)'
* 
