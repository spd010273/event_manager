CC 		= gcc
LIBS 	= -lm -lpq
CFLAGS 	= -I./src/ -I./src/lib/ -I/usr/pgsql-9.6/include

event_manager: src/event_manager.o
	$(CC) -o event_manager src/event_manager.o -I./src/ -I./src/lib/ -L/usr/pgsql-9.6/lib -lm -lpq

EXTENSION 	= event_manager
EXTVERSION 	= 0.1
DOCS 		= README.md
PG_CONFIG 	= pg_config
MODULES     = src/event_manager
EXTRA_CLEAN = src/event_manager.o event_manager
DATA		= $(wildcard sql/$(EXTENSION)--*.sql)

PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
