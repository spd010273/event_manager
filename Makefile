PGLIBDIR     = $(shell pg_config --libdir)
PGINCLUDEDIR = $(shell pg_config --includedir)
CC           = gcc
LIBS         = -lm -lpq -lcurl
CFLAGS       = -I./src/ -I./src/lib/ -I$(PGINCLUDEDIR) -g -DDEBUG

event_manager: src/event_manager.o src/lib/util.o src/lib/query_helper.o src/lib/jsmn/jsmn.o
	$(CC) -o event_manager src/event_manager.o src/lib/util.o src/lib/query_helper.o src/lib/jsmn/jsmn.o -g -I./src/ -I./src/lib/ -I./src/lib/jsmn -L$(PGLIBDIR) -lm -lpq -lcurl -DDEBUG

EXTENSION   = event_manager
EXTVERSION  = 0.1
DOCS        = README.md
PG_CONFIG   = pg_config
MODULES     = src/event_manager
EXTRA_CLEAN = src/event_manager.o event_manager src/lib/*.o
PG_CPPFLAGS = -DDEBUG -g
DATA        = $(wildcard sql/$(EXTENSION)--*.sql)

PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
