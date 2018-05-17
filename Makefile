PGLIBDIR     = $(shell pg_config --libdir)
PGINCLUDEDIR = $(shell pg_config --includedir)
CC           = gcc
LIBS         = -lm -lpq -lcurl
CFLAGS       = -I./src/ -I./src/lib/ -I$(PGINCLUDEDIR) -g

event_manager: src/event_manager.o src/lib/util.o src/lib/query_helper.o
	$(CC) -o event_manager src/event_manager.o src/lib/util.o src/lib/query_helper.o -g -I./src/ -I./src/lib/ -L$(PGLIBDIR) -lm -lpq -lcurl

EXTENSION   = event_manager
EXTVERSION  = 0.1
DOCS        = README.md
PG_CONFIG   = pg_config
MODULES     = src/event_manager
EXTRA_CLEAN = src/event_manager.o event_manager src/lib/*.o
DATA        = $(wildcard sql/$(EXTENSION)--*.sql)

PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
