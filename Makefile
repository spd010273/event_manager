LIBDIR     = $(shell pg_config --libdir)
INCLUDEDIR = $(shell pg_config --includedir)
CC         = gcc
LIBS       = -lm -lpq
CFLAGS     = -I./src/ -I./src/lib/ -I$(INCLUDEDIR)

event_manager: src/event_manager.o
	$(CC) -o event_manager src/event_manager.o -I./src/ -I./src/lib/ -L$(LIBDIR) -lm -lpq

EXTENSION   = event_manager
EXTVERSION  = 0.1
DOCS        = README.md
PG_CONFIG   = pg_config
MODULES     = src/event_manager
EXTRA_CLEAN = src/event_manager.o event_manager
DATA        = $(wildcard sql/$(EXTENSION)--*.sql)

PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)
