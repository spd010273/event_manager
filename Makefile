PG_CONFIG   ?= pg_config
PGLIBDIR     = $(shell pg_config --libdir)
PGINCLUDEDIR = $(shell pg_config --includedir)
CC           = gcc
LIBS         = -lm -lpq -lcurl
#DEBUG		 = -g -DDEBUG
PG_CPPFLAGS	 = -I./src/ -I./src/lib/ -I$(PGINCLUDEDIR) $(DEBUG) $(LIBS)
EXTENSION    = event_manager
EMPTY		 = ''
VERSION_FILE = CURRENT_VERSION
EXTVERSION 	 = $(shell cat ${VERSION_FILE})
EXT_MAJ_VER  = $(basename $(EXTVERSION))
MIN_VER_SUF  = $(suffix $(EXTVERSION))
EXT_MIN_VER  = $(subst .,,$(MIN_VER_SUF))
DOCS         = README.md
EXTRA_CLEAN  = src/event_manager.o event_manager src/lib/*.o src/lib/version.h
DATA         = $(wildcard sql/$(EXTENSION)--$(EXTVERSION).sql) $(wildcard sql/$(EXTENSION)--*--$(EXTVERSION).sql)

# Echo out the version.h source prior to compile
define __VERSION_H_SOURCE
/*------------------------------------------------------------------------
 *
 * version.h
 *     Versioning information and feature flags
 *
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        version.h
 *
 *------------------------------------------------------------------------
 */

#ifndef VERSION_H
#define VERSION_H

#define TO_STR(x) #x

#define MAJOR_VERSION $(EXT_MAJ_VER)
#define MINOR_VERSION $(EXT_MIN_VER)
#define VERSION TO_STR( $(EXTVERSION) )
#if defined MAJOR_VERSION & MAJOR_VERSION == 0
 #if defined MINOR_VERSION & MINOR_VERSION >= 2
  #define ALLOW_CONFIG_MANAGER
  #define REDEF_CURL_HANDLE
  #define ALLOW_QUEUE_CHECK_WITH_GUC
  #define ALLOW_OVERRIDE_WORKER_COUNTS
 #elif defined MINOR_VERSION & MINOR_VERSION >= 3
  #define ALLOW_BULK_AND_DEDUPE
  #define ALLOW_CACHE
 #endif // MINOR_VERSION
#endif // MAJOR_VERSION

#endif // VERSION_H
endef
export __VERSION_H_SOURCE

event_manager: version.h src/event_manager.o src/lib/util.o src/lib/query_helper.o src/lib/jsmn/jsmn.o src/lib/em_shm.o
	$(CC) -o event_manager src/event_manager.o src/lib/util.o src/lib/query_helper.o src/lib/jsmn/jsmn.o src/lib/em_shm.o -g -I./src/ -I./src/lib/ -I./src/lib/jsmn -L$(PGLIBDIR) -lm -lpq -lcurl ${DEBUG}

version.h:
	./tools/valid_version_check.pl
	$(info ************ Executing Build for Version $(EXTVERSION) ************)
	echo "$$__VERSION_H_SOURCE" > src/lib/version.h

PGXS := $(shell $(PG_CONFIG) --pgxs)

buildcheck:
	$(info ************ Executing Test Harness with Valgrind ************)
	./run_tests -M -D -V

installcheck:
	$(info ************ Executing Test Harness ************)
	./run_tests -M -D

include $(PGXS)
