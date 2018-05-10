Event Manager
=============

# Summary

Event Manager (event_manager) is a PostgreSQL extension that implements a loosly-coupled event triggering system.

Event Manager consists of two processing loops handing events and work, respectively.

Events can be processed either synchronously or asynchronously in both stages of processing, independently.

![Concept](https://bitbucket.org/neadwerx/event_manager/src/master/images/event_manager_concept.png)

# Usage

## Creating Events
Events can be 'subscribed to' by inserting the table into tb_event_table and creating at least one tb_event_table_work_item for that table.

When DML occurs on that table, the work items are executed.

Work item queries are expected to generate one or more rows of JSONB aliased as parameters which are fed into the action.

## Creating Actions
Actions are either local database modification or remote API calls that happen after an event is processed. Parameters for these calls are gathered from the action's static parameter list, as well as the results from the work item query.

Results of actions are discarded.

# Schema

![Schema](https://bitbucket.org/neadwerx/event_manager/src/master/images/event_manager_schema.png)

## Requirements:
* PostgreSQL 9.4+
* gcc
* PostgreSQL development packages
  * postgresql<version>-devel for CentOS / RHEL
  * libpq-dev and postgresql-server-dev-<version> for Debian / Ubuntu

## Installation
1. Checkout the event_manager source from github
2. Ensure that pg_config is in the user's path
3. Run make to compile the event_manager executable
4. As superuser, run 'make install' with event_manager in your current directory
5. Log into the database server
6. Run 'CREATE EXTENSION event_manager;'
7. Start the event_manager program with connection parameters to your target DB server/instance.

Note: You may need to symling the libpq-fe.h file to /usr/includes. This file is located in /usr/pgsql-<version>/includes/ or wherever your repo version of PostgreSQL installed its includes

## Changelog

### Version 0.1
Initial Version
