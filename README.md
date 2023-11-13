Event Manager
=============

# Summary

Event Manager (event_manager) is a PostgreSQL extension that implements a loosly-coupled event triggering system.

Event Manager consists of two processing loops handing events and work, respectively.

Events can be processed either synchronously or asynchronously.

![Concept](images/event_manager_concept.png | width=400)

Conceptually, Event Manager implements asynchronous database triggers. The event queue is a queue of triggers to be executed, and the work queue being the output of those triggers.

# Usage

For more information on using this extension, see docs/usage.md


## Events

Events can be 'subscribed to' by inserting the table into tb_event_table and creating at least one tb_event_table_work_item for that table.

When DML occurs on that table (an event), the work item entries for that event table are executed. These potential queue elements are optionally filtered by a function ( the when function ) that can determine whether these events apply to this specific work item.

Work item queries are expected to generate one or more rows of JSONB aliased as 'parameters' which are fed into the action. Along with these parameters, the original event's transaction timestamp and other datapoints will be made available to the action.

Conceptually, an event and its subsequent work items operate like a standard database trigger.

## Event Query (work_item_query)

This query gathers the dynamic arguments for the following Action, and together with optional session and static parameters, form the full argument list for the action.

## Actions

Actions are either local database modification or remote API calls that happen after an event is processed. Parameters for these calls are gathered from the action's static parameter list, as well as the results from the work item query, or static parameters stored with the action. Results of actions are currently discarded.

![FlowChart](images/event_manager_flow.png | width=400)

# Schema

![Schema](images/event_manager_schema.png | width=400)

## Requirements:

( See docs/setup.md and docs/test.md for detailed requuirements )

* PostgreSQL 9.6+
* gcc
* libcurl
* PostgreSQL development packages
  * postgresql<version>-devel for CentOS / RHEL
  * libpq-dev and postgresql-server-dev-<version> for Debian / Ubuntu
* Valgrind (optional)

## Installation

( See docs/setup.md for detailed instructions )

1. Checkout the event_manager source from github
2. Ensure that pg_config is in the user's path
3. Follow installation guide in docs/setup.md for setting up and checking prerequisites
4. Run make to compile the event_manager executable
5. As superuser, run the install script in the event_manager repo
6. Log into the database server
7. Run 'CREATE EXTENSION event_manager;'
8. Start event_manager with 'systemctl start event_manager'

## Settings

It is important that event_manager.tb_setting contains appropriate values for your environment. This is especially important if there are events that make API calls. As of version 0.2, event_manager will SIGHUP the queue processors from the database if tb_setting is updated. This will force the workers to reconnect to the database and receive new GUC values (the reconnect is necessary as some poolers cache GUC values)

## Changelog

See CHANGELOG.md
