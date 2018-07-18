# Usage

## Work Item Queries
Event Manager relies on work_item_queries to generate parameters for action queries or remote API calls.

work_item_queries perform several critical tasks:

* Gathering arguments for the action
* Sanity checking the state of the data
* implementing customer/business logic
* Limiting the scope of the action

work item queries can take multiple bindpoints pertaining to the event that happened.
The following values from the new tb_event_queue row will be bound into the query, if a bindpoint exists:

```bash
?work_item_event_table?
?uid?
?recorded?
?pk_value?
?op?
?transaction_label?
```

Work item queries also have access to several JSONB structures, old, new, and session_values

old and new are copies of the OLD and NEW pl/pgsql psuedorecords, respectively. Bindpoints are represented similarly to how they are expressed in pl/pgsql:
```bash
... ?OLD.column_name? ...
```

session_values take the name of their respecive GUC they are copied from, and represent the state of the GUC at the time the event was triggered. These enable session information to be passed to the action in asynchronous mode.
```bash
... ?foo.bar_baz? ...
```

Any unbound bindpoints in a work_item_query are replaced with NULL prior to execution.

The work item query is expected to return JSONB aliased as parameters. These key-value pairs will be used as bindpoints for the action query or parameters in a remote API call

Example work item query:

```sql
    SELECT jsonb_object(
               ARRAY[ f.col_1, br.col_2 ]::TEXT[],
               ARRAY[ bz.col_1, bz.col_2 ]::TEXT[]
           ) AS parameters
      FROM myschema.tb_foo f
INNER JOIN myschema.tb_bar br
        ON br.foo = f.foo
INNER JOIN myschema.tb_baz bz
        ON bz.bar = br.bar
       AND bz.baz = ?pk_value?::INTEGER
INNER JOIN event_manager.tb_event_table_work_item etwi
        ON etwi.event_table_work_item = ?event_table_work_item?::INTEGER
INNER JOIN event_manager.tb_event_table et
        ON et.event_table = etwi.event_table
       AND et.table_name = 'tb_baz'
       AND et.schema_name = 'myschema'
     WHERE ?op?::CHAR(1) = 'U'
       AND ?OLD.foobar?::TEXT IS DISTINCT FROM ?NEW.foobar?
```

Some guidelines to work queries:

* Parameters are bound in using a regular expression in the form of \?key\? or [?]key[?]
* Typecasting is strongly recommended, as translating from JSONB types to SQL types is best-effort (in PostgreSQL).
* It goes without saying that due to this, there is a real SQL injection risk if you allow users to insert directly into these tables
* Work item queries can have a bindpoint make multiple appearances in the same query
* Work item queries that generate multiple rows will result in multiple actions (1:1 to query output)
* Work item queries will have access to the trigger's copy of row psuedorecords NEW and OLD. These will appead in the new and old JSONB entries in tb_work_queue, and can be bound in with ?NEW.<record_column_name>? or ?OLD.<record_column_name>?, for example
* Work item queries and actions will have access to session GUCs specified in event_manager.session_gucs (this is a comma delimited list)
* Any unbound placeholders will be replaced with SQL NULL upon execution

## Actions

Actions can consist of either DML or a URI call.
Actions will consume work_queue items produced by a work_item_query, and take their parameters from both a static parameter list and parameters.
If there is a collision on keys in static_parameters and parameters, the parameter's keys are given priority.

Action queries follow the same rules as work_item_queries for parameter binding.

If an action is a URI call, the parameter list is built in the following form:
```bash
<uri>?param1=value1&param2=value2&....
```

Additionally, a special bindpoint within URIs exists: __BASE_URL__, which will be overwritten with the value of the GUC event_manager.base_url, if present.

## When Function

When functions act as a gatekeeper to the event queue, preventing spurious entries from making their way into the queue.

Below is a useful prototype for a when function:

```sql
CREATE FUNCTION my_when_function
(
    in_event_table_work_item    INTEGER,
    in_pk_value                 INTEGER,
    in_op                       CHAR(1),
    in_new                      JSONB,
    in_old                      JSONB
)
RETURNS BOOLEAN AS
 $_$
    <function body>
 $_$
    LANGUAGE SQL;
```

When functions are executed prior to inserting into the event queue, within the same transaction that is causing the event.

# Synchronous / Asynchronous mode

Event and Action pairs can execute either synchronously or asynchronously. Synchronous execution means the parameters for the action are enumerated and the action is performed within the same transaction that triggered the event. Asynchronous mode issues a NOTIFY to the queue processors when a new queue item is present, and the processing happens outside of the originating transaction.

Some notes about the various modes:

* Remote API calls cannot be made in synchronous mode
* event_table_work_item.execute_asynchronously overrides the global setting (the global setting defines the default for this value)
* Asynchronous mode requires the event_manager process to be started:
  * Start two copies of the process, one with the -W flag (for work queue processing) and another with the -E flag (for event queue processing)

## The Queues

There exist two queues within this extension, tb_event_queue, and tb_work_queue. All of the columns within each queue forms the possible arguments for the query or action that executes from that queue. JSON formatted arguments are expected to be an object (key-value pairs), where the keys are named bind points within the queries and values are what is substituted within those bindpoints.

# The Event Queue

The event queue forms arguments for work_item_queries, and the results of these queries populate tb_work_queue. Upon successful processing of the event, the queue entry is fully dequeued from the table.

# The Work Queue

The work queue lists arguments for actions, and the result of these actions are discarded. Upon successful completion of the action, the entry is fully dequeued from the table.

