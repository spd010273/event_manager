#define EXTENSION_NAME "event_manager"

/* Static Strings */
static const char * user_agent =
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36";

static const char * usage_string =
    "Usage: event_manager\n"
    "    -U DB User (default: postgres)\n"
    "    -p DB Port (default: 5432)\n"
    "    -h DB Host (default: localhost)\n"
    "    -d DB name (default: DB User)\n"
    "    -E | -W Start Event or Work Queue Processor, respectively\n"
    "  [ -D debug mode\n"
    "    -v VERSION\n"
    "    -? HELP ] \n";

/* Queries */
static const char * extension_check_query =
    "    SELECT n.nspname AS ext_schema "
    "      FROM pg_catalog.pg_extension e "
    "INNER JOIN pg_catalog.pg_namespace n "
    "        ON n.oid = e.extnamespace "
    "     WHERE e.extname = $1 ";

static const char * get_event_queue_item =
    "    SELECT eq.event_table_work_item, "
    "           eq.uid, "
    "           eq.recorded, "
    "           eq.pk_value, "
    "           eq.op, "
    "           eq.action, "
    "           eq.transaction_label, "
    "           eq.work_item_query, "
    "           eq.execute_asynchronously, "
    "           eq.ctid "
    "      FROM " EXTENSION_NAME ".tb_event_queue eq "
    "  ORDER BY eq.recorded DESC "
    "     LIMIT 1 "
    "       FOR UPDATE SKIP LOCKED ";

static const char * delete_event_queue_item =
    "DELETE FROM " EXTENSION_NAME ".tb_event_queue "
    "      WHERE event_table_work_item = $1::INTEGER "
    "        AND uid IS NOT DISTINCT FROM $2::INTEGER "
    "        AND recorded = $3::TIMESTAMP "
    "        AND pk_value = $4::INTEGER "
    "        AND op = $5::CHAR(1) "
    "        AND action = $6::INTEGER "
    "        AND transaction_label IS NOT DISTINCT FROM $7::VARCHAR "
    "        AND work_item_query = $8::TEXT "
    "        AND ctid = $9::TID ";

static const char * get_work_queue_item =
    "   SELECT wq.parameters, "
    "          wq.uid, "
    "          wq.recorded, "
    "          wq.transaction_label, "
    "          wq.action, "
    "          wq.ctid "
    "     FROM " EXTENSION_NAME ".tb_work_queue wq "
    " ORDER BY wq.recorded DESC  "
    "    LIMIT 1 "
    "      FOR UPDATE SKIP LOCKED ";

static const char * delete_work_queue_item =
    "DELETE FROM " EXTENSION_NAME ".tb_work_queue "
    "      WHERE parameters::TEXT IS NOT DISTINCT FROM $1::JSONB::TEXT "
    "        AND uid IS NOT DISTINCT FROM $2::INTEGER "
    "        AND recorded = $3::TIMESTAMP "
    "        AND transaction_label IS NOT DISTINCT FROM $4::VARCHAR "
    "        AND action = $5::INTEGER "
    "        AND ctid = $6::TID ";

static const char * new_work_item_query =
    "INSERT INTO " EXTENSION_NAME ".tb_work_queue "
    "            ( "
    "                parameters, "
    "                uid, "
    "                recorded, "
    "                transaction_label, "
    "                action, "
    "                execute_asynchronously "
    "            ) "
    "     VALUES "
    "            ( "
    "                $1::JSONB, "
    "                NULLIF( $2, '' )::INTEGER, "
    "                COALESCE( NULLIF( $3, '' )::TIMESTAMP, clock_timestamp() ), "
    "                NULLIF( $4, '' )::VARCHAR, "
    "                NULLIF( $5, '' )::INTEGER, "
    "                NULLIF( $6, '' )::BOOLEAN "
    "            ) ";

static const char * get_action =
    "    SELECT a.query, "
    "           a.uri, "
    "           COALESCE( a.method, 'GET' ) AS method "
    "      FROM " EXTENSION_NAME ".tb_action a "
    "     WHERE a.action = $1 ";

static const char * expand_jsonb =
    "    SELECT key, "
    "           value "
    "      FROM jsonb_each_text( $1 ) "
    "     UNION "
    "    SELECT key, "
    "           value "
    "      FROM " EXTENSION_NAME ".tb_action "
    "INNER JOIN jsonb_each_text( static_parameters ) "
    "        ON TRUE "
    "     WHERE action = $2 ";

static const char * _uid_function =
    "    SELECT current_setting( "
    "               '" EXTENSION_NAME ".' || $1::VARCHAR, "
    "               TRUE "
    "           ) AS uid_function ";

static const char * cyanaudit_check =
    "    SELECT p.proname::VARCHAR "
    "      FROM pg_proc p "
    "INNER JOIN pg_namespace n "
    "        ON n.oid = p.pronamespace "
    "       AND n.nspname::VARCHAR = 'cyanaudit' "
    "     WHERE p.proname = 'fn_label_transaction' ";

static const char * cyanaudit_label_tx =
    "    SELECT cyanaudit.fn_label_last_transaction( $1 ) ";