PLACEHOLDER_QUERY_ID = "[QUERY_ID]"
PLACEHOLDER_LIMIT = "[MIGRATION_LIMIT]"
PLACEHOLDER_TABLE = "[TABLE]"
INSERT_COLUMNS = "[COLUMNS]"
INSERT_VALUES = "[VALUES]"


QUERIES_FIELDS = [
    "id",  # INTEGER
    "timestamp",  # INTEGER NOT NULL
    "type",  # INTEGER NOT NULL
    "status",  # INTEGER NOT NULL
    "domain",  # TEXT NOT NULL
    "client",
    "forward",  # TEXT
    "additional_info",  # TEXT
]

NETWORK_ADDRESSES_FIELDS = [
    "network_id",  # INTEGER NOT NULL,
    "ip",  # TEXT NOT NULL UNIQUE,
    "lastSeen",  # INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS int)),
    "name",  # TEXT,
    "nameUpdated",  # INTEGER
]

MYSQL_QUERIES_FIELDS_MAPPING = {
    0: {
        "name": "query_id",
        "type": "int"
    },
    1: {
        "name": "query_timestamp",
        "type": "timestamp"
    },
    2: {
        "name": "query_type",
        "type": "int"
    },
    3: {
        "name": "query_status",
        "type": "int"
    },
    4: {
        "name": "query_domain",
        "type": "str"
    },
    6: {
        "name": "query_forward",
        "type": "str"
    },
    7: {
        "name": "query_additional_info",
        "type": "str"
    },
    5: {
        "name": "client_ip",
        "type": "str"
    },
    8: {
        "name": "client_network_id",
        "type": "int"
    },
    10: {
        "name": "client_last_seen",
        "type": "timestamp"
    },
    11: {
        "name": "client_name",
        "type": "str"
    },
    12: {
        "name": "client_last_update",
        "type": "timestamp"
    }
}

QUERIES_FIELDS_STR = ", ".join(map('q.{0}'.format, QUERIES_FIELDS))
NETWORK_ADDRESSES_FIELDS_STR = ", ".join(map('na.{0}'.format, NETWORK_ADDRESSES_FIELDS))

DATA_COLUMNS_QUERY = f"{QUERIES_FIELDS_STR}, {NETWORK_ADDRESSES_FIELDS_STR}"
DATA_COLUMNS_RESULT = DATA_COLUMNS_QUERY.replace("na.", "").replace("q.", "")
DATA_COLUMNS_RESULT_ARR = DATA_COLUMNS_RESULT.split(", ")

PIHOLE_LOAD_QUERY = (
    f"SELECT {DATA_COLUMNS_QUERY} "
    "FROM queries as q "
    "LEFT JOIN network_addresses as na "
    "   ON "
    "       na.ip = q.client "
    "WHERE "
    f"   q.id > {PLACEHOLDER_QUERY_ID} "
    "ORDER BY q.id "
    f"LIMIT {PLACEHOLDER_LIMIT};"
)

SQL_COMMAND_MIGRATE = (
    f"INSERT INTO {PLACEHOLDER_TABLE} "
    f"  ({INSERT_COLUMNS}) "
    f"VALUES "
    f"  ({INSERT_VALUES});"
)

SQL_MIGRATION_TABLE_COUNT = (
    f"SELECT table_rows "
    f"FROM information_schema.tables "
    f"WHERE "
    f"  table_name = '{PLACEHOLDER_TABLE}';"
)

SQL_MIGRATION_TABLE_MAX_QUERY_ID = (
    f"SELECT MAX(query_id) "
    f"FROM {PLACEHOLDER_TABLE};"
)
