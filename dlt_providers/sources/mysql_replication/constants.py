# MySQL types that are supported by ConnectorX
# https://sfu-db.github.io/connector-x/databases/mysql.html
# https://github.com/sfu-db/connector-x/blob/main/connectorx/src/transports/mysql_arrow.rs
# Types that are NOT in this set should be cast to CHAR
CONNECTORX_SUPPORTED_MYSQL_TYPES = {
    # Integer types
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
    # Floating point types
    "float",
    "double",
    "decimal",
    "numeric",
    # String types
    "char",
    "varchar",
    "text",
    "tinytext",
    "mediumtext",
    "longtext",
    "binary",
    "varbinary",
    "blob",
    "tinyblob",
    "mediumblob",
    "longblob",
    # Date and time types
    "date",
    "time",
    "datetime",
    "timestamp",
    "year",
    # JSON type
    "json",
    # Boolean type
    "boolean",
    "bool",
    # Bit type
    "bit",
}
