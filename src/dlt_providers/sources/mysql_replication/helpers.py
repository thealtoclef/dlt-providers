import fnmatch
import socket
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import connectorx as cx
import dlt
import pendulum
import pymysql
from dlt.common import logger
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.data_writers.escape import (
    escape_hive_identifier as escape_mysql_identifier,
)
from dlt.common.schema.typing import (
    TColumnNames,
    TTableSchema,
    TTableSchemaColumns,
    TWriteDisposition,
)
from dlt.common.schema.utils import merge_column
from dlt.common.typing import TDataItem
from dlt.extract import DltResource
from dlt.extract.items import DataItemWithMeta
from dlt.sources.config import with_config
from dlt.sources.credentials import ConnectionStringCredentials
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import (
    BinLogEvent,
    GtidEvent,
    MariadbGtidEvent,
    RotateEvent,
)
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    RowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from .constants import CONNECTORX_SUPPORTED_MYSQL_TYPES
from .exceptions import StopReplication
from .schema_types import _to_dlt_column_schema, _to_dlt_val


@with_config(
    sections=("sources", "mysql_replication"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
    section_arg_name="server_id",
)
def init_replication(
    schema_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    reset: bool = False,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
    initial_snapshots: bool = True,
) -> Optional[Union[DltResource, List[DltResource]]]:
    """Initializes MySQL replication with comprehensive validation and setup.

    Validates MySQL server configuration, checks user privileges, detects optimal
    replication method (GTID vs binlog coordinates), and creates initial snapshot
    resources using connectorx backend.

    Args:
        server_id (int): MySQL server ID for replication.
        schema_name (str): MySQL database/schema name.
        credentials (ConnectionStringCredentials): MySQL database credentials.
        include_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to include in the snapshot table(s).
          Any column not in the sequence is excluded. If not provided, all columns
          are included. For example:
          ```
          include_columns={
              "table_x": ["col_a", "col_c"],
              "table_y": ["col_x", "col_y", "col_z"],
          }
          ```
          Argument is only used if `initial_snapshots` is `True`.
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the snapshot table resource(s).
          For example:
          ```
          columns={
              "table_x": {"col_a": {"data_type": "json"}},
              "table_y": {"col_y": {"precision": 32}},
          }
          ```
          Argument is only used if `initial_snapshots` is `True`.
        reset (bool): If set to True, clears snapshot completion state to force
          re-snapshot of all tables.
        include_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to include.
          Can be used to filter tables.
        exclude_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to exclude.
          Can be used to filter tables.
        initial_snapshots (bool): Whether to create initial snapshot resources
          using connectorx backend. Snapshots are only created once per table
          and tracked using pipeline state. Each table's snapshot state is checked
          at runtime when the pipeline is active, preventing duplicate snapshots
          on subsequent runs. Use `reset=True` to force re-snapshot.

    Returns:
        - None if `initial_snapshots` is `False`
        - a `DltResource` object or a list of `DltResource` objects for the snapshot
          table(s) if `initial_snapshots` is `True`

    Raises:
        ValueError: If MySQL server configuration is invalid or user lacks privileges
    """

    # List to store resources to be returned
    resources = []

    try:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                if reset:
                    # Clear snapshot completion state to force re-snapshot
                    # Note: We clear state for all tables in this schema
                    source_state = dlt.current.source_state()
                    state_prefix = f"snapshots_completed_{schema_name}_"
                    keys_to_delete = [
                        key
                        for key in source_state.keys()
                        if key.startswith(state_prefix)
                    ]
                    for key in keys_to_delete:
                        del source_state[key]
                    if keys_to_delete:
                        logger.info(
                            f"Cleared {len(keys_to_delete)} snapshot completion states for schema {schema_name}"
                        )

                # Get all tables in schema with include/exclude filters applied
                table_names_only = discover_schema_tables(
                    schema_name, credentials, include_tables, exclude_tables
                )

                if not table_names_only:
                    raise ValueError(
                        f"No tables found in schema '{schema_name}' after filtering."
                    )

                # Verify MySQL replication configuration
                _verify_replication_config(credentials)

                # Verify user privileges for replication
                _verify_replication_privileges(credentials)

                # Save the initial position
                resources.append(
                    save_init_position_resource(
                        schema_name=schema_name, credentials=credentials
                    )
                )

                # Handle initial snapshots
                if initial_snapshots:
                    # Create snapshot resources
                    for table_name in table_names_only:
                        # Get primary key for the table
                        primary_key = _get_pk(cur, table_name, schema_name)
                        # For snapshots, always use append write disposition
                        # since they are initial data loads
                        write_disposition: TWriteDisposition = "append"
                        resources.append(
                            snapshot_table_resource(
                                schema_name=schema_name,
                                table_name=table_name,
                                primary_key=primary_key,
                                write_disposition=write_disposition,
                                columns=columns.get(table_name) if columns else None,
                                credentials=credentials,
                                include_columns=include_columns.get(table_name)
                                if include_columns
                                else None,
                            )
                        )
    except Exception as e:
        logger.error(f"Failed to initialize MySQL replication: {e}")
        raise

    return resources


def build_snapshot_query(
    table_name: str,
    schema_name: str,
    include_columns: Optional[Sequence[str]],
    credentials: ConnectionStringCredentials,
) -> str:
    """Build a SQL query for snapshot with type casting for unsupported MySQL types.

    This function queries the information_schema to get column types and casts
    unsupported types to TEXT to prevent ConnectorX from panicking.

    Args:
        table_name: Name of the table to snapshot
        schema_name: Database schema name
        include_columns: Specific columns to include, if None includes all
        credentials: Database connection credentials

    Returns:
        SQL query string with appropriate type casting
    """

    qualified_table = (
        f"{escape_mysql_identifier(schema_name)}.{escape_mysql_identifier(table_name)}"
    )

    # Get column information
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            # Query to get column names and types using parameterized query
            column_query = """
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            cur.execute(column_query, (schema_name, table_name))
            columns_info = cur.fetchall()

    if not columns_info:
        raise ValueError(f"Table {qualified_table} not found or has no columns")

    # Filter columns if include_columns is specified
    if include_columns:
        include_set = set(include_columns)
        columns_info = [col for col in columns_info if col[0] in include_set]

    # Build SELECT clause with type casting
    select_parts = []
    for column_name, data_type, column_type in columns_info:
        # Use data_type for type checking
        mysql_type = data_type.lower()
        escaped_column = escape_mysql_identifier(column_name)

        # Check if the type is supported by ConnectorX
        if mysql_type in CONNECTORX_SUPPORTED_MYSQL_TYPES:
            select_parts.append(escaped_column)
        else:
            # Cast unsupported types to CHAR for ConnectorX compatibility
            select_parts.append(f"CAST({escaped_column} AS CHAR) AS {escaped_column}")
            logger.debug(
                f"Casting column {escaped_column} ({mysql_type}) to CHAR for ConnectorX compatibility"
            )

    # Build the final query
    select_clause = ", ".join(select_parts)
    query = f"SELECT {select_clause} FROM {qualified_table}"

    logger.debug(f"Built snapshot query: {query}")
    return query


def snapshot_table_resource(
    schema_name: str,
    table_name: str,
    primary_key: TColumnNames,
    write_disposition: TWriteDisposition,
    columns: TTableSchemaColumns = None,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Sequence[str]] = None,
) -> DltResource:
    """Returns a state-aware resource for direct table snapshot using ConnectorX.

    Can be used to perform an initial load of the table, so all data that
    existed in the table prior to initializing replication is also captured.

    The resource is always state-aware and will:
    - Check if snapshots have already been completed and skip execution if they have
    - Mark snapshots as completed after successful execution
    - Use ConnectorX directly for optimal performance
    - Cast unsupported MySQL types to CHAR to prevent ConnectorX panics

    Args:
        schema_name: Database schema name
        table_name: Name of the table in the database and destination
        primary_key: Primary key column(s) for the table
        write_disposition: How to write data (append, replace, merge)
        columns: Column schema hints
        credentials: Database connection credentials
        include_columns: Specific columns to include in the snapshot
    """

    # Create state key for tracking snapshot completion
    state_key = f"snapshots_completed_{schema_name}_{table_name}"

    def read_snapshot_with_connectorx():
        # Check if snapshot was already completed
        source_state = dlt.current.source_state()
        if source_state.get(state_key, False):
            logger.info(
                f"Snapshot already completed for {schema_name}.{table_name}. Skipping."
            )
            return

        # Build the SQL query with type casting for unsupported types
        query = build_snapshot_query(
            table_name, schema_name, include_columns, credentials
        )

        # Use ConnectorX to read data
        try:
            data = cx.read_sql(
                conn=credentials.to_native_representation(),
                query=query,
                protocol="binary",
                return_type="arrow",
            )

            # Yield the data if any was returned
            if len(data) > 0:
                yield data
                # Mark snapshot as completed
                source_state[state_key] = True
                logger.info(
                    f"Marked snapshot as completed for {schema_name}.{table_name}"
                )
            else:
                logger.info(f"No data found for {schema_name}.{table_name}")

        except Exception as e:
            logger.error(f"Error reading snapshot for {schema_name}.{table_name}: {e}")
            raise

    # Create and configure the resource
    resource = dlt.resource(
        read_snapshot_with_connectorx,
        name=f"{schema_name}_{table_name}",
        table_name=table_name,
        write_disposition=write_disposition,
        columns=columns,
        primary_key=primary_key,
    )

    logger.info(f"Created snapshot resource for {schema_name}.{table_name}")

    return resource


def save_init_position_resource(
    schema_name: str,
    credentials: ConnectionStringCredentials,
) -> DltResource:
    """Returns a resource for saving the initial position for the replication resource."""

    # Create state key for tracking initial position completion
    state_key = f"init_position_completed_{schema_name}"

    def save_init_position():
        # Check if initial position was already saved
        source_state = dlt.current.source_state()
        if source_state.get(state_key, False):
            logger.info(f"Initial position already saved for {schema_name}. Skipping.")
            return

        # Capture initial replication position
        try:
            # Capture GTID if enabled
            if _check_gtid_enabled(credentials):
                gtid = _get_current_gtid(credentials)
                logger.info(f"Captured initial GTID: {gtid}")
                source_state["init_gtid"] = gtid
            else:
                # Capture binlog coordinates
                log_file, log_pos = _get_current_log_file_and_pos(credentials)
                logger.info(f"Captured initial binlog position: {log_file}:{log_pos}")
                source_state["init_log_file"] = log_file
                source_state["init_log_pos"] = log_pos
        except Exception as e:
            logger.warning(f"Could not capture replication position: {e}")

        yield []
        # Mark initial position as saved
        source_state[state_key] = True
        logger.info(f"Marked initial position as saved for {schema_name}")

    # Create and configure the resource
    resource = dlt.resource(
        save_init_position,
        name=f"{schema_name}_init_position",
        table_name="init_position",
        write_disposition="replace",
    )

    return resource


def check_schema_exists(
    schema_name: str, credentials: ConnectionStringCredentials
) -> bool:
    """Check if a schema exists in the MySQL database.

    Args:
        schema_name: Database schema name to check
        credentials: MySQL connection credentials

    Returns:
        bool: True if schema exists, False otherwise
    """
    try:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT SCHEMA_NAME 
                    FROM INFORMATION_SCHEMA.SCHEMATA 
                    WHERE SCHEMA_NAME = %s
                """
                cur.execute(query, (schema_name,))
                return bool(cur.fetchone())
    except Exception as e:
        logger.error(f"Failed to check if schema {schema_name} exists: {e}")
        return False


def discover_schema_tables(
    schema_name: str,
    credentials: ConnectionStringCredentials,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
) -> List[str]:
    """Discover tables in the specified schema.

    Args:
        schema_name: Database schema name
        credentials: MySQL connection credentials
        include_tables: Glob patterns for tables to include
        exclude_tables: Glob patterns for tables to exclude

    Returns:
        List of table names that match the include/exclude patterns.
        Raises ValueError if schema doesn't exist.
    """
    if not check_schema_exists(schema_name, credentials):
        raise ValueError(f"Schema {schema_name} does not exist")

    # Get all tables in schema
    try:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                    ORDER BY TABLE_NAME
                """
                cur.execute(query, (schema_name,))
                all_tables = [row[0] for row in cur.fetchall()]

                if not all_tables:
                    logger.warning(f"Schema {schema_name} has no tables")
                    return []

        # Apply table filtering
        return filter_tables(all_tables, include_tables, exclude_tables)
    except Exception as e:
        logger.error(f"Failed to discover tables in schema {schema_name}: {e}")
        return []


def filter_tables(
    all_tables: List[str],
    include_tables: Optional[Union[str, Sequence[str]]] = None,
    exclude_tables: Optional[Union[str, Sequence[str]]] = None,
) -> List[str]:
    """Filter table names using include/exclude patterns.

    Args:
        all_tables: List of table names to filter
        include_tables: Glob patterns for tables to include
        exclude_tables: Glob patterns for tables to exclude

    Returns:
        Filtered list of table names
    """
    if include_tables is None and exclude_tables is None:
        return all_tables

    # Convert single patterns to lists
    if isinstance(include_tables, str):
        include_tables = [include_tables]
    if isinstance(exclude_tables, str):
        exclude_tables = [exclude_tables]

    filtered_tables = all_tables.copy()

    # Apply include filters
    if include_tables:
        included = []
        for table in filtered_tables:
            for pattern in include_tables:
                if fnmatch.fnmatch(table, pattern):
                    included.append(table)
                    break
        filtered_tables = included

    # Apply exclude filters
    if exclude_tables:
        excluded = []
        for table in filtered_tables:
            should_exclude = False
            for pattern in exclude_tables:
                if fnmatch.fnmatch(table, pattern):
                    should_exclude = True
                    break
            if not should_exclude:
                excluded.append(table)
        filtered_tables = excluded

    logger.info(f"Filtered {len(all_tables)} tables to {len(filtered_tables)} tables")

    if not filtered_tables:
        logger.warning("No tables matched the include/exclude patterns")
        return []

    return filtered_tables


def _get_mysql_settings(credentials: ConnectionStringCredentials) -> Dict[str, Any]:
    """Parse MySQL credentials into settings dict."""
    import urllib.parse

    # Parse connection string
    parsed = urllib.parse.urlparse(credentials.to_native_representation())

    return {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "password": parsed.password,
        "database": parsed.path.lstrip("/") if parsed.path else None,
        "charset": "utf8mb4",
        "autocommit": True,
    }


def _get_pk(
    cur: pymysql.cursors.Cursor,
    table_name: str,
    schema_name: str,
) -> Optional[TColumnNames]:
    """Returns primary key column(s) for MySQL table.

    Returns None if no primary key columns exist.
    """
    # Query to get primary key columns for MySQL
    query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
    """
    cur.execute(query, (schema_name, table_name))
    result = [row[0] for row in cur.fetchall()]
    if len(result) == 0:
        return None
    elif len(result) == 1:
        return result[0]  # type: ignore[no-any-return]
    else:
        return result  # type: ignore[no-any-return]


def _get_conn(credentials: ConnectionStringCredentials) -> pymysql.Connection:
    """Returns a pymysql connection to interact with MySQL.

    The connection is configured with autocommit=True for metadata queries.
    Use as a context manager for automatic cleanup.
    """
    mysql_settings = _get_mysql_settings(credentials)
    return pymysql.connect(**mysql_settings)


@dlt.resource(
    name=lambda args: str(args["server_id"]),
    standalone=True,
)
def replication_resource(
    server_id: int,
    schema_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    target_batch_size: int = 1000,
    write_mode: Literal["merge", "append-only"] = "merge",
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
) -> Iterable[TDataItem]:
    """Resource yielding data items for changes in MySQL tables via binlog replication.

    - Uses MySQL binary log replication with GTID or binlog coordinates
    - Maintains replication position in state to track progress
    - Processes events in batches to limit memory usage
    - Supports both merge and append-only write modes

    Args:
        server_id: Unique server ID for MySQL replication connection
        schema_name: MySQL database/schema name
        credentials: MySQL database credentials
        include_columns: Maps table name(s) to sequence of column names to include
        columns: Maps table name(s) to column hints to apply
        target_batch_size: Desired number of data items yielded in a batch
        write_mode: "merge" for final state tables, "append-only" for change stream
        include_tables: Glob patterns for tables to include. If not provided, all tables are included
        exclude_tables: Glob patterns for tables to exclude. These patterns are applied after include_tables

    Yields:
        Data items for changes in the MySQL tables
    """

    # Get current state
    source_state = dlt.current.source_state()
    resource_state = dlt.current.resource_state()

    # Set up binlog stream reader
    options = {
        "connection_settings": _get_mysql_settings(credentials),
        "server_id": server_id,
        "report_slave": socket.gethostname() or "dlt-mysql-replication",
        "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        "only_schemas": [schema_name],
        "only_tables": discover_schema_tables(
            schema_name, credentials, include_tables, exclude_tables
        ),
    }

    use_gtid = _check_gtid_enabled(credentials)
    if use_gtid:
        start_gtid = resource_state.get(
            "last_event_gtid", source_state.get("init_gtid")
        )
        options["only_events"].append(GtidEvent)
    else:
        start_log_file = resource_state.get(
            "last_event_log_file", source_state.get("init_log_file")
        )
        start_log_pos = resource_state.get(
            "last_event_log_pos", source_state.get("init_log_pos")
        )
        end_log_file, end_log_pos = _get_current_log_file_and_pos(credentials)
        options["only_events"].append(RotateEvent)
        options["resume_stream"] = True

    # generate items in batches
    while True:
        gen_kwargs = {
            "credentials": credentials,
            "server_id": server_id,
            "options": options,
            "end_log_file": end_log_file,
            "end_log_pos": end_log_pos,
            "target_batch_size": target_batch_size,
            "include_columns": include_columns,
            "columns": columns,
            "write_mode": write_mode,
        }
        if use_gtid:
            gen_kwargs["start_gtid"] = start_gtid
        else:
            gen_kwargs["start_log_file"] = start_log_file
            gen_kwargs["start_log_pos"] = start_log_pos
        gen = ItemGenerator(**gen_kwargs)
        yield from gen
        if gen.generated_all:
            resource_state["last_event_gtid"] = gen.last_event_gtid
            resource_state["last_event_log_file"] = gen.last_event_log_file
            resource_state["last_event_log_pos"] = gen.last_event_log_pos
            break
        start_gtid = gen.last_event_gtid
        start_log_file = gen.last_event_log_file
        start_log_pos = gen.last_event_log_pos


def _verify_replication_config(credentials: ConnectionStringCredentials) -> None:
    """Verify MySQL server configuration for replication.

    Checks:
    - Binary logging is enabled
    - binlog_format is set to 'ROW'
    - binlog_row_image is set to 'FULL' (MySQL 5.6.2+)
    """
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            # Check if binary logging is enabled
            cur.execute("SHOW MASTER STATUS")
            result = cur.fetchone()
            if result is None:
                raise ValueError(
                    "MySQL binary logging is not enabled. Please enable binary logging."
                )

            # Check binlog_format
            cur.execute("SELECT @@binlog_format")
            binlog_format = cur.fetchone()[0]
            if binlog_format != "ROW":
                raise ValueError(
                    f"Unable to replicate binlog stream because binlog_format is "
                    f"not set to 'ROW': {binlog_format}. Please set binlog_format=ROW."
                )

            # Check binlog_row_image (MySQL 5.6.2+)
            try:
                cur.execute("SELECT @@binlog_row_image")
                binlog_row_image = cur.fetchone()[0]
                if binlog_row_image != "FULL":
                    raise ValueError(
                        f"Unable to replicate binlog stream because binlog_row_image is "
                        f"not set to 'FULL': {binlog_row_image}. Please set binlog_row_image=FULL."
                    )
            except pymysql.err.InternalError as ex:
                if ex.args[0] == 1193:
                    raise ValueError(
                        "Unable to replicate binlog stream because binlog_row_image "
                        "system variable does not exist. MySQL version must be at "
                        "least 5.6.2 to use binlog replication."
                    ) from ex
                raise


def _verify_replication_privileges(credentials: ConnectionStringCredentials) -> None:
    """Verify that the MySQL user has the necessary privileges for replication.

    Required privileges:
    - REPLICATION SLAVE
    - REPLICATION CLIENT
    - SELECT (on tables to be replicated)
    """
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            # Get current user
            cur.execute("SELECT USER()")
            current_user = cur.fetchone()[0]

            # Check privileges
            cur.execute("SHOW GRANTS")
            grants = [row[0] for row in cur.fetchall()]

            has_replication_slave = False
            has_replication_client = False
            has_select = False

            for grant in grants:
                grant_upper = grant.upper()
                if "REPLICATION SLAVE" in grant_upper:
                    has_replication_slave = True
                if "REPLICATION CLIENT" in grant_upper:
                    has_replication_client = True
                if "SELECT" in grant_upper or "ALL PRIVILEGES" in grant_upper:
                    has_select = True

            missing_privileges = []
            if not has_replication_slave:
                missing_privileges.append("REPLICATION SLAVE")
            if not has_replication_client:
                missing_privileges.append("REPLICATION CLIENT")
            if not has_select:
                missing_privileges.append("SELECT")

            if missing_privileges:
                raise ValueError(
                    f"User {current_user} is missing required privileges for replication: "
                    f"{', '.join(missing_privileges)}. Please grant these privileges."
                )


def _check_gtid_enabled(credentials: ConnectionStringCredentials) -> bool:
    """Check if GTID is enabled on the MySQL server."""
    try:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT @@gtid_mode")
                result = cur.fetchone()
                if result and result[0] == "ON":
                    return True
                return False
    except Exception as e:
        logger.warning(f"Could not check GTID mode: {e}")
        return False


def _get_current_gtid(credentials: ConnectionStringCredentials) -> str:
    """Get the current GTID from the MySQL server."""
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT @@gtid_executed")
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            raise ValueError("No GTID available")


def _get_current_log_file_and_pos(
    credentials: ConnectionStringCredentials,
) -> Tuple[str, int]:
    """Get the current binlog file and position from the MySQL server."""
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW MASTER STATUS")
            result = cur.fetchone()
            if result:
                return result[0], result[1]
            raise ValueError("No master status available")


def _make_lsn(log_file: str, log_pos: int) -> int:
    """Create a single comparable integer (Log Sequence Number) from log file and position.

    The LSN is constructed as (file_number << 32) | log_pos, where:
    - file_number is the numeric part of the log file name (e.g., 1 for 'binlog.000001')
    - log_pos is the position within the log file

    Comparison of two LSNs is equivalent to first comparing their file numbers,
    and if equal, then comparing their positions.

    Example:
        _make_lsn('binlog.000001', 100) < _make_lsn('binlog.000002', 0)  # True
        _make_lsn('binlog.000001', 100) < _make_lsn('binlog.000001', 200)  # True
    """
    # Extract the numeric part after the last dot
    file_number = int(log_file.split(".")[-1])
    # Combine into a single 64-bit integer (file_number in upper 32 bits, position in lower 32 bits)
    return (file_number << 32) | log_pos


def _make_qualified_table_name(table_name: str, schema_name: str) -> str:
    """Escapes and combines a schema and table name."""
    return (
        escape_mysql_identifier(schema_name) + "." + escape_mysql_identifier(table_name)
    )


@dataclass
class ItemGenerator:
    """Generator for MySQL binlog events.

    Yields batches of data items from the MySQL binary log.
    Maintains state between batches to allow for resuming from the last processed position.
    """

    credentials: ConnectionStringCredentials
    server_id: int
    options: Dict[str, str]
    end_log_file: Optional[str] = None
    end_log_pos: Optional[int] = None
    start_gtid: Optional[str] = None
    start_log_file: Optional[str] = None
    start_log_pos: Optional[int] = None
    target_batch_size: int = 1000
    include_columns: Optional[Dict[str, Sequence[str]]] = None
    columns: Optional[Dict[str, TTableSchemaColumns]] = None
    write_mode: Literal["merge", "append-only"] = "merge"
    last_event_gtid: Optional[str] = field(default=None, init=False)
    last_event_log_file: Optional[str] = field(default=None, init=False)
    last_event_log_pos: Optional[int] = field(default=None, init=False)
    generated_all: bool = False

    def __iter__(self) -> Iterator[Union[TDataItem, DataItemWithMeta]]:
        """Yields replication messages from MessageConsumer.

        Starts consuming binlog events from the MySQL server.
        Maintains position of last consumed event in object state.
        """

        reader = BinLogStreamReader(
            **self.options,
            auto_position=self.start_gtid,
            log_file=self.start_log_file,
            log_pos=self.start_log_pos,
        )

        consumer = MessageConsumer(
            reader=reader,
            end_log_file=self.end_log_file,
            end_log_pos=self.end_log_pos,
            target_batch_size=self.target_batch_size,
            include_columns=self.include_columns,
            columns=self.columns,
            write_mode=self.write_mode,
            credentials=self.credentials,
        )

        try:
            for event in reader:
                consumer.process_msg(event, reader)
            consumer.consumed_all = True
        except StopReplication:  # completed batch or reached end position
            pass
        finally:
            reader.close()

        # yield data items
        for rel_id, data_items in consumer.data_items.items():
            table_name = consumer.last_table_schema[rel_id]["name"]
            yield data_items[0]  # meta item with column hints only, no data
            yield dlt.mark.with_table_name(data_items[1:], table_name)

        # update state
        self.last_event_gtid = consumer.last_event_gtid
        self.last_event_log_file = consumer.last_event_log_file
        self.last_event_log_pos = consumer.last_event_log_pos
        self.generated_all = consumer.consumed_all


class MessageConsumer:
    """Consumes MySQL binlog events and converts them to dlt-compatible data items.

    Processes binlog events in batches and maintains table schema information.
    """

    def __init__(
        self,
        reader: BinLogStreamReader,
        end_log_file: str,
        end_log_pos: int,
        target_batch_size: int = 1000,
        include_columns: Optional[Dict[str, Sequence[str]]] = None,
        columns: Optional[Dict[str, TTableSchemaColumns]] = None,
        write_mode: Literal["merge", "append-only"] = "merge",
        credentials: Optional[ConnectionStringCredentials] = None,
    ) -> None:
        self.end_log_file = end_log_file
        self.end_log_pos = end_log_pos
        self.end_lsn = _make_lsn(end_log_file, end_log_pos)
        self.target_batch_size = target_batch_size
        self.include_columns = include_columns
        self.columns = columns
        self.write_mode = write_mode
        self.credentials = credentials

        # Cache for primary keys: maps (schema_name, table_name) -> list of pk column names
        self._pk_cache: Dict[Tuple[str, str], List[str]] = {}

        self.consumed_all: bool = False
        # data_items attribute maintains all data items
        self.data_items: Dict[
            int, List[Union[TDataItem, DataItemWithMeta]]
        ] = {}  # maps relation_id to list of data items
        # other attributes only maintain last-seen values
        self.last_table_schema: Dict[
            int, TTableSchema
        ] = {}  # maps relation_id to table schema
        self.last_event_gtid = None
        self.last_event_log_file = None
        self.last_event_log_pos = None

    def process_msg(self, event: BinLogEvent, reader: BinLogStreamReader) -> None:
        """Processes encoded replication message.

        Identifies message type and decodes accordingly.
        Message treatment is different for various message types.
        Breaks out of stream with StopReplication exception when
        - `end_lsn` is reached
        - `target_batch_size` is reached
        - a table's schema has changed
        """

        # Get current position and create a comparable position integer
        event_log_file = reader.log_file
        event_log_pos = reader.log_pos
        event_lsn = _make_lsn(event_log_file, event_log_pos)

        # Check if we've reached or passed the end position
        if event_lsn >= self.end_lsn:
            self.last_event_log_file = event_log_file
            self.last_event_log_pos = event_log_pos
            self.consumed_all = True
            raise StopReplication("End of binlog reached")

        if isinstance(event, (GtidEvent, MariadbGtidEvent)):
            self.last_event_gtid = event.gtid

            # There is strange behavior happening when using GTID in the pymysqlreplication lib,
            # explained here: https://github.com/noplay/python-mysql-replication/issues/367
            # Fix: Updating the reader's auto-position to the newly encountered gtid means we won't have to restart
            # consuming binlog from old GTID pos when connection to server is lost.
            reader.auto_position = self.last_event_gtid
        elif isinstance(event, RotateEvent):
            self.last_event_log_file = event.next_binlog
            self.last_event_log_pos = event.position
        elif isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
            self.process_relation(event)
            self.process_change(event, event_lsn)
        else:
            logger.warning(
                f"Events with type {type(event)} are currently not supported. They are ignored."
            )

        # update state
        self.last_event_log_file = event_log_file
        self.last_event_log_pos = event_log_pos

    def _get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table, using cache if available."""
        cache_key = (schema_name, table_name)
        if cache_key not in self._pk_cache and self.credentials:
            with _get_conn(self.credentials) as conn:
                with conn.cursor() as cur:
                    pk = _get_pk(cur, table_name, schema_name)
                    self._pk_cache[cache_key] = (
                        [pk] if isinstance(pk, str) else (pk or [])
                    )
        return self._pk_cache.get(cache_key, [])

    def _is_schema_changed(self, event: RowsEvent) -> bool:
        """Check if the schema of the table has changed.

        Args:
            event: The binlog event containing row data

        Returns:
            bool: True if the schema has changed for tracked columns, False otherwise
        """
        schema_name = event.schema
        table_name = event.table
        relation_id = f"{schema_name}.{table_name}"

        # If we have no previous schema, it's a new table so no change
        if relation_id not in self.data_items:
            return False

        last_columns = set(self.last_table_schema[relation_id]["columns"].keys())
        event_columns = set([col.name for col in event.columns])

        # If include_columns is defined, only check those columns in the event
        if self.include_columns and table_name in self.include_columns:
            included_columns = set(self.include_columns[table_name])
            event_columns = event_columns.intersection(included_columns)

        # Compare the filtered column sets
        return last_columns != event_columns

    def process_relation(self, event: RowsEvent) -> None:
        """Processes a replication message of type Relation.

        Stores table schema in object state.
        Creates meta item to emit column hints while yielding data.

        Raises StopReplication when a table's schema changes.
        """

        # get table schema information from source and store in object state
        schema_name = event.schema
        table_name = event.table
        relation_id = f"{schema_name}.{table_name}"

        # raise StopReplication if table schema has changed
        if self._is_schema_changed(event):
            raise StopReplication(
                f"Table {schema_name}.{table_name} has changed schema."
            )

        columns: TTableSchemaColumns = {
            c.name: _to_dlt_column_schema(c) for c in event.columns
        }

        self.last_table_schema[relation_id] = {
            "name": table_name,
            "columns": columns,
        }

        # apply user input
        # 1) exclude columns
        include_columns = (
            None
            if self.include_columns is None
            else self.include_columns.get(table_name)
        )
        if include_columns is not None:
            columns = {k: v for k, v in columns.items() if k in include_columns}
        # 2) override source hints
        column_hints: TTableSchemaColumns = (
            {} if self.columns is None else self.columns.get(table_name, {})
        )
        for column_name, column_val in column_hints.items():
            columns[column_name] = merge_column(columns[column_name], column_val)

        # add hints for replication columns
        columns["_dlt_lsn"] = {
            "dedup_sort": "desc",
            "data_type": "bigint",
            "nullable": True,
        }
        columns["_dlt_deleted_ts"] = {
            "hard_delete": True,
            "data_type": "timestamp",
            "nullable": True,
        }

        # determine write disposition based on write_mode
        write_disposition: TWriteDisposition = (
            "append" if self.write_mode == "append-only" else "merge"
        )

        # Get primary keys for this table
        primary_keys = self._get_primary_keys(schema_name, table_name)

        # include meta item to emit hints while yielding data
        meta_item = dlt.mark.with_hints(
            [],
            dlt.mark.make_hints(
                table_name=table_name,
                write_disposition=write_disposition,
                columns=columns,
                primary_key=primary_keys,
            ),
            create_table_variant=True,
        )
        self.data_items[relation_id] = [meta_item]

    def process_change(self, event: BinLogEvent, lsn: int) -> None:
        """Process row event (insert, update, delete) from MySQL binlog."""

        schema_name = event.schema
        table_name = event.table
        relation_id = f"{schema_name}.{table_name}"

        if isinstance(event, UpdateRowsEvent):
            values_key = "after_values"
        else:
            values_key = "values"

        for row in event.rows:
            data_item = self.gen_data_item(
                data=row[values_key],
                column_type_ids={col.name: col.type for col in event.columns},
                column_schema=self.last_table_schema[relation_id]["columns"],
                lsn=lsn,
                event_ts=event.timestamp,
                for_delete=isinstance(event, DeleteRowsEvent),
                include_columns=(
                    None
                    if self.include_columns is None
                    else self.include_columns.get(table_name)
                ),
            )
            self.data_items[relation_id].append(data_item)

    @staticmethod
    def gen_data_item(
        data: Dict[str, Any],
        column_type_ids: Dict[str, int],
        column_schema: TTableSchemaColumns,
        lsn: int,
        event_ts: pendulum.DateTime,
        for_delete: bool,
        include_columns: Optional[Sequence[str]] = None,
    ) -> TDataItem:
        """Generates data item from row event data and corresponding metadata."""
        data_item = {
            col_name: _to_dlt_val(
                val=col_value,
                type_id=column_type_ids[col_name],
                data_type=column_schema[col_name]["data_type"],
                for_delete=for_delete,
            )
            for col_name, col_value in data.items()
            if (include_columns is None or col_name in include_columns)
        }
        data_item["_dlt_lsn"] = lsn
        if for_delete:
            data_item["_dlt_deleted_ts"] = event_ts
        return data_item
