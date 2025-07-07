import fnmatch
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import connectorx as cx
import dlt
import psycopg2
from dlt.common import logger
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.data_writers.escape import escape_postgres_identifier
from dlt.common.pendulum import pendulum
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
from psycopg2.extensions import connection as ConnectionExt
from psycopg2.extensions import cursor
from psycopg2.extras import (
    LogicalReplicationConnection,
    ReplicationCursor,
    ReplicationMessage,
    StopReplication,
)

from .constants import CONNECTORX_SUPPORTED_PG_TYPES
from .decoders import (
    Begin,
    ColumnData,
    Delete,
    Insert,
    Relation,
    Update,
)
from .exceptions import PublicationNotFoundError
from .schema_types import _to_dlt_column_schema, _to_dlt_val


def source_setup(
    credentials: ConnectionStringCredentials,
    publication_name: str,
    slot_name: str,
    schema_name: str,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
    reset: bool = False,
    manage_publication: bool = True,
) -> Dict[str, Any]:
    """Setup for pg_replication source.

    Args:
        credentials (ConnectionStringCredentials): Database connection credentials.
        schema_name (str): The schema to replicate tables from.
        publication_name (str): The name of the publication to create or use.
        slot_name (str): The name of the replication slot to create or use.
        reset (bool): If set to True, the existing slot and publication are dropped
            and recreated. Has no effect if a slot and publication with the provided
            names do not yet exist.
        include_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to include.
            If not provided, all tables in the schema are included.
        exclude_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to exclude.
            These patterns are applied after include_tables.
        manage_publication (bool): Whether to create and drop publication
            automatically. If set to False, the publication must exist before
            running the pipeline.

    Returns:
        Dict[str, Any]: Dictionary containing the following derived information:
            - 'tables': List of table names after filtering
            - 'setup_resources': List of setup resources
    """
    setup_resources = []

    rep_conn = _get_rep_conn(credentials)
    try:
        with rep_conn.cursor() as rep_cur:
            if reset:
                drop_replication_slot(slot_name, rep_cur)
                if manage_publication:
                    drop_publication(publication_name, rep_cur)
                # Clear snapshot completion state to force re-snapshot
                # Note: We clear state for all tables in this schema/publication
                source_state = dlt.current.source_state()
                state_prefix = f"snapshots_completed_{schema_name}_"
                keys_to_delete = [
                    key for key in source_state.keys() if key.startswith(state_prefix)
                ]
                for key in keys_to_delete:
                    del source_state[key]
                if keys_to_delete:
                    logger.info(
                        f"Cleared {len(keys_to_delete)} snapshot completion states for schema {schema_name}"
                    )

            # Check if publication exists and get its tables
            publication_already_exist = False
            try:
                publication_tables = get_publication_tables(publication_name, rep_cur)
                publication_already_exist = True
            except PublicationNotFoundError:
                if manage_publication:
                    logger.info(
                        f"Creating publication '{publication_name}' as it doesn't exist"
                    )
                    create_publication(publication_name, rep_cur)
                else:
                    raise ValueError(
                        f"Publication '{publication_name}' does not exist and manage_publication is False. "
                        "Please create the publication manually or set manage_publication=True."
                    )

            # Get all tables in schema with include/exclude filters applied
            tables = discover_schema_tables(
                credentials=credentials,
                schema_name=schema_name,
                include_tables=include_tables,
                exclude_tables=exclude_tables,
            )
            if not tables:
                raise ValueError(
                    f"No tables found in schema '{schema_name}' after filtering"
                )

            # Verify that publication includes the needed tables if it already exists
            if publication_already_exist:
                qualified_tables = [f"{schema_name}.{table}" for table in tables]
                missing_tables = [
                    table
                    for table in qualified_tables
                    if table not in publication_tables
                ]

                if missing_tables:
                    raise ValueError(
                        f"Publication '{publication_name}' is missing required tables: {missing_tables}. "
                        "Please ensure the publication includes these tables before proceeding."
                    )

            # Create replication slot if it doesn't exist
            create_replication_slot(slot_name, rep_cur)

    except Exception:
        rep_conn.rollback()
        raise
    else:
        rep_conn.commit()
    finally:
        rep_conn.close()

    return {
        "tables": tables,
        "setup_resources": setup_resources,
    }


def create_publication(
    name: str,
    cur: cursor,
) -> None:
    """Creates a publication for logical replication at database level if it doesn't exist.

    Does nothing if the publication already exists.
    Raises error if the user does not have the CREATE privilege for the database.
    """
    esc_name = escape_postgres_identifier(name)
    try:
        cur.execute(
            f"CREATE PUBLICATION {esc_name} FOR ALL TABLES WITH (publish = 'insert, update, delete');"
        )
        logger.info(
            f"Successfully created publication {esc_name} for all tables with publish = 'insert, update, delete'."
        )
    except psycopg2.errors.DuplicateObject:  # the publication already exists
        logger.info(f'Publication "{name}" already exists.')


def create_replication_slot(  # type: ignore[return]
    name: str, cur: ReplicationCursor, output_plugin: str = "pgoutput"
) -> Optional[Dict[str, str]]:
    """Creates a replication slot if it doesn't exist yet."""
    try:
        cur.create_replication_slot(name, output_plugin=output_plugin)
        logger.info(f'Successfully created replication slot "{name}".')
        result = cur.fetchone()
        return {
            "slot_name": result[0],
            "consistent_point": result[1],
            "snapshot_name": result[2],
            "output_plugin": result[3],
        }
    except psycopg2.errors.DuplicateObject:  # the replication slot already exists
        logger.info(
            f'Replication slot "{name}" cannot be created because it already exists.'
        )


def drop_replication_slot(name: str, cur: ReplicationCursor) -> None:
    """Drops a replication slot if it exists."""
    try:
        cur.drop_replication_slot(name)
        logger.info(f'Successfully dropped replication slot "{name}".')
    except psycopg2.errors.UndefinedObject:  # the replication slot does not exist
        logger.info(
            f'Replication slot "{name}" cannot be dropped because it does not exist.'
        )


def drop_publication(name: str, cur: ReplicationCursor) -> None:
    """Drops a publication if it exists."""
    esc_name = escape_postgres_identifier(name)
    try:
        cur.execute(f"DROP PUBLICATION {esc_name};")
        logger.info(f"Successfully dropped publication {esc_name}.")
    except psycopg2.errors.UndefinedObject:  # the publication does not exist
        logger.info(
            f"Publication {esc_name} cannot be dropped because it does not exist."
        )


def build_snapshot_query(
    table_name: str,
    schema_name: str,
    include_columns: Optional[Sequence[str]],
    credentials: ConnectionStringCredentials,
) -> str:
    """Build a SQL query for snapshot with type casting for unsupported PostgreSQL types.

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

    qualified_table = f"{escape_postgres_identifier(schema_name)}.{escape_postgres_identifier(table_name)}"

    # Get column information
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            # Query to get column names and types
            query = """
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """
            cur.execute(query, (schema_name, table_name))

            columns_info = cur.fetchall()

    if not columns_info:
        raise ValueError(f"Table {qualified_table} not found or has no columns")

    # Filter columns if include_columns is specified
    if include_columns:
        include_set = set(include_columns)
        columns_info = [col for col in columns_info if col[0] in include_set]

    # Build SELECT clause with type casting
    select_parts = []
    for column_name, data_type, udt_name in columns_info:
        # Use udt_name for more specific type information (e.g., custom types, enums)
        pg_type = udt_name.lower() if udt_name else data_type.lower()
        escaped_column = escape_postgres_identifier(column_name)

        # Check if the type is supported by ConnectorX
        if pg_type in CONNECTORX_SUPPORTED_PG_TYPES:
            select_parts.append(escaped_column)
        else:
            # Cast unsupported types to text
            select_parts.append(f"{escaped_column}::text")
            logger.debug(
                f"Casting column {escaped_column} ({pg_type}) to TEXT for ConnectorX compatibility"
            )

    # Build the final query
    select_clause = ", ".join(select_parts)
    query = f"SELECT {select_clause} FROM {qualified_table}"

    logger.debug(f"Built snapshot query: {query}")
    return query


@with_config(
    sections=("sources", "pg_replication"),
    sections_merge_style=ConfigSectionContext.resource_merge_style,
    section_arg_name="slot_name",
)
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
    - Cast unsupported PostgreSQL types to TEXT to prevent ConnectorX panics

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


def create_snapshot_resources(
    credentials: ConnectionStringCredentials,
    schema_name: str,
    tables: List[str],
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
) -> List[DltResource]:
    """Create snapshot resources for the specified tables.

    Args:
        tables: List of table names to create snapshot resources for
        schema_name: Name of the schema containing the tables
        credentials: Database connection credentials
        include_columns: Optional mapping of table names to columns to include
        columns: Optional column schema hints for the tables

    Returns:
        List of DltResource objects for the snapshot tables
    """
    resources = []

    for table_name in tables:
        # Get primary key for the table
        primary_key = _get_pk(credentials, schema_name, table_name)
        # For snapshots, always use append write disposition since they are initial data loads
        write_disposition: TWriteDisposition = "append"

        # Get include_columns for this table if specified
        table_include_columns = None
        if include_columns and table_name in include_columns:
            table_include_columns = include_columns[table_name]

        # Get columns for this table if specified
        table_columns = None
        if columns and table_name in columns:
            table_columns = columns[table_name]

        # Create the snapshot resource
        resource = snapshot_table_resource(
            schema_name=schema_name,
            table_name=table_name,
            primary_key=primary_key,
            write_disposition=write_disposition,
            columns=table_columns,
            credentials=credentials,
            include_columns=table_include_columns,
        )
        resources.append(resource)

    return resources


def get_max_lsn(
    slot_name: str,
    options: Dict[str, str],
    credentials: ConnectionStringCredentials,
) -> Optional[int]:
    """Returns maximum Log Sequence Number (LSN) in replication slot.

    Returns None if the replication slot is empty.
    Does not consume the slot, i.e. messages are not flushed.
    Raises error if the replication slot or publication does not exist.
    """
    # comma-separated value string
    options_str = ", ".join(
        f"'{x}'" for xs in list(map(list, options.items())) for x in xs
    )
    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(lsn) - '0/0' AS max_lsn "  # subtract '0/0' to convert pg_lsn type to int (https://stackoverflow.com/a/73738472)
                f"FROM pg_logical_slot_peek_binary_changes('{slot_name}', NULL, NULL, {options_str});"
            )
            lsn: int = cur.fetchone()[0]
        return lsn


def lsn_int_to_hex(lsn: int) -> str:
    """Convert integer LSN to postgres' hexadecimal representation."""
    # https://stackoverflow.com/questions/66797767/lsn-external-representation.
    return f"{lsn >> 32 & 4294967295:X}/{lsn & 4294967295:08X}"


def advance_slot(
    end_lsn: int,
    slot_name: str,
    credentials: ConnectionStringCredentials,
) -> None:
    """Advances position in the replication slot.

    Flushes all messages upto (and including) the message with LSN = `end_lsn`.
    This function is used as alternative to psycopg2's `send_feedback` method, because
    the behavior of that method seems odd when used outside of `consume_stream`.
    """
    if end_lsn != 0:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM pg_replication_slot_advance('{slot_name}', '{lsn_int_to_hex(end_lsn)}');"
                )


def _get_conn(
    credentials: ConnectionStringCredentials,
    connection_factory: Optional[Any] = None,
) -> ConnectionExt:
    """Returns a psycopg2 connection to interact with postgres."""
    conn = psycopg2.connect(
        dsn=credentials.to_native_representation(),
        connection_factory=connection_factory,
    )
    if connection_factory is not LogicalReplicationConnection:
        conn.autocommit = False
    return conn  # type: ignore[no-any-return]


def _get_rep_conn(
    credentials: ConnectionStringCredentials,
) -> LogicalReplicationConnection:
    """Returns a psycopg2 LogicalReplicationConnection to interact with postgres replication functionality.

    Raises error if the user does not have the REPLICATION attribute assigned.
    """
    return _get_conn(credentials, LogicalReplicationConnection)  # type: ignore[return-value]


def _make_qualified_table_name(schema_name: str, table_name: str) -> str:
    """Escapes and combines a schema and table name."""
    return (
        escape_postgres_identifier(schema_name)
        + "."
        + escape_postgres_identifier(table_name)
    )


def _get_pk(
    credentials: ConnectionStringCredentials,
    schema_name: str,
    table_name: str,
) -> Optional[TColumnNames]:
    """Returns primary key column(s) for postgres table.

    Returns None if no primary key columns exist.
    """
    qual_name = _make_qualified_table_name(schema_name, table_name)

    with _get_conn(credentials) as conn:
        with conn.cursor() as cur:
            # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
            cur.execute(
                f"""
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = '{qual_name}'::regclass
                AND    i.indisprimary;
            """
            )
            result = [tup[0] for tup in cur.fetchall()]
            if len(result) == 0:
                return None
            elif len(result) == 1:
                return result[0]
            return result


def get_publication_tables(
    publication_name: str,
    cur: cursor,
) -> List[str]:
    """Returns list of tables in a publication.

    Args:
        publication_name: Name of the publication
        cur: Database cursor

    Returns:
        List of table names in the publication, or None if the publication doesn't exist
    """
    # First check if publication exists
    cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication_name,))
    if cur.fetchone() is None:
        raise PublicationNotFoundError(
            f"Publication {publication_name} does not exist."
        )

    query = """
        SELECT schemaname, tablename
        FROM pg_publication_tables
        WHERE pubname = %s
        ORDER BY schemaname, tablename;
    """
    cur.execute(query, (publication_name,))
    result = cur.fetchall()
    return [f"{row[0]}.{row[1]}" for row in result]


@dlt.resource(
    name=lambda args: args["slot_name"],
    standalone=True,
)
def replication_resource(
    credentials: ConnectionStringCredentials,
    schema_name: str,
    publication_name: str,
    slot_name: str,
    tables: List[str],
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    target_batch_size: int = 1000,
    write_mode: Literal["merge", "append-only"] = "merge",
    flush_slot: bool = True,
) -> Iterable[Union[TDataItem, DataItemWithMeta]]:
    """Resource yielding data items for changes in one or more postgres tables.

    - Relies on a replication slot and publication that publishes DML operations
    (i.e. `insert`, `update`, and/or `delete`). Helper `init_replication` can be
    used to set this up. The publication can be automatically created if `manage_publication`
    is set to True in the main `pg_replication` function.
    - Maintains LSN of last consumed message in state to track progress.
    - At start of the run, advances the slot upto last consumed message in previous run.
    - Processes in batches to limit memory usage.

    Args:
        slot_name (str): Name of the replication slot to consume replication messages from.
        publication_name (str): Name of the publication that publishes DML operations for the table(s).
        schema_name (str): Name of the schema to filter tables from.
        credentials (ConnectionStringCredentials): Postgres database credentials.
        tables (List[str]): List of table names to include in the replication resource.
        include_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
            sequence of names of columns to include in the generated data items.
            Any column not in the sequence is excluded. If not provided, all columns
            are included. For example:
            ```
            include_columns={
                "table_x": ["col_a", "col_c"],
                "table_y": ["col_x", "col_y", "col_z"],
            }
            ```
        columns (Optional[Dict[str, TTableHintTemplate[TAnySchemaColumns]]]): Maps
            table name(s) to column hints to apply on the replicated table(s). For example:
            ```
            columns={
                "table_x": {"col_a": {"data_type": "json"}},
                "table_y": {"col_y": {"precision": 32}},
            }
            ```
        target_batch_size (int): Desired number of data items yielded in a batch.
            Can be used to limit the data items in memory. Note that the number of
            data items yielded can be (far) greater than `target_batch_size`, because
            all messages belonging to the same transaction are always processed in
            the same batch, regardless of the number of messages in the transaction
            and regardless of the value of `target_batch_size`. The number of data
            items can also be smaller than `target_batch_size` when the replication
            slot is exhausted before a batch is full.
        write_mode (Literal["merge", "append-only"]): Write mode for data processing.
            - "merge": Default mode. Consolidates changes with existing data, creating final tables
            that are replicas of source tables. No historical record of change events is kept.
            - "append-only": Adds data as a stream of changes (INSERT, UPDATE-INSERT, UPDATE-DELETE,
            DELETE events). Retains historical state of data with all change events preserved.
        flush_slot (bool): Whether processed messages are discarded from the replication
            slot. Recommended value is True. Be careful when setting False—not flushing
            can eventually lead to a "disk full" condition on the server, because
            the server retains all the WAL segments that might be needed to stream
            the changes via all of the currently open replication slots.

        Yields:
            Data items for changes published in the publication.
    """
    # start where we left off in previous run
    resource_state = dlt.current.resource_state()
    start_lsn = resource_state.get("last_commit_lsn", 0)
    if flush_slot and start_lsn:
        advance_slot(start_lsn, slot_name, credentials)

    # continue until last message in replication slot
    options = {"publication_names": publication_name, "proto_version": "1"}
    end_lsn = get_max_lsn(slot_name, options, credentials)
    if end_lsn is None:
        return
    logger.info(
        f"Replicating slot {slot_name} publication {publication_name} from {start_lsn} to {end_lsn}"
    )

    # generate items in batches
    while True:
        gen = ItemGenerator(
            credentials=credentials,
            slot_name=slot_name,
            options=options,
            end_lsn=end_lsn,
            start_lsn=start_lsn,
            target_batch_size=target_batch_size,
            include_columns=include_columns,
            columns=columns,
            write_mode=write_mode,
            schema_name=schema_name,
            tables=tables,
        )
        yield from gen
        if gen.generated_all:
            resource_state["last_commit_lsn"] = gen.last_commit_lsn
            break
        start_lsn = gen.last_commit_lsn


def check_schema_exists(
    schema_name: str, credentials: ConnectionStringCredentials
) -> bool:
    """Check if a schema exists in the database.

    Args:
        schema_name: Name of the schema to check
        credentials: Database credentials

    Returns:
        bool: True if schema exists, False otherwise
    """
    try:
        with _get_conn(credentials) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 1
                    FROM information_schema.schemata
                    WHERE schema_name = %s
                """
                cur.execute(query, (schema_name,))
                return bool(cur.fetchone())
    except Exception as e:
        logger.error(f"Failed to check if schema {schema_name} exists: {e}")
        raise


def discover_schema_tables(
    credentials: ConnectionStringCredentials,
    schema_name: str,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
) -> List[str]:
    """Discover tables in a schema with optional filtering.

    Args:
        schema_name: Name of the schema
        credentials: Database credentials
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
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
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
        raise


def filter_tables(
    table_names: List[str],
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
) -> List[str]:
    """Filter table names using include/exclude patterns.

    Args:
        table_names: List of table names to filter
        include_tables: Glob patterns for tables to include
        exclude_tables: Glob patterns for tables to exclude

    Returns:
        Filtered list of table names
    """
    if include_tables is None and exclude_tables is None:
        return table_names

    # Convert single patterns to lists
    if isinstance(include_tables, str):
        include_tables = [include_tables]
    if isinstance(exclude_tables, str):
        exclude_tables = [exclude_tables]

    filtered_tables = table_names.copy()

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

    logger.info(f"Filtered {len(table_names)} tables to {len(filtered_tables)} tables")

    if not filtered_tables:
        logger.warning("No tables matched the include/exclude patterns")
        return []

    return filtered_tables


@dataclass
class ItemGenerator:
    credentials: ConnectionStringCredentials
    slot_name: str
    tables: List[str]
    options: Dict[str, str]
    end_lsn: int
    start_lsn: int = 0
    target_batch_size: int = 1000
    include_columns: Optional[Dict[str, Sequence[str]]] = None
    columns: Optional[Dict[str, TTableSchemaColumns]] = None
    write_mode: Literal["merge", "append-only"] = "merge"
    schema_name: str = "public"
    last_commit_lsn: Optional[int] = field(default=None, init=False)
    generated_all: bool = False

    def __iter__(self) -> Iterator[Union[TDataItem, DataItemWithMeta]]:
        """Yields replication messages from MessageConsumer.

        Starts replication of messages published by the publication from the replication slot.
        Maintains LSN of last consumed Commit message in object state.
        Does not advance the slot.
        """
        try:
            cur = _get_rep_conn(self.credentials).cursor()
            cur.start_replication(
                slot_name=self.slot_name,
                start_lsn=self.start_lsn,
                decode=False,
                options=self.options,
            )
            consumer = MessageConsumer(
                end_lsn=self.end_lsn,
                target_batch_size=self.target_batch_size,
                include_columns=self.include_columns,
                columns=self.columns,
                write_mode=self.write_mode,
                credentials=self.credentials,
                schema_name=self.schema_name,
                tables=self.tables,
            )
            cur.consume_stream(consumer)
        except StopReplication:  # completed batch or reached `end_lsn`
            pass
        finally:
            cur.connection.close()

        # yield data items
        for rel_id, data_items in consumer.data_items.items():
            table_name = consumer.last_table_schema[rel_id]["name"]
            yield data_items[0]  # meta item with column hints only, no data
            yield dlt.mark.with_table_name(data_items[1:], table_name)

        # update state
        self.last_commit_lsn = consumer.last_commit_lsn
        self.generated_all = consumer.consumed_all


class MessageConsumer:
    """Consumes messages from a ReplicationCursor sequentially.

    Generates data item for each `insert`, `update`, and `delete` message.
    Processes in batches to limit memory usage.
    Maintains message data needed by subsequent messages in internal state.
    """

    def __init__(
        self,
        tables: List[str],
        end_lsn: int,
        target_batch_size: int = 1000,
        include_columns: Optional[Dict[str, Sequence[str]]] = None,
        columns: Optional[Dict[str, TTableSchemaColumns]] = None,
        write_mode: Literal["merge", "append-only"] = "merge",
        credentials: Optional[ConnectionStringCredentials] = None,
        schema_name: str = "public",
    ) -> None:
        self.end_lsn = end_lsn
        self.target_batch_size = target_batch_size
        self.include_columns = include_columns
        self.columns = columns
        self.write_mode = write_mode
        self.credentials = credentials
        self.schema_name = schema_name
        self.tables = tables

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
        self.last_commit_ts: pendulum.DateTime
        self.last_commit_lsn = None

    def __call__(self, msg: ReplicationMessage) -> None:
        """Processes message received from stream."""
        self.process_msg(msg)

    def process_msg(self, msg: ReplicationMessage) -> None:
        """Processes encoded replication message.

        Identifies message type and decodes accordingly.
        Message treatment is different for various message types.
        Breaks out of stream with StopReplication exception when
        - `end_lsn` is reached
        - `target_batch_size` is reached
        - a table's schema has changed
        """
        op = msg.payload[0]
        if op == 73:  # ASCII for 'I'
            self.process_change(Insert(msg.payload), msg.data_start)
        elif op == 85:  # ASCII for 'U'
            self.process_change(Update(msg.payload), msg.data_start)
        elif op == 68:  # ASCII for 'D'
            self.process_change(Delete(msg.payload), msg.data_start)
        elif op == 66:  # ASCII for 'B'
            self.last_commit_ts = Begin(msg.payload).commit_ts  # type: ignore[assignment]
        elif op == 67:  # ASCII for 'C'
            self.process_commit(msg)
        elif op == 82:  # ASCII for 'R'
            self.process_relation(Relation(msg.payload))
        elif op == 84:  # ASCII for 'T'
            logger.warning(
                "The truncate operation is currently not supported. "
                "Truncate replication messages are ignored."
            )
        else:
            raise ValueError(f"Unknown replication op {op}")

    def process_commit(self, msg: ReplicationMessage) -> None:
        """Updates object state when Commit message is observed.

        Raises StopReplication when `end_lsn` or `target_batch_size` is reached.
        """
        self.last_commit_lsn = msg.data_start
        if msg.data_start >= self.end_lsn:
            self.consumed_all = True
        n_items = sum(
            [len(items) for items in self.data_items.values()]
        )  # combine items for all tables
        if self.consumed_all or n_items >= self.target_batch_size:
            raise StopReplication

    def _get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table, using cache if available."""
        cache_key = (schema_name, table_name)
        if cache_key not in self._pk_cache and self.credentials:
            pk = _get_pk(self.credentials, schema_name, table_name)
            self._pk_cache[cache_key] = [pk] if isinstance(pk, str) else (pk or [])
        return self._pk_cache.get(cache_key, [])

    def _is_schema_changed(self, decoded_msg: Relation) -> bool:
        """Check if the schema of the table has changed.

        Args:
            decoded_msg: The binlog event containing row data

        Returns:
            bool: True if the schema has changed for tracked columns, False otherwise
        """

        return decoded_msg.relation_id in self.data_items

    def process_relation(self, decoded_msg: Relation) -> None:
        """Processes a replication message of type Relation.

        Stores table schema in object state.
        Creates meta item to emit column hints while yielding data.

        Raises StopReplication when a table's schema changes.
        """
        # get table schema information from source and store in object state
        schema_name = decoded_msg.namespace or "public"
        table_name = decoded_msg.relation_name

        # raise StopReplication if table schema has changed
        if self._is_schema_changed(decoded_msg):
            raise StopReplication(
                f"Table {schema_name}.{table_name} has changed schema."
            )

        columns: TTableSchemaColumns = {
            c.name: _to_dlt_column_schema(c) for c in decoded_msg.columns
        }

        self.last_table_schema[decoded_msg.relation_id] = {
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
        self.data_items[decoded_msg.relation_id] = [meta_item]

    def process_change(
        self, decoded_msg: Union[Insert, Update, Delete], msg_start_lsn: int
    ) -> None:
        """Processes replication message of type Insert, Update, or Delete.

        Adds data item for inserted/updated/deleted record to instance attribute.
        """
        if isinstance(decoded_msg, (Insert, Update)):
            column_data = decoded_msg.new_tuple.column_data
        elif isinstance(decoded_msg, Delete):
            column_data = decoded_msg.old_tuple.column_data
        table_name = self.last_table_schema[decoded_msg.relation_id]["name"]
        data_item = self.gen_data_item(
            data=column_data,
            column_schema=self.last_table_schema[decoded_msg.relation_id]["columns"],
            lsn=msg_start_lsn,
            commit_ts=self.last_commit_ts,
            for_delete=isinstance(decoded_msg, Delete),
            include_columns=(
                None
                if self.include_columns is None
                else self.include_columns.get(table_name)
            ),
        )
        self.data_items[decoded_msg.relation_id].append(data_item)

    @staticmethod
    def gen_data_item(
        data: List[ColumnData],
        column_schema: TTableSchemaColumns,
        lsn: int,
        commit_ts: pendulum.DateTime,
        for_delete: bool,
        include_columns: Optional[Sequence[str]] = None,
    ) -> TDataItem:
        """Generates data item from replication message data and corresponding metadata."""
        data_item = {
            schema["name"]: _to_dlt_val(
                val=data.col_data,
                data_type=schema["data_type"],
                byte1=data.col_data_category,
                for_delete=for_delete,
            )
            for (schema, data) in zip(column_schema.values(), data)
            if (True if include_columns is None else schema["name"] in include_columns)
        }
        data_item["_dlt_lsn"] = lsn
        if for_delete:
            data_item["_dlt_deleted_ts"] = commit_ts
        return data_item
