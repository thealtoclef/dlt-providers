from typing import Dict, List, Literal, Optional, Sequence, Union

import dlt
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract import DltSource
from dlt.sources.credentials import ConnectionStringCredentials

from .helpers import init_replication, replication_resource


@dlt.source
def pg_replication(
    slot_name: str = dlt.config.value,
    publication_name: str = dlt.config.value,
    schema_name: str = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    reset: bool = False,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
    initial_snapshots: bool = True,
    target_batch_size: int = 1000,
    write_mode: Literal["merge", "append-only"] = "merge",
    flush_slot: bool = True,
    manage_publication: bool = True,
) -> DltSource:
    """PostgreSQL replication source with initial snapshots and ongoing replication.

    This unified source combines initial table snapshots (using connectorx for read-only access)
    with ongoing logical replication. It automatically sets up replication slots and publications.

    Args:
        slot_name (str): Name of the replication slot to be created.
        publication_name (str): Name of the publication to be used or created.
        schema_name (str): Postgres schema name.
        credentials (ConnectionStringCredentials): Postgres database credentials.
        include_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to include in the snapshot table(s).
          Any column not in the sequence is excluded. If not provided, all columns
          are included.
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the snapshot table resource(s).
        reset (bool): If set to True, the existing slot and publication are dropped
          and recreated. Has no effect if a slot and publication with the provided
          names do not yet exist.
        include_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to include.
          If not provided, all tables in the schema are included.
        exclude_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to exclude.
          These patterns are applied after include_tables.
        initial_snapshots (bool): Whether to create read-only initial snapshot resources
          using connectorx backend.
        target_batch_size (int): Desired number of data items yielded in a batch for replication.
        write_mode (Literal["merge", "append-only"]): Write mode for data processing.
            - "merge": Default mode. Consolidates changes with existing data, creating final tables
              that are replicas of source tables. No historical record of change events is kept.
            - "append-only": Adds data as a stream of changes (INSERT, UPDATE-INSERT, UPDATE-DELETE,
              DELETE events). Retains historical state of data with all change events preserved.
        flush_slot (bool): Whether processed messages are discarded from the replication slot.
        manage_publication (bool): If True (default), automatically creates the publication if it doesn't exist.
          If False, uses the publication as-is without checking or creating it (will fail if publication doesn't exist).

    Returns:
        DltSource: A dlt source containing both snapshot and replication resources.

    Example:
        ```python
        import dlt
        from dlt_providers.sources.pg_replication import pg_replication

        # Basic usage
        source = pg_replication(
            slot_name="my_slot",
            publication_name="my_publication",
            schema_name="public",
            credentials="postgresql://user:pass@localhost/db"
        )

        # With table filtering
        source = pg_replication(
            slot_name="my_slot",
            publication_name="my_publication",
            schema_name="public",
            include_tables=["users", "orders*"],
            exclude_tables=["*_temp"],
            credentials="postgresql://user:pass@localhost/db"
        )

        # Load the data
        pipeline = dlt.pipeline("pg_repl", destination="duckdb")
        pipeline.run(source)
        ```
    """
    # Initialize replication and get snapshot resources
    snapshot_resources = init_replication(
        slot_name=slot_name,
        publication_name=publication_name,
        schema_name=schema_name,
        manage_publication=manage_publication,
        credentials=credentials,
        include_columns=include_columns,
        columns=columns,
        reset=reset,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        initial_snapshots=initial_snapshots,
    )

    # Create replication resource
    replication_res = replication_resource(
        slot_name=slot_name,
        publication_name=publication_name,
        credentials=credentials,
        include_columns=include_columns,
        columns=columns,
        target_batch_size=target_batch_size,
        flush_slot=flush_slot,
        write_mode=write_mode,
    )

    # Combine resources
    resources = []
    if snapshot_resources:
        if isinstance(snapshot_resources, list):
            resources.extend(snapshot_resources)
        else:
            resources.append(snapshot_resources)
    resources.append(replication_res)

    return resources
