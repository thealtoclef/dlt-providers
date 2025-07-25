from typing import Dict, List, Literal, Optional, Union

import dlt
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract import DltSource
from dlt.sources.credentials import ConnectionStringCredentials

from .helpers import create_snapshot_resources, replication_resource, source_setup


@dlt.source
def pg_replication(
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    schema_name: str = dlt.config.value,
    publication_name: str = dlt.config.value,
    slot_name: str = dlt.config.value,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    reset: bool = False,
    initial_snapshots: bool = True,
    batch_size: int = 1000,
    write_mode: Literal["merge", "append-only"] = "merge",
    manage_publication: bool = True,
    flush_slot: bool = True,
) -> DltSource:
    """PostgreSQL replication source with initial snapshots and ongoing replication.

    This unified source combines initial table snapshots (using connectorx for read-only access)
    with ongoing logical replication. It automatically sets up replication slots and publications.

    Args:
        credentials (ConnectionStringCredentials): Postgres database credentials.
        schema_name (str): Postgres schema name.
        publication_name (str): Name of the publication to be used or created.
        slot_name (str): Name of the replication slot to be created.
        include_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to include.
          If not provided, all tables in the schema are included.
        exclude_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to exclude.
          These patterns are applied after include_tables.
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the snapshot table resource(s).
        reset (bool): If set to True, the existing slot and publication are dropped
          and recreated. Has no effect if a slot and publication with the provided
          names do not yet exist.
        initial_snapshots (bool): Whether to create read-only initial snapshot resources
          using connectorx backend.
        batch_size (int): Desired number of data items yielded in a batch.
        write_mode (Literal["merge", "append-only"]): Write mode for data processing.
            - "merge": Default mode. Consolidates changes with existing data, creating final tables
              that are replicas of source tables. No historical record of change events is kept.
            - "append-only": Adds data as a stream of changes (INSERT, UPDATE-INSERT, UPDATE-DELETE,
              DELETE events). Retains historical state of data with all change events preserved.
        manage_publication (bool): If True (default), automatically creates the publication if it doesn't exist.
          If False, uses the publication as-is without checking or creating it (will fail if publication doesn't exist).
        flush_slot (bool): Whether processed messages are discarded from the replication slot.

    Returns:
        DltSource: A dlt source containing both snapshot and replication resources.
    """
    resources = []

    # Initialize replication and get replication info
    setup_info = source_setup(
        credentials=credentials,
        schema_name=schema_name,
        publication_name=publication_name,
        slot_name=slot_name,
        reset=reset,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        manage_publication=manage_publication,
    )
    resources.extend(setup_info.get("setup_resources", []))

    # Create snapshot resources if requested
    if initial_snapshots:
        snapshot_resources = create_snapshot_resources(
            credentials=credentials,
            schema_name=schema_name,
            tables=setup_info["tables"],
            columns=columns,
            batch_size=batch_size,
        )
        if isinstance(snapshot_resources, list):
            resources.extend(snapshot_resources)
        else:
            resources.append(snapshot_resources)

    # Create replication resource
    replication_res = replication_resource(
        credentials=credentials,
        schema_name=schema_name,
        publication_name=publication_name,
        slot_name=slot_name,
        tables=setup_info["tables"],
        columns=columns,
        batch_size=batch_size,
        write_mode=write_mode,
        flush_slot=flush_slot,
    )
    resources.append(replication_res)

    return resources
