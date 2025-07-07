from typing import Dict, List, Literal, Optional, Sequence, Union

import dlt
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract import DltSource
from dlt.sources.credentials import ConnectionStringCredentials

from .helpers import create_snapshot_resources, replication_resource, source_setup


@dlt.source
def mysql_replication(
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    schema_name: str = dlt.config.value,
    server_id: int = dlt.config.value,
    include_tables: Optional[Union[str, List[str]]] = None,
    exclude_tables: Optional[Union[str, List[str]]] = None,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    reset: bool = False,
    initial_snapshots: bool = True,
    target_batch_size: int = 1000,
    write_mode: Literal["merge", "append-only"] = "merge",
) -> DltSource:
    """MySQL replication source with initial snapshots and ongoing replication.

    This unified source combines initial table snapshots (using connectorx for read-only access)
    with ongoing binary log replication using GTID or binlog coordinates. It automatically chooses
    the appropriate replication method and sets up replication streams.

    Args:
        credentials (ConnectionStringCredentials): MySQL database credentials.
        schema_name (str): MySQL database/schema name.
        server_id (int): Unique server ID for MySQL replication connection.
        include_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to include.
          If not provided, all tables in the schema are included.
        exclude_tables (Optional[Union[str, List[str]]]): Glob patterns for tables to exclude.
          These patterns are applied after include_tables.
        include_columns (Optional[Dict[str, Sequence[str]]]): Maps table name(s) to
          sequence of names of columns to include in the snapshot table(s).
          Any column not in the sequence is excluded. If not provided, all columns
          are included.
        columns (Optional[Dict[str, TTableSchemaColumns]]): Maps
          table name(s) to column hints to apply on the snapshot table resource(s).
        reset (bool): If set to True, the existing replication state is reset
          and replication starts from the beginning. Has no effect if no previous
          state exists.
        initial_snapshots (bool): Whether to create read-only initial snapshot resources
          using connectorx backend.
        target_batch_size (int): Desired number of data items yielded in a batch for replication.
        write_mode (Literal["merge", "append-only"]): Write mode for data processing.
            - "merge": Default mode. Consolidates changes with existing data, creating final tables
              that are replicas of source tables. No historical record of change events is kept.
            - "append-only": Adds data as a stream of changes (INSERT, UPDATE-INSERT, UPDATE-DELETE,
              DELETE events). Retains historical state of data with all change events preserved.

    Returns:
        DltSource: A dlt source containing both snapshot and replication resources.
    """
    resources = []

    # Initialize replication and get replication info
    setup_info = source_setup(
        credentials=credentials,
        schema_name=schema_name,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
    )
    resources.extend(setup_info.get("setup_resources", []))

    # Create snapshot resources if requested
    if initial_snapshots:
        snapshot_resources = create_snapshot_resources(
            credentials=credentials,
            schema_name=schema_name,
            tables=setup_info["tables"],
            include_columns=include_columns,
            columns=columns,
            reset=reset,
        )
        if isinstance(snapshot_resources, list):
            resources.extend(snapshot_resources)
        else:
            resources.append(snapshot_resources)

    # Create replication resource
    replication_res = replication_resource(
        credentials=credentials,
        schema_name=schema_name,
        server_id=server_id,
        tables=setup_info["tables"],
        include_columns=include_columns,
        columns=columns,
        target_batch_size=target_batch_size,
        write_mode=write_mode,
    )
    resources.append(replication_res)

    return resources
