"""Type conversion utilities for MySQL replication."""

import codecs
import datetime
import json
from functools import lru_cache
from typing import Any, Dict

import pendulum
from dlt.common import Decimal, logger
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import TColumnSchema, TColumnType
from plpygis import Geometry
from pymysqlreplication.column import Column
from pymysqlreplication.constants import FIELD_TYPE

_DUMMY_VALS: Dict[TDataType, Any] = {
    "bigint": 0,
    "binary": b" ",
    "bool": True,
    "json": [0],
    "date": "2000-01-01",
    "decimal": Decimal(0),
    "double": 0.0,
    "text": "",
    "time": "00:00:00",
    "timestamp": "2000-01-01T00:00:00",
    "wei": 0,
}
"""Dummy values used to replace NULLs in NOT NULL columns in key-only delete records."""


@lru_cache(maxsize=None)
def _type_mapper(type_id: int) -> TColumnType:
    """Map MySQL type ID to dlt column type.

    Args:
        type_id: MySQL field type ID from pymysqlreplication.constants.FIELD_TYPE

    Returns:
        TColumnType: The corresponding dlt column type
    """
    # Integer types
    if type_id in {
        FIELD_TYPE.TINY,
        FIELD_TYPE.SHORT,
        FIELD_TYPE.INT24,
        FIELD_TYPE.LONG,
        FIELD_TYPE.LONGLONG,
        FIELD_TYPE.YEAR,
    }:
        return {"data_type": "bigint"}

    # Floating point types
    if type_id in {FIELD_TYPE.FLOAT, FIELD_TYPE.DOUBLE}:
        return {"data_type": "double"}

    # Decimal types
    if type_id in {FIELD_TYPE.DECIMAL, FIELD_TYPE.NEWDECIMAL}:
        return {"data_type": "decimal", "precision": 36, "scale": 2}

    # Date/Time types
    if type_id in {
        FIELD_TYPE.DATETIME,
        FIELD_TYPE.DATETIME2,
        FIELD_TYPE.TIMESTAMP,
        FIELD_TYPE.TIMESTAMP2,
    }:
        return {"data_type": "timestamp", "precision": 6}

    if type_id == FIELD_TYPE.DATE:
        return {"data_type": "date"}

    if type_id in {FIELD_TYPE.TIME, FIELD_TYPE.TIME2}:
        return {"data_type": "time", "precision": 6}

    # String types
    if type_id in {
        FIELD_TYPE.STRING,
        FIELD_TYPE.VAR_STRING,
        FIELD_TYPE.VARCHAR,
        FIELD_TYPE.CHAR,
        FIELD_TYPE.ENUM,
        FIELD_TYPE.SET,
    }:
        return {"data_type": "text"}

    # Binary types
    if type_id in {
        FIELD_TYPE.TINY_BLOB,
        FIELD_TYPE.MEDIUM_BLOB,
        FIELD_TYPE.LONG_BLOB,
        FIELD_TYPE.BLOB,
        FIELD_TYPE.BIT,
    }:
        return {"data_type": "binary"}

    # Special types
    if type_id == FIELD_TYPE.JSON:
        return {"data_type": "json"}

    if type_id == FIELD_TYPE.GEOMETRY:
        return {"data_type": "geometry"}

    # Default to text for unknown types
    logger.warning(
        f"MySQL type ID {type_id} is not explicitly handled, mapping to 'text'"
    )
    return {"data_type": "text"}


def _to_dlt_column_type(type_id: int) -> TColumnType:
    """Converts mysql type id to dlt column type.

    If the type is unknown, it will be mapped to "text" type.
    """
    return _type_mapper(type_id)


def _to_dlt_column_schema(col: Column) -> TColumnSchema:
    """Converts MySQL BinLog Column to dlt column schema."""
    dlt_column_type = _to_dlt_column_type(col.type)
    return {**dlt_column_type, "name": col.name}


def _to_dlt_val(val: Any, type_id: int, data_type: TDataType, for_delete: bool) -> Any:
    """Converts mysql binlog's text-formatted value into dlt-compatible data value."""
    print(
        f"DEBUG: val={val}, type_id={type_id}, data_type={data_type}, for_delete={for_delete}"
    )

    if val is None:
        if for_delete:
            # replace None with dummy value to prevent NOT NULL violations in staging table
            return _DUMMY_VALS[data_type]
        return None

    if isinstance(val, datetime.datetime):
        if type_id in (FIELD_TYPE.TIMESTAMP, FIELD_TYPE.TIMESTAMP2):
            # The mysql-replication library creates datetimes from TIMESTAMP columns using fromtimestamp which
            # will use the local timezone. We need to convert it to UTC.
            # https://github.com/noplay/python-mysql-replication/blob/master/pymysqlreplication/row_event.py#L143-L145
            dt = pendulum.instance(val, "local").in_tz("UTC")
            return dt.isoformat()
        else:
            # For non-timestamp datetime types, ensure they're in UTC
            dt = pendulum.instance(val).in_tz("UTC")
            return dt.isoformat()

    elif isinstance(val, datetime.date):
        # Convert date to datetime at midnight UTC
        dt = pendulum.datetime(val.year, val.month, val.day, tz="UTC")
        return dt.isoformat()

    elif isinstance(val, datetime.timedelta):
        if type_id in (FIELD_TYPE.TIME, FIELD_TYPE.TIME2):
            # this should convert time column into 'HH:MM:SS' formatted string
            return str(val)
        else:
            # Convert timedelta to datetime since epoch
            epoch = pendulum.datetime(1970, 1, 1, tz="UTC")
            dt = epoch + val
            return dt.isoformat()

    elif type_id == FIELD_TYPE.JSON:
        return json.dumps(_json_bytes_to_string(val))

    elif type_id == FIELD_TYPE.GEOMETRY:
        if val:
            srid = int.from_bytes(val[:4], byteorder="little")
            geom = Geometry(val[4:], srid=srid)
            return json.dumps(geom.geojson)
        else:
            return None

    elif isinstance(val, bytes):
        # encode bytes as hex bytes then to utf8 string
        return codecs.encode(val, "hex").decode("utf-8")

    elif type_id == FIELD_TYPE.BIT:
        return int(val) != 0

    return val


def _json_bytes_to_string(data):
    if isinstance(data, bytes):
        return data.decode()

    if isinstance(data, dict):
        return dict(map(_json_bytes_to_string, data.items()))

    if isinstance(data, tuple):
        return tuple(map(_json_bytes_to_string, data))

    if isinstance(data, list):
        return list(map(_json_bytes_to_string, data))

    return data
