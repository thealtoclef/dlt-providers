"""This source uses Jira API and dlt to load data such as Issues, Users to the database."""

from typing import Iterable

import dlt
import pendulum
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import get_paginated_data
from .settings import DEFAULT_PAGE_SIZE


@dlt.source(max_table_nesting=0)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    page_size: int = DEFAULT_PAGE_SIZE,
    start_date: str | None = None,
) -> Iterable[DltResource]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of results per page
    Returns:
        Iterable[DltResource]: List of resource functions.
    """

    @dlt.resource(
        primary_key="id",
        write_disposition="merge",
        max_table_nesting=1,
    )
    def issues(
        updated=dlt.sources.incremental(
            cursor_path="fields.updated", initial_value=start_date, last_value_func=max
        ),
    ) -> Iterable[TDataItem]:
        conditions = []
        if updated.start_value:
            conditions.append(
                f"updated >= '{pendulum.parse(updated.start_value).format('YYYY-MM-DD HH:mm')}'"
            )
        if updated.end_value:
            conditions.append(
                f"updated < '{pendulum.parse(updated.end_value).format('YYYY-MM-DD HH:mm')}'"
            )
        jql = " AND ".join(conditions) + " ORDER BY created ASC"
        params = {
            "fields": "*all",
            "expand": "fields,renderedFields,changelog,operations,transitions,names",
            "validateQuery": "strict",
            "jql": jql,
        }

        yield get_paginated_data(
            api_path="rest/api/3/search",
            params=params,
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size,
            data_path="issues",
        )

    @dlt.resource(
        primary_key="account_id",
        write_disposition="replace",
    )
    def users() -> Iterable[TDataItem]:
        params = {"includeInactiveUsers": True}

        return get_paginated_data(
            api_path="rest/api/3/users",
            params=params,
            subdomain=subdomain,
            email=email,
            api_token=api_token,
            page_size=page_size,
        )

    return [users, issues]
