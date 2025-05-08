from typing import Generator

import dlt
import pendulum
from dlt.sources import DltResource
from dlt.sources.helpers.rest_client.paginators import (
    JSONLinkPaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
)
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from .settings import DEFAULT_START_DATE, REST_API_BASE_URL


@dlt.source(max_table_nesting=0)
def jira(
    subdomain: str = dlt.secrets.value,
    email: str = dlt.secrets.value,
    api_token: str = dlt.secrets.value,
    start_date: str = DEFAULT_START_DATE,
) -> Generator[DltResource, None, None]:
    """
    Jira source function that generates a list of resource functions based on endpoints.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of items to fetch per page.
        maximum_total_items: Maximum total items to fetch.
    Returns:
        Generator[DltResource, None, None]: A generator of DltResource objects.
    """

    config: RESTAPIConfig = {
        "client": {
            "base_url": REST_API_BASE_URL.format(subdomain=subdomain),
            "auth": {
                "type": "http_basic",
                "username": email,
                "password": api_token,
            },
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "issues",
                "max_table_nesting": 1,
                "write_disposition": "merge",
                "primary_key": "id",
                "endpoint": {
                    "path": "/rest/api/3/search/jql",
                    "paginator": JSONResponseCursorPaginator(
                        cursor_path="nextPageToken", cursor_param="nextPageToken"
                    ),
                    "data_selector": "issues",
                    "incremental": {
                        "cursor_path": "fields.updated",
                        "initial_value": start_date,
                        "convert": lambda start_value: f"updated >= '{pendulum.parse(start_value).format('YYYY-MM-DD HH:mm')}'",
                    },
                    "params": {
                        "jql": "{incremental.start_value}",
                        "fields": "*all",
                        "expand": "renderedFields",
                    },
                },
            },
            {
                "name": "issue_changelogs",
                "write_disposition": "merge",
                "primary_key": "id",
                "endpoint": {
                    "path": "/rest/api/3/issue/{issue_id}/changelog",
                    "paginator": OffsetPaginator(
                        limit=100,
                        offset_param="startAt",
                        limit_param="maxResults",
                        total_path="total",
                    ),
                    "data_selector": "values",
                    "params": {
                        "issue_id": {
                            "type": "resolve",
                            "resource": "issues",
                            "field": "id",
                        }
                    },
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "issue_fields",
                "endpoint": {
                    "path": "/rest/api/3/field/search",
                    "paginator": OffsetPaginator(
                        limit=50,
                        offset_param="startAt",
                        limit_param="maxResults",
                        total_path="total",
                    ),
                    "data_selector": "values",
                },
            },
            {
                "name": "statuses",
                "endpoint": {
                    "path": "/rest/api/3/statuses/search",
                    "paginator": JSONLinkPaginator(next_url_path="nextPage"),
                    "data_selector": "values",
                },
            },
            {
                "name": "users",
                "endpoint": {
                    "path": "/rest/api/3/users/search",
                    "paginator": OffsetPaginator(
                        limit=50,
                        offset_param="startAt",
                        limit_param="maxResults",
                        total_path=None,
                    ),
                },
            },
        ],
    }

    yield from rest_api_resources(config)
