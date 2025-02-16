"""Source that load github issues, pull requests and reactions for a specific repository via customizable graphql query. Loads events incrementally."""

from typing import Generator

import dlt
from dlt.sources import DltResource
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from .settings import REST_API_BASE_URL


@dlt.source(max_table_nesting=2)
def github(
    org: str,
    access_token=dlt.secrets.value,
    start_date: str | None = "1970-01-01",
) -> Generator[DltResource, None, None]:
    config: RESTAPIConfig = {
        "client": {
            "base_url": REST_API_BASE_URL,
            "auth": {
                "token": access_token,
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        "resources": [
            {
                "name": "repos",
                "endpoint": {
                    "path": f"/orgs/{org}/repos",
                },
            },
            {
                "name": "workflow_runs",
                "write_disposition": "append",
                "endpoint": {
                    "path": "/repos/{owner}/{repo}/actions/runs",
                    "data_selector": "workflow_runs",
                    "incremental": {
                        "start_param": "created",
                        "cursor_path": "created_at",
                        "initial_value": start_date,
                        "convert": lambda created_at: f">={created_at}",
                    },
                    "params": {
                        "owner": {
                            "type": "resolve",
                            "resource": "repos",
                            "field": "owner.login",
                        },
                        "repo": {
                            "type": "resolve",
                            "resource": "repos",
                            "field": "name",
                        },
                    },
                },
            },
            {
                "name": "events",
                "endpoint": {
                    "path": "/repos/{owner}/{repo}/events",
                    "params": {
                        "owner": {
                            "type": "resolve",
                            "resource": "repos",
                            "field": "owner.login",
                        },
                        "repo": {
                            "type": "resolve",
                            "resource": "repos",
                            "field": "name",
                        },
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


# @dlt.source(max_table_nesting=2)
# def github_repo_events(
#     owner: str,
#     name: str,
#     access_token: Optional[str] = None,
#     start_date: str = "1970-01-01",
# ) -> DltResource:
#     """Gets events for repository `name` with owner `owner` incrementally.

#     This source contains a single resource `repo_events` that gets given repository's events and dispatches them to separate tables with names based on event type.
#     The data is loaded incrementally. Subsequent runs will get only new events and append them to tables.
#     Please note that Github allows only for 300 events to be retrieved for public repositories. You should get the events frequently for the active repos.

#     Args:
#         owner (str): The repository owner
#         name (str): The repository name
#         access_token (str): The classic or fine-grained access token. If not provided, calls are made anonymously

#     Returns:
#         DltSource: source with the `repo_events` resource

#     """

#     # use naming function in table name to generate separate tables for each event
#     @dlt.resource(primary_key="id", table_name=lambda i: i["type"])
#     def repo_events(
#         last_created_at: dlt.sources.incremental[str] = dlt.sources.incremental(
#             cursor_path="created_at",
#             initial_value=start_date,
#             last_value_func=max,
#         ),
#     ) -> Iterator[TDataItems]:
#         repos_path = (
#             f"/repos/{urllib.parse.quote(owner)}/{urllib.parse.quote(name)}/events"
#         )

#         for page in get_rest_pages(access_token, repos_path + "?per_page=100"):
#             yield page

#             # stop requesting pages if the last element was already older than initial value
#             # note: incremental will skip those items anyway, we just do not want to use the api limits
#             if last_created_at.start_out_of_range:
#                 print(
#                     f"Overlap with previous run created at {last_created_at.initial_value}"
#                 )
#                 break

#     return repo_events
