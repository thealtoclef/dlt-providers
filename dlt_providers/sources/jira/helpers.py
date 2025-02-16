from typing import Iterable, Optional

from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources.helpers import requests


def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str = "rest/api/2/search",
    data_path: Optional[str] = None,
    params: Optional[DictStrAny] = None,
) -> Iterable[TDataItem]:
    """
    Function to fetch paginated data from a Jira API endpoint.

    Args:
        subdomain: The subdomain for the Jira instance.
        email: The email to authenticate with.
        api_token: The API token to authenticate with.
        page_size: Maximum number of results per page
        api_path: The API path for the Jira endpoint.
        data_path: Optional data path to extract from the response.
        params: Optional parameters for the API request.
    Yields:
        Iterable[TDataItem]: Yields pages of data from the API.
    """
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json"}
    auth = (email, api_token)
    params = {} if params is None else params
    params["startAt"] = start_at = 0
    params["maxResults"] = page_size

    while True:
        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()

        if data_path:
            results_page = result.pop(data_path)
        else:
            results_page = result

        if len(results_page) == 0:
            break

        yield results_page

        # continue from next page
        start_at += len(results_page)
        params["startAt"] = start_at
