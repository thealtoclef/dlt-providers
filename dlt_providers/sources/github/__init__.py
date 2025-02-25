import base64
from typing import Iterable, Literal, Optional

import dlt
from dlt.common import logger
from dlt.common.exceptions import DltException
from dlt.sources import DltResource
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

from .helpers import GitHubAppAuth
from .settings import (
    DEFAULT_START_DATE,
    REST_API_BASE_URL,
)


@dlt.source(max_table_nesting=2)
def github(
    org: str = dlt.secrets.value,
    auth_type: Literal["pat", "gha"] = "pat",
    access_token: Optional[str] = dlt.secrets.value,
    gha_installation_id: Optional[str] = dlt.secrets.value,
    gha_client_id: Optional[str] = dlt.secrets.value,
    gha_private_key: Optional[str] = dlt.secrets.value,
    gha_private_key_base64: Optional[str] = dlt.secrets.value,
    start_date: str = DEFAULT_START_DATE,
) -> Iterable[DltResource]:
    match auth_type:
        case "pat":
            auth = BearerTokenAuth(token=access_token)
        case "gha":
            # HACK: GitHubAppAuth does not require token, but we need to set it to empty string to avoid error
            dlt.secrets["sources.github.credentials.token"] = ""
            auth = GitHubAppAuth(
                client_id=gha_client_id,
                private_key=gha_private_key
                or base64.b64decode(gha_private_key_base64).decode("utf-8"),
                auth_endpoint=f"https://api.github.com/app/installations/{gha_installation_id}/access_tokens",
                scopes=[],  # HACK: GitHubAppAuth does not require scopes, but we need to set it to empty list to avoid error
            )
        case _:
            raise DltException(
                f"Invalid auth type {auth_type}. Must be one of ['pat', 'gha']"
            )

    client = RESTClient(
        base_url=REST_API_BASE_URL,
        auth=auth,
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,
        ),
    )

    @dlt.resource(write_disposition="merge", primary_key="id")
    def repositories():
        """Load GitHub repositories data.

        Yields:
            Paginated repository data for the organization
        """
        yield from client.paginate(
            path=f"/orgs/{org}/repos",
            params={"per_page": 100, "sort": "updated", "direction": "desc"},
        )

    @dlt.transformer(
        data_from=repositories, primary_key="sha", write_disposition="merge"
    )
    def commits(repositories):
        """Transform and load GitHub commits data with checkpointing.

        Args:
            repositories: Iterator of repository data from GitHub API

        Yields:
            Paginated commit data for each repository
        """
        checkpoints = dlt.current.resource_state().setdefault("checkpoints", {})

        for repository in repositories:
            repo_full_name = repository["full_name"]
            try:
                # Initialize checkpoint and tracking variables
                checkpoint = checkpoints.get(repo_full_name, start_date)
                latest_commit_date = None

                # Get paginated commits since last checkpoint
                pages = client.paginate(
                    path=f"/repos/{repo_full_name}/commits",
                    params={"per_page": 100, "since": checkpoint},
                )

                for page in pages:
                    if not page:
                        break

                    # Track most recent commit date from first page
                    if latest_commit_date is None:
                        latest_commit_date = page[0]["commit"]["committer"]["date"]
                    yield page

                # Update checkpoint after successful processing
                if latest_commit_date:
                    checkpoints[repo_full_name] = latest_commit_date
                    logger.info(
                        f"Updated commits checkpoint for {repo_full_name} to {latest_commit_date}"
                    )

            except Exception as e:
                logger.error(
                    f"Failed to process commits for {repo_full_name}: {str(e)}"
                )
                continue

    @dlt.transformer(
        data_from=repositories, primary_key="id", write_disposition="merge"
    )
    def workflow_runs(repositories):
        """Transform and load GitHub workflow runs data with checkpointing.
        Handles GitHub's 1000 results per query limit by fetching all pages
        before updating the time window.

        Args:
            repositories: Iterator of repository data from GitHub API

        Yields:
            Paginated workflow runs data for each repository
        """
        checkpoints = dlt.current.resource_state().setdefault("checkpoints", {})

        for repository in repositories:
            repo_full_name = repository["full_name"]
            try:
                # Initialize checkpoint and tracking variables
                checkpoint = checkpoints.get(repo_full_name, start_date)
                cursor = "*"
                latest_run_date = None

                while True:
                    in_limit = True
                    oldest_run_date = None

                    # Get paginated workflow runs with cursor-based navigation
                    pages = client.paginate(
                        path=f"/repos/{repo_full_name}/actions/runs",
                        params={
                            "per_page": 100,
                            "created": f"{checkpoint}..{cursor}",
                        },
                        data_selector="workflow_runs",
                    )

                    # Process all pages for current time window
                    for idx, page in enumerate(pages):
                        if not page:
                            break

                        # Track most recent run date from first result
                        if latest_run_date is None and page:
                            latest_run_date = page[0]["created_at"]

                        # Track oldest run date from last result
                        if page:
                            oldest_run_date = page[-1]["created_at"]

                        yield page

                        # Set in_limit to False if we have reached the 1000 results limit
                        if idx >= 9:
                            in_limit = False

                    # Break conditions:
                    # 1. No results in this window
                    # 2. In limit and no more pages to fetch
                    if not page or in_limit:
                        break

                    # Update cursor to oldest run date for next iteration
                    if oldest_run_date:
                        cursor = oldest_run_date
                        logger.debug(
                            f"Updating cursor to {cursor} for {repo_full_name}"
                        )

                # Update checkpoint after all pages are processed
                if latest_run_date:
                    checkpoints[repo_full_name] = latest_run_date
                    logger.info(
                        f"Updated workflow runs checkpoint for {repo_full_name} to {latest_run_date}"
                    )

            except Exception as e:
                logger.error(
                    f"Failed to process workflow runs for {repo_full_name}: {str(e)}"
                )
                continue

    return [repositories, commits, workflow_runs]
