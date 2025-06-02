import base64
from datetime import datetime, timedelta
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
    lookback_days: int = 1,
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
        data_from=repositories, primary_key="id", write_disposition="merge"
    )
    def repository_labels(repositories):
        """Transform and load GitHub repository labels data with checkpointing.

        Args:
            repositories: Iterator of repository data from GitHub API

        Yields:
            Paginated repository label data for each repository
        """
        for repository in repositories:
            repo_full_name = repository["full_name"]
            yield from client.paginate(
                path=f"/repos/{repo_full_name}/labels",
                params={"per_page": 100},
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
                stored_checkpoint = checkpoints.get(repo_full_name, start_date)
                if stored_checkpoint != start_date:
                    checkpoint_date = datetime.fromisoformat(
                        stored_checkpoint.replace("Z", "+00:00")
                    )
                    adjusted_checkpoint = checkpoint_date - timedelta(
                        days=lookback_days
                    )
                    checkpoint = adjusted_checkpoint.isoformat()
                else:
                    checkpoint = stored_checkpoint

                latest_commit_date = None

                # Get paginated commits since adjusted checkpoint
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
    def workflows(repositories):
        """Transform and load GitHub workflows data with checkpointing.

        Args:
            repositories: Iterator of repository data from GitHub API

        Yields:
            Paginated workflows data for each repository
        """
        for repository in repositories:
            repo_full_name = repository["full_name"]
            try:
                # Get paginated workflows with cursor-based navigation
                yield from client.paginate(
                    path=f"/repos/{repo_full_name}/actions/workflows",
                    params={
                        "per_page": 100,
                    },
                )
            except Exception as e:
                logger.error(
                    f"Failed to process workflows for {repo_full_name}: {str(e)}"
                )
                continue

    @dlt.transformer(data_from=workflows, primary_key="id", write_disposition="merge")
    def workflow_runs(workflows):
        """Transform and load GitHub workflow runs data with checkpointing.

        Args:
            workflows: Iterator of workflow data from GitHub API

        Yields:
            Paginated workflow runs data for each workflow
        """
        checkpoints = dlt.current.resource_state().setdefault(
            "workflow_runs_checkpoints", {}
        )

        for workflow in workflows:
            workflow_id = workflow["id"]
            # Extract repository full name from the workflow URL
            repo_full_name = (
                workflow["url"].split("/repos/")[1].split("/actions/workflows")[0]
            )

            try:
                # Initialize checkpoint and tracking variables
                checkpoint_key = f"{repo_full_name}_{workflow_id}"
                stored_checkpoint = checkpoints.get(checkpoint_key, start_date)
                if stored_checkpoint != start_date:
                    checkpoint_date = datetime.fromisoformat(
                        stored_checkpoint.replace("Z", "+00:00")
                    )
                    adjusted_checkpoint = checkpoint_date - timedelta(
                        days=lookback_days
                    )
                    checkpoint = adjusted_checkpoint.isoformat()
                else:
                    checkpoint = stored_checkpoint

                latest_run_date = None

                # Get paginated workflow runs since adjusted checkpoint
                pages = client.paginate(
                    path=f"/repos/{repo_full_name}/actions/workflows/{workflow_id}/runs",
                    params={
                        "per_page": 100,
                        "created": f">={checkpoint}",
                    },
                )

                for page in pages:
                    if not page:
                        break

                    # Track most recent run date from first page
                    if latest_run_date is None and page:
                        latest_run_date = page[0]["created_at"]

                    yield page

                # Update checkpoint after successful processing
                if latest_run_date:
                    checkpoints[checkpoint_key] = latest_run_date
                    logger.info(
                        f"Updated workflow runs checkpoint for {repo_full_name}/{workflow_id} to {latest_run_date}"
                    )

            except Exception as e:
                logger.error(
                    f"Failed to process workflow runs for {repo_full_name}/{workflow_id}: {str(e)}"
                )
                continue

    @dlt.transformer(
        data_from=repositories, primary_key="id", write_disposition="merge"
    )
    def pull_requests(repositories):
        """Transform and load GitHub pull requests data with checkpointing.

        Args:
            repositories: Iterator of repository data from GitHub API

        Yields:
            Paginated pull requests data for each repository
        """
        checkpoints = dlt.current.resource_state().setdefault("pr_checkpoints", {})

        for repository in repositories:
            repo_full_name = repository["full_name"]
            try:
                # Initialize checkpoint and tracking variables
                stored_checkpoint = checkpoints.get(repo_full_name, start_date)
                if stored_checkpoint != start_date:
                    checkpoint_date = datetime.fromisoformat(
                        stored_checkpoint.replace("Z", "+00:00")
                    )
                    adjusted_checkpoint = checkpoint_date - timedelta(
                        days=lookback_days
                    )
                    checkpoint = adjusted_checkpoint.isoformat()
                else:
                    checkpoint = stored_checkpoint

                latest_pr_date = None

                # Get paginated pull requests since adjusted checkpoint
                pages = client.paginate(
                    path=f"/repos/{repo_full_name}/pulls",
                    params={
                        "per_page": 100,
                        "state": "all",
                        "sort": "updated",
                        "direction": "desc",
                    },
                )

                for page in pages:
                    if not page:
                        break

                    if latest_pr_date is None and page:
                        latest_pr_date = page[0]["updated_at"]

                    # Filter by updated_at date to respect the checkpoint
                    filtered_prs = [pr for pr in page if pr["updated_at"] >= checkpoint]

                    if not filtered_prs:
                        break

                    yield filtered_prs

                # Update checkpoint after successful processing
                if latest_pr_date:
                    checkpoints[repo_full_name] = latest_pr_date
                    logger.info(
                        f"Updated pull requests checkpoint for {repo_full_name} to {latest_pr_date}"
                    )

            except Exception as e:
                logger.error(
                    f"Failed to process pull requests for {repo_full_name}: {str(e)}"
                )
                continue

    return [repositories, repository_labels, commits, workflows, workflow_runs, pull_requests]
