import math
import time
from functools import wraps
from typing import Dict, Union, Callable, Any

import pendulum
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.sources.helpers.rest_client.auth import OAuthJWTAuth


def with_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple = (Exception,),
):
    """
    A decorator that implements exponential backoff retry logic for API calls.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay between retries in seconds (default: 1.0)
        backoff_factor: Multiplier for delay between retries (default: 2.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        retryable_exceptions: Tuple of exceptions that should trigger a retry

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} retries. "
                            f"Last error: {str(e)}"
                        )
                        raise e

                    # Calculate delay with exponential backoff
                    delay = min(initial_delay * (backoff_factor**attempt), max_delay)
                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt + 1}/{max_retries + 1}. "
                        f"Retrying in {delay:.2f} seconds. Error: {str(e)}"
                    )
                    time.sleep(delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


class GitHubAppAuth(OAuthJWTAuth):
    def create_jwt_payload(self) -> Dict[str, Union[str, int]]:
        now = pendulum.now()
        return {
            "iss": self.client_id,
            "exp": math.floor((now.add(minutes=10)).timestamp()),
            "iat": math.floor(now.timestamp()),
        }

    def obtain_token(self) -> None:
        try:
            import jwt
        except ModuleNotFoundError:
            raise MissingDependencyException("dlt OAuth helpers", ["PyJWT"])

        payload = self.create_jwt_payload()
        obtain_token_headers = {
            "Authorization": f"Bearer {jwt.encode(payload, self.load_private_key(), algorithm='RS256')}"
        }

        logger.debug(f"Obtaining token from {self.auth_endpoint}")

        response = self.session.post(self.auth_endpoint, headers=obtain_token_headers)
        response.raise_for_status()

        token_response = response.json()
        self.token = token_response["token"]
        self.token_expiry = pendulum.parse(token_response.get("expires_at"))
