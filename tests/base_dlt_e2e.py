import os
import signal
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Optional

import dlt
import docker
import duckdb
from pydantic import BaseModel, Field, field_validator

# ============================================================================
# UTILITIES
# ============================================================================


@contextmanager
def timeout(seconds):
    """Context manager for adding timeout to operations."""

    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")

    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


# ============================================================================
# BASE DLT E2E TEST FRAMEWORK
# ============================================================================


class DltE2eTest(BaseModel, ABC):
    """Base class for DLT E2E tests with setup/test/teardown phases.

    This class combines configuration and test logic into a single Pydantic model.
    The DuckDB database file name is automatically derived from the test_name.

    Example:
        test = MyE2eTest(
            test_name="my_postgres_test",
            docker_config={
                "image": "postgres:13",
                "name": "test-pg",
                "ports": {"5432/tcp": 5432},
                "environment": {"POSTGRES_PASSWORD": "password"}
            }
        )
        test.run_test_suite()
    """

    # Docker configuration
    docker_config: Dict[str, Any] = Field(
        description="Docker configuration for setting up source infrastructure using native Docker SDK parameters."
    )

    # Source readiness configuration
    source_ready_timeout: int = Field(
        default=60, description="Timeout in seconds for waiting for source to be ready"
    )
    source_ready_delay: int = Field(
        default=2, description="Delay in seconds between source readiness checks"
    )

    # Test name for DuckDB file naming
    test_name: str = Field(description="Name of the test, used for DuckDB file naming")

    # Internal attributes (not part of Pydantic model)
    docker_client: Optional[docker.DockerClient] = Field(default=None, exclude=True)
    container: Optional[docker.models.containers.Container] = Field(
        default=None, exclude=True
    )
    duckdb_connection: Optional[duckdb.DuckDBPyConnection] = Field(
        default=None, exclude=True
    )
    duckdb_file: Optional[str] = Field(default=None, exclude=True)

    class Config:
        arbitrary_types_allowed = True

    @field_validator("docker_config")
    def validate_docker_config(cls, v):
        if not v:
            raise ValueError("docker_config cannot be empty")
        if not isinstance(v, dict):
            raise ValueError("docker_config must be a dictionary")
        if "image" not in v:
            raise ValueError("Docker config must have an 'image' field")
        return v

    @field_validator("test_name")
    def validate_test_name(cls, v):
        if not v or not v.strip():
            raise ValueError("test_name cannot be empty")
        # Remove any characters that might be problematic for file names
        import re

        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "test_name can only contain letters, numbers, underscores, and hyphens"
            )
        return v

    def __init__(self, **data):
        """Initialize the E2E test with configuration."""
        super().__init__(**data)
        self.docker_client = docker.from_env()
        self.container = None
        self.duckdb_connection = None
        self.duckdb_file = f"{self.test_name}.duckdb"

    def run_test_suite(self) -> bool:
        """Run the complete E2E test suite with setup/test/teardown phases."""
        print(f"🧪 Starting {self.__class__.__name__}")
        print("=" * 60)

        try:
            # Setup phase
            if not self.setup_phase():
                print("❌ Setup phase failed")
                return False

            # Test phase
            success = self.test_phase()
            if success:
                print("✅ Test phase completed successfully")
            else:
                print("❌ Test phase failed")

            return success

        except Exception as e:
            print(f"❌ Test suite failed with exception: {e}")
            import traceback

            traceback.print_exc()
            return False

        finally:
            # Teardown phase (always runs)
            self.teardown_phase()

    def setup_phase(self) -> bool:
        """Setup phase: start Docker source infrastructure and DuckDB destination."""
        print("\n🏗️  SETUP PHASE")
        print("-" * 30)

        try:
            # Setup Docker source infrastructure
            if not self._setup_docker_source():
                return False

            # Wait for source to be ready
            if not self._wait_for_source_ready():
                self._cleanup_docker()
                return False

            # Setup DuckDB destination
            if not self._setup_duckdb_destination():
                self._cleanup_docker()
                return False

            print("✅ Setup phase completed successfully")
            return True

        except Exception as e:
            print(f"❌ Setup phase failed: {e}")
            self._cleanup_docker()
            return False

    @abstractmethod
    def test_phase(self) -> bool:
        """Test phase: run the actual DLT pipeline tests."""
        raise NotImplementedError

    def teardown_phase(self):
        """Teardown phase: cleanup Docker containers and DuckDB connections."""
        print("\n🧹 TEARDOWN PHASE")
        print("-" * 30)

        # Close DuckDB connection
        if self.duckdb_connection:
            try:
                self.duckdb_connection.close()
                print("✅ DuckDB connection closed")
            except Exception as e:
                print(f"⚠️  Failed to close DuckDB connection: {e}")

        # Cleanup Docker containers
        self._cleanup_docker()

        # Cleanup DuckDB files
        self._cleanup_duckdb_files()

        print("✅ Teardown phase completed")

    # ========================================================================
    # DOCKER SOURCE INFRASTRUCTURE
    # ========================================================================

    @abstractmethod
    def check_source_ready(self) -> bool:
        """Check if the source infrastructure is ready for connections."""
        raise NotImplementedError

    def _setup_docker_source(self) -> bool:
        """Setup Docker source infrastructure."""
        print("🐳 Setting up Docker source infrastructure...")

        try:
            # Parse Docker configuration
            config = self._parse_docker_config()
            container_name = config.get("name") or config.get("container_name")

            # Stop and remove existing container if it exists and name is provided
            if container_name:
                try:
                    existing_container = self.docker_client.containers.get(
                        container_name
                    )
                    existing_container.stop()
                    existing_container.remove()
                    print(f"✅ Removed existing container: {container_name}")
                except docker.errors.NotFound:
                    # Container doesn't exist, which is fine
                    pass
                except Exception as e:
                    print(f"⚠️  Warning: Could not clean up existing container: {e}")

            print(f"ℹ️  Container arguments: {config}")

            # Run Docker container
            self.container = self.docker_client.containers.run(**config)

            print("✅ Docker source infrastructure started successfully")
            return True

        except Exception as e:
            print(f"❌ Error setting up Docker source infrastructure: {e}")
            return False

    def _parse_docker_config(self) -> Dict[str, Any]:
        """Parse docker_config into Docker SDK format."""
        config = self.docker_config.copy()
        # Ensure detach is set for non-blocking container execution
        config.setdefault("detach", True)
        return config

    def _wait_for_source_ready(self) -> bool:
        """Wait for source infrastructure to be ready."""
        print("⏳ Waiting for source infrastructure to be ready...")

        timeout_seconds = self.source_ready_timeout
        delay_seconds = self.source_ready_delay

        @timeout(timeout_seconds)
        def check_ready():
            while True:
                if self.check_source_ready():
                    return True
                time.sleep(delay_seconds)

        try:
            check_ready()
            print("✅ Source infrastructure is ready")
            return True
        except TimeoutError:
            print(
                f"❌ Timeout waiting for source infrastructure to be ready (timeout: {timeout_seconds}s)"
            )
            return False

    def _cleanup_docker(self):
        """Cleanup Docker containers."""
        try:
            if self.container:
                # Stop and remove the container using the container object
                self.container.stop()
                self.container.remove()
                print("✅ Docker container cleaned up")
            else:
                print("ℹ️  No container to clean up")
        except docker.errors.NotFound:
            print("ℹ️  Container not found (already cleaned up)")
        except Exception as e:
            print(f"⚠️ Failed to clean up Docker containers: {e}")
        finally:
            self.container = None

    # ========================================================================
    # DUCKDB DESTINATION
    # ========================================================================

    def _setup_duckdb_destination(self) -> bool:
        """Setup DuckDB destination."""
        print("🦆 Setting up DuckDB destination...")

        try:
            self._cleanup_duckdb_files()
            self.duckdb_connection = duckdb.connect(self.duckdb_file)
            print("✅ DuckDB destination established")
            return True
        except Exception as e:
            print(f"❌ Failed to setup DuckDB destination: {e}")
            return False

    def _cleanup_duckdb_files(self):
        """Clean up DuckDB files."""
        files_to_remove = [self.duckdb_file, f"{self.duckdb_file}.wal"]

        for file_path in files_to_remove:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"✅ Removed existing file: {file_path}")
            except Exception as e:
                print(f"⚠️ Failed to remove file {file_path}: {e}")

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def run_dlt_pipeline(self, pipeline_kwargs: dict, run_kwargs: dict) -> Any:
        """Helper method to run a DLT pipeline with the DuckDB destination.

        Args:
            pipeline_kwargs: Arguments for dlt.pipeline()
            run_kwargs: Arguments for pipeline.run()

        Returns:
            Load info from the pipeline run
        """
        if not self.duckdb_connection:
            raise RuntimeError(
                "DuckDB connection not established. Call setup_phase() first."
            )

        # Create pipeline
        pipeline = dlt.pipeline(
            **pipeline_kwargs,
            destination=dlt.destinations.duckdb(self.duckdb_connection),
        )

        # Run pipeline
        load_info = pipeline.run(**run_kwargs)
        print(f"ℹ️  {load_info}")

        return load_info

    def query_duckdb(self, query: str) -> Any:
        """Execute a query on the DuckDB connection.

        Args:
            query: SQL query to execute

        Returns:
            Query result
        """
        if not self.duckdb_connection:
            raise RuntimeError(
                "DuckDB connection not established. Call setup_phase() first."
            )

        return self.duckdb_connection.sql(query)

    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table in DuckDB.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows in the table
        """
        result = self.query_duckdb(f"SELECT COUNT(*) FROM {table_name}")
        return result.fetchone()[0]
