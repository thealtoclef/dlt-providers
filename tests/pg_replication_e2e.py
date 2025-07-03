import time

import dlt
import psycopg2
from dlt.sources.credentials import ConnectionStringCredentials

from dlt_providers.sources.pg_replication import pg_replication
from base_dlt_e2e import DltE2eTest


class PostgreSQLReplicationE2eTest(DltE2eTest):
    """E2E test for PostgreSQL replication using the base DLT E2E framework."""

    # PostgreSQL configuration
    postgres_credentials: ConnectionStringCredentials = ConnectionStringCredentials(
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    slot_name: str = "my_replication_slot"
    pub_name: str = "my_publication"
    schema_name: str = "inventory"
    test_table: str = "customers"

    def check_source_ready(self) -> bool:
        """Check if PostgreSQL is ready to accept connections."""
        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False

    def test_phase(self) -> bool:
        """Run the main test phase with sequential procedural testing."""
        print("\n🧪 Starting PostgreSQL replication test phase...")

        try:
            # Configure DLT
            dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True
            dlt.config["normalize.parquet_normalizer.add_dlt_id"] = True

            # Run the sequential procedural test flow
            return self._run_sequential_test_flow()

        except Exception as e:
            print(f"❌ Test phase failed: {e}")
            return False

    def _run_sequential_test_flow(self) -> bool:
        """Run the sequential procedural test flow as specified."""
        print("\n🔄 Starting sequential procedural test flow...")

        # Track row counts for validation
        baseline_count = 0

        try:
            # 1st run (snapshot) - assert by row count of source and destination
            print("\n📊 STEP 1: Initial snapshot")
            if not self._run_pipeline():
                return False

            # First, check that all source tables exist in destination
            if not self._check_all_tables_replicated():
                print(
                    "❌ STEP 1 FAILED: Not all source tables replicated to destination"
                )
                return False

            # Get baseline counts
            source_count = self._get_source_count()
            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if source_count != dest_count:
                print(
                    f"❌ STEP 1 FAILED: Source count ({source_count}) != Destination count ({dest_count})"
                )
                return False

            baseline_count = dest_count
            print(f"✅ STEP 1 PASSED: Snapshot validation (count: {baseline_count})")

            # 2nd run without changes - assert by row count of destination not change
            print("\n📊 STEP 2: No changes run")
            time.sleep(2)
            if not self._run_pipeline("append-only"):
                return False

            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if dest_count != baseline_count:
                print(
                    f"❌ STEP 2 FAILED: Expected={baseline_count}, actual={dest_count}"
                )
                return False

            print(f"✅ STEP 2 PASSED: No changes validation (count: {dest_count})")

            # 3rd run with insert - assert by row count increase
            print("\n📊 STEP 3: Insert data replication")
            inserted_ids = self._insert_test_data()
            if not inserted_ids:
                print("❌ STEP 3 FAILED: Could not insert test data")
                return False

            time.sleep(2)
            if not self._run_pipeline():
                return False

            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if dest_count <= baseline_count:
                print(
                    f"❌ STEP 3 FAILED: count did not increase (before={baseline_count}, after={dest_count})"
                )
                return False

            baseline_count = dest_count
            print(
                f"✅ STEP 3 PASSED: Insert validation (count increased to: {dest_count})"
            )

            # 4th run with update (merge mode) - assert by changed data and same row count
            print("\n📊 STEP 4: Update data replication (merge mode)")
            updated_ids = self._update_test_data()
            if not updated_ids:
                print("❌ STEP 4 FAILED: Could not update test data")
                return False

            time.sleep(2)
            if not self._run_pipeline("merge"):
                return False

            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if dest_count != baseline_count:
                print(
                    f"❌ STEP 4 FAILED: count changed (before={baseline_count}, after={dest_count})"
                )
                return False

            # Verify the updated data
            if not self._verify_updated_data(updated_ids[0]):
                print("❌ STEP 4 FAILED: data not updated")
                return False

            print(f"✅ STEP 4 PASSED: Update merge validation (count: {dest_count})")

            # 5th run with delete (merge mode) - assert by row count decrease
            print("\n📊 STEP 5: Delete data replication (merge mode)")
            deleted_ids = self._delete_test_data()
            if not deleted_ids:
                print("❌ STEP 5 FAILED: Could not delete test data")
                return False

            time.sleep(2)
            if not self._run_pipeline("merge"):
                return False

            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if dest_count >= baseline_count:
                print(
                    f"❌ STEP 5 FAILED: count did not decrease (before={baseline_count}, after={dest_count})"
                )
                return False

            baseline_count = dest_count
            print(
                f"✅ STEP 5 PASSED: Delete merge validation (count decreased to: {dest_count})"
            )

            # 6th run with update (append-only mode) - assert by latest record and count increase
            print("\n📊 STEP 6: Update data replication (append-only mode)")
            updated_ids_append = self._update_test_data()
            if not updated_ids_append:
                print("❌ STEP 6 FAILED: Could not update test data")
                return False

            time.sleep(2)
            if not self._run_pipeline("append-only"):
                return False

            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if dest_count <= baseline_count:
                print(
                    f"❌ STEP 6 FAILED: count did not increase (before={baseline_count}, after={dest_count})"
                )
                return False

            # Verify the latest record has updated data
            if not self._verify_latest_record_updated(updated_ids_append[0]):
                print("❌ STEP 6 FAILED: latest record not updated")
                return False

            baseline_count = dest_count
            print(
                f"✅ STEP 6 PASSED: Update append-only validation (count increased to: {dest_count})"
            )

            # 7th run with delete (append-only mode) - assert by _dlt_deleted_ts and count increase
            print("\n📊 STEP 7: Delete data replication (append-only mode)")
            deleted_ids_append = self._delete_test_data()
            if not deleted_ids_append:
                print("❌ STEP 7 FAILED: Could not delete test data")
                return False

            time.sleep(2)
            if not self._run_pipeline("append-only"):
                return False

            dest_count = self._get_dest_count()

            # Show table description before assertion for debugging
            self._describe_destination_table()

            if dest_count <= baseline_count:
                print(
                    f"❌ STEP 7 FAILED: count did not increase (before={baseline_count}, after={dest_count})"
                )
                return False

            # Verify the latest record has _dlt_deleted_ts set
            if not self._verify_latest_record_deleted(deleted_ids_append[0]):
                print("❌ STEP 7 FAILED: latest record not marked as deleted")
                return False

            print(
                f"✅ STEP 7 PASSED: Delete append-only validation (count increased to: {dest_count})"
            )

            # Summary
            print("\n🎉 ALL POSTGRESQL REPLICATION TESTS COMPLETED!")
            print("✅ Sequential procedural test flow completed successfully")
            print("✅ All 7 test steps passed validation")

            return True

        except Exception as e:
            print(f"❌ Sequential test flow failed: {e}")
            return False

    def _run_pipeline(self, write_mode: str | None = None) -> bool:
        """Run a single pipeline with specified write mode."""
        try:
            # Create source
            source_kwargs = {
                "credentials": self.postgres_credentials,
                "slot_name": self.slot_name,
                "pub_name": self.pub_name,
                "schema_name": self.schema_name,
            }
            if write_mode:
                source_kwargs["write_mode"] = write_mode
            source = pg_replication(**source_kwargs)

            # Run pipeline using base class helper
            load_info = self.run_dlt_pipeline(
                pipeline_kwargs={
                    "pipeline_name": "pg_replication_e2e_test",
                    "dataset_name": "pg_replication",
                },
                run_kwargs={
                    "data": source,
                    "loader_file_format": "parquet",
                    "schema_contract": "evolve",
                },
            )

            # Check for failed jobs
            if load_info.has_failed_jobs:
                print("❌ Pipeline has failed jobs:")
                for job in load_info.failed_jobs:
                    print(f"  - {job}")
                return False

            return True

        except Exception as e:
            print(f"❌ Pipeline run failed: {e}")
            return False

    def _get_source_count(self) -> int:
        """Get row count from PostgreSQL source."""
        try:
            postgres_conn = self._get_postgres_connection()
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute(
                f"SELECT COUNT(*) FROM {self.schema_name}.{self.test_table}"
            )
            count = postgres_cursor.fetchone()[0]
            postgres_cursor.close()
            postgres_conn.close()
            return count
        except Exception as e:
            print(f"⚠️  Could not get source count: {e}")
            return 0

    def _get_dest_count(self) -> int:
        """Get row count from DuckDB destination."""
        try:
            result = self.query_duckdb(f"SELECT COUNT(*) FROM {self.test_table}")
            return result.fetchone()[0]
        except Exception as e:
            print(f"⚠️  Could not get destination count: {e}")
            return 0

    def _verify_updated_data(self, record_id: int) -> bool:
        """Verify that a record has been updated in the destination."""
        try:
            # Get the record from source
            postgres_conn = self._get_postgres_connection()
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute(
                f"SELECT email FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                (record_id,),
            )
            source_result = postgres_cursor.fetchone()
            postgres_cursor.close()
            postgres_conn.close()

            if not source_result:
                return False

            source_email = source_result[0]

            # Get the record from destination
            dest_result = self.query_duckdb(
                f"SELECT email FROM {self.test_table} WHERE id = {record_id}"
            )
            dest_row = dest_result.fetchone()

            if not dest_row:
                return False

            dest_email = dest_row[0]

            # Check if emails match (indicating update was replicated)
            return source_email == dest_email

        except Exception as e:
            print(f"⚠️  Could not verify updated data: {e}")
            return False

    def _verify_latest_record_updated(self, record_id: int) -> bool:
        """Verify that the latest record for an ID has updated data in append-only mode."""
        try:
            # Get the latest record for this ID ordered by _dlt_load_id
            result = self.query_duckdb(
                f"SELECT email FROM {self.test_table} WHERE id = {record_id} "
                f"ORDER BY CAST(_dlt_load_id AS BIGINT) DESC LIMIT 1"
            )
            latest_row = result.fetchone()

            if not latest_row:
                return False

            # Get the record from source to compare
            postgres_conn = self._get_postgres_connection()
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute(
                f"SELECT email FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                (record_id,),
            )
            source_result = postgres_cursor.fetchone()
            postgres_cursor.close()
            postgres_conn.close()

            if not source_result:
                return False

            # Check if the latest record matches the source
            return latest_row[0] == source_result[0]

        except Exception as e:
            print(f"⚠️  Could not verify latest record updated: {e}")
            return False

    def _verify_latest_record_deleted(self, record_id: int) -> bool:
        """Verify that the latest record for an ID has _dlt_deleted_ts set in append-only mode."""
        try:
            # Get the latest record for this ID ordered by _dlt_load_id
            result = self.query_duckdb(
                f"SELECT _dlt_deleted_ts FROM {self.test_table} WHERE id = {record_id} "
                f"ORDER BY CAST(_dlt_load_id AS BIGINT) DESC LIMIT 1"
            )
            latest_row = result.fetchone()

            if not latest_row:
                return False

            # Check if _dlt_deleted_ts is not null
            return latest_row[0] is not None

        except Exception as e:
            print(f"⚠️  Could not verify latest record deleted: {e}")
            return False

    def _describe_destination_table(self) -> None:
        """Describe the destination table structure and sample data using DuckDB native formatting."""
        try:
            print(f"\n📋 Destination Table Description: {self.test_table}")

            # Use DuckDB's native DESCRIBE for better formatting
            print("\n📊 Table Schema:")
            describe_result = self.query_duckdb(f"DESCRIBE {self.test_table}")
            # Print the result in DuckDB's native table format
            print(describe_result.df().to_string(index=False))

            # Get row count
            count = self._get_dest_count()
            print(f"\n📈 Total rows: {count}")

            # Show sample data if table has rows
            if count > 0:
                print("\n📄 Data:")
                sample_result = self.query_duckdb(f"SELECT * FROM {self.test_table}")
                print(sample_result.df().to_string(index=False))

        except Exception as e:
            print(f"⚠️  Could not describe destination table: {e}")

    def _check_all_tables_replicated(self) -> bool:
        """Check that all source tables exist in destination."""
        try:
            print("\n🔍 Checking table replication coverage...")

            # Get source tables
            postgres_conn = self._get_postgres_connection()
            postgres_cursor = postgres_conn.cursor()
            postgres_cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{self.schema_name}' AND table_type = 'BASE TABLE'"
            )
            source_tables = [row[0] for row in postgres_cursor.fetchall()]
            postgres_cursor.close()
            postgres_conn.close()

            # Get destination tables
            dest_result = self.query_duckdb("SHOW TABLES")
            dest_tables = [row[0] for row in dest_result.fetchall()]

            print(f"   📊 Source tables ({len(source_tables)}): {source_tables}")
            print(f"   📊 Destination tables ({len(dest_tables)}): {dest_tables}")

            # Check if all source tables exist in destination
            missing_tables = [
                table for table in source_tables if table not in dest_tables
            ]

            if missing_tables:
                print(f"   ❌ Missing tables in destination: {missing_tables}")
                return False
            else:
                print("   ✅ All source tables found in destination")
                return True

        except Exception as e:
            print(f"⚠️  Could not check table replication: {e}")
            return False

    def _validate_data_consistency(
        self, write_mode: str, test_type: str, test_ids: list = None
    ) -> bool:
        """Validate data consistency based on write mode and test type."""
        print(
            f"🔍 Validating data consistency (mode: {write_mode}, type: {test_type})..."
        )

        try:
            # Check if table exists in DuckDB
            try:
                tables_result = self.query_duckdb("SHOW TABLES")
                tables = tables_result.fetchall()
                print(f"📋 Available tables in DuckDB: {tables}")

                table_exists = any(self.test_table in str(table) for table in tables)
                if not table_exists:
                    print(f"❌ Table '{self.test_table}' not found in DuckDB")
                    return False
            except Exception as e:
                print(f"⚠️  Could not list DuckDB tables: {e}")
                return False

            if test_type == "snapshot":
                return self._validate_snapshot_data(write_mode)
            elif test_type == "insert":
                return self._validate_insert_data(test_ids)
            elif test_type == "update":
                return self._validate_update_data(write_mode, test_ids)
            elif test_type == "delete":
                return self._validate_delete_data(write_mode, test_ids)
            else:
                print(f"❌ Unknown test type: {test_type}")
                return False

        except Exception as e:
            print(f"❌ Data validation failed: {e}")
            return False

    def _validate_snapshot_data(self, write_mode: str) -> bool:
        """Validate snapshot data using count comparison."""
        print("📊 Validating snapshot data using count comparison...")

        # Get PostgreSQL data count
        postgres_conn = self._get_postgres_connection()
        postgres_cursor = postgres_conn.cursor()
        postgres_cursor.execute(
            f"SELECT COUNT(*) FROM {self.schema_name}.{self.test_table}"
        )
        postgres_count = postgres_cursor.fetchone()[0]
        postgres_cursor.close()
        postgres_conn.close()
        print(f"✅ PostgreSQL row count: {postgres_count}")

        # Get DuckDB data count
        duckdb_result = self.query_duckdb(f"SELECT COUNT(*) FROM {self.test_table}")
        duckdb_count = duckdb_result.fetchone()[0]
        print(f"✅ DuckDB row count: {duckdb_count}")

        if postgres_count != duckdb_count:
            print(
                f"❌ Row count mismatch: PostgreSQL={postgres_count}, DuckDB={duckdb_count}"
            )
            return False

        print("✅ Snapshot validation passed")
        return True

    def _validate_insert_data(self, test_ids: list) -> bool:
        """Validate insert operations by checking specific IDs."""
        print(f"📝 Validating insert data for IDs: {test_ids}...")

        for test_id in test_ids:
            # Check if ID exists in DuckDB
            duckdb_result = self.query_duckdb(
                f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id}"
            )
            duckdb_count = duckdb_result.fetchone()[0]

            if duckdb_count == 0:
                print(f"❌ Inserted ID {test_id} not found in DuckDB")
                return False

            print(f"✅ Inserted ID {test_id} found in DuckDB")

        print("✅ Insert validation passed")
        return True

    def _validate_update_data(self, write_mode: str, test_ids: list) -> bool:
        """Validate update operations based on write mode."""
        print(f"✏️  Validating update data for IDs: {test_ids} (mode: {write_mode})...")

        if write_mode == "append-only":
            # In append-only mode, check for _dlt_deleted_ts column existence
            try:
                duckdb_result = self.query_duckdb(
                    f"PRAGMA table_info('{self.test_table}')"
                )
                columns = [row[1] for row in duckdb_result.fetchall()]

                if "_dlt_deleted_ts" not in columns:
                    print("❌ _dlt_deleted_ts column not found in append-only mode")
                    return False

                print("✅ _dlt_deleted_ts column exists in append-only mode")

                # Check that updated IDs have multiple rows (original + updated)
                for test_id in test_ids:
                    duckdb_result = self.query_duckdb(
                        f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id}"
                    )
                    row_count = duckdb_result.fetchone()[0]

                    if row_count < 2:
                        print(
                            f"❌ Updated ID {test_id} should have multiple rows in append-only mode, found {row_count}"
                        )
                        return False

                    print(
                        f"✅ Updated ID {test_id} has {row_count} rows in append-only mode"
                    )

            except Exception as e:
                print(f"❌ Error validating append-only update: {e}")
                return False

        elif write_mode == "merge":
            # In merge mode, compare source and destination data for updated IDs
            for test_id in test_ids:
                # Get PostgreSQL data
                postgres_conn = self._get_postgres_connection()
                postgres_cursor = postgres_conn.cursor()
                postgres_cursor.execute(
                    f"SELECT id, first_name, last_name, email FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                    (test_id,),
                )
                postgres_row = postgres_cursor.fetchone()
                postgres_cursor.close()
                postgres_conn.close()

                # Get DuckDB data
                duckdb_result = self.query_duckdb(
                    f"SELECT id, first_name, last_name, email FROM {self.test_table} WHERE id = {test_id}"
                )
                duckdb_row = duckdb_result.fetchone()

                if postgres_row != duckdb_row:
                    print(
                        f"❌ Updated ID {test_id} data mismatch: PostgreSQL={postgres_row}, DuckDB={duckdb_row}"
                    )
                    return False

                print(
                    f"✅ Updated ID {test_id} data matches between source and destination"
                )

        print("✅ Update validation passed")
        return True

    def _validate_delete_data(self, write_mode: str, test_ids: list) -> bool:
        """Validate delete operations based on write mode."""
        print(f"🗑️  Validating delete data for IDs: {test_ids} (mode: {write_mode})...")

        if write_mode == "append-only":
            # In append-only mode, check for _dlt_deleted_ts column and deleted records
            try:
                duckdb_result = self.query_duckdb(
                    f"PRAGMA table_info('{self.test_table}')"
                )
                columns = [row[1] for row in duckdb_result.fetchall()]

                if "_dlt_deleted_ts" not in columns:
                    print("❌ _dlt_deleted_ts column not found in append-only mode")
                    return False

                print("✅ _dlt_deleted_ts column exists in append-only mode")

                # Check that deleted IDs have records with _dlt_deleted_ts set
                for test_id in test_ids:
                    duckdb_result = self.query_duckdb(
                        f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id} AND _dlt_deleted_ts IS NOT NULL"
                    )
                    deleted_count = duckdb_result.fetchone()[0]

                    if deleted_count == 0:
                        print(
                            f"❌ Deleted ID {test_id} not found with _dlt_deleted_ts set"
                        )
                        return False

                    print(f"✅ Deleted ID {test_id} found with _dlt_deleted_ts set")

            except Exception as e:
                print(f"❌ Error validating append-only delete: {e}")
                return False

        elif write_mode == "merge":
            # In merge mode, check that deleted IDs are not present in destination
            for test_id in test_ids:
                duckdb_result = self.query_duckdb(
                    f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id}"
                )
                duckdb_count = duckdb_result.fetchone()[0]

                if duckdb_count > 0:
                    print(
                        f"❌ Deleted ID {test_id} still found in DuckDB (should be hard deleted)"
                    )
                    return False

                print(f"✅ Deleted ID {test_id} properly removed from DuckDB")

        print("✅ Delete validation passed")
        return True

    def _get_postgres_connection(self):
        """Get a direct PostgreSQL connection for testing."""
        return psycopg2.connect(
            host="localhost",
            port=5432,
            user="postgres",
            password="postgres",
            database="postgres",
        )

    def _insert_test_data(self) -> list:
        """Insert test data to trigger replication."""
        print("📝 Attempting to insert test data...")
        test_ids = []

        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()

            # Insert two new customer records with high IDs to avoid conflicts
            base_id = int(time.time()) % 100000 + 10000  # Generate a base unique ID

            # First record
            test_id1 = base_id
            cursor.execute(
                f"INSERT INTO {self.schema_name}.{self.test_table} (id, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
                (test_id1, "Test", "User", f"test.user.{test_id1}@example.com"),
            )
            test_ids.append(test_id1)
            print(f"✅ Inserted test record with ID {test_id1}")

            # Second record with different ID and email
            test_id2 = base_id + 1
            cursor.execute(
                f"INSERT INTO {self.schema_name}.{self.test_table} (id, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
                (test_id2, "Test", "User2", f"test.user2.{test_id2}@example.com"),
            )
            test_ids.append(test_id2)
            print(f"✅ Inserted test record with ID {test_id2}")

            conn.commit()
            cursor.close()
            conn.close()
            return test_ids

        except Exception as e:
            print(f"⚠️  Could not insert test data: {e}")
            print("   This might be due to permission restrictions or missing table")
            return []

    def _update_test_data(self) -> list:
        """Update test data to trigger replication."""
        print("✏️  Attempting to update test data...")

        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()

            # Try to update an existing record
            cursor.execute(
                f"SELECT id FROM {self.schema_name}.{self.test_table} LIMIT 1"
            )
            result = cursor.fetchone()
            if result:
                test_id = result[0]
                new_email = f"updated.{int(time.time())}.{test_id}@example.com"
                cursor.execute(
                    f"UPDATE {self.schema_name}.{self.test_table} SET email = %s WHERE id = %s",
                    (new_email, test_id),
                )
                conn.commit()
                print(f"✅ Updated record ID {test_id} with new email: {new_email}")
                cursor.close()
                conn.close()
                return [test_id]
            else:
                print("⚠️  No records found to update")
                cursor.close()
                conn.close()
                return []

        except Exception as e:
            print(f"⚠️  Could not update test data: {e}")
            print("   This might be due to permission restrictions or missing table")
            return []

    def _delete_test_data(self) -> list:
        """Delete test data to trigger replication."""
        print("🗑️  Attempting to delete test data...")

        try:
            conn = self._get_postgres_connection()
            cursor = conn.cursor()

            # Find records that we can safely delete (test records we inserted)
            cursor.execute(
                f"SELECT id FROM {self.schema_name}.{self.test_table} WHERE first_name = 'Test' AND last_name = 'User' LIMIT 1"
            )
            result = cursor.fetchone()
            if result:
                test_id = result[0]
                cursor.execute(
                    f"DELETE FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                    (test_id,),
                )
                conn.commit()
                print(f"✅ Deleted test record with ID {test_id}")
                cursor.close()
                conn.close()
                return [test_id]
            else:
                # If no test records found, delete the oldest record as a fallback
                cursor.execute(
                    f"SELECT id FROM {self.schema_name}.{self.test_table} ORDER BY id DESC LIMIT 1"
                )
                result = cursor.fetchone()
                if result:
                    test_id = result[0]
                    cursor.execute(
                        f"DELETE FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                        (test_id,),
                    )
                    conn.commit()
                    print(f"✅ Deleted record with ID {test_id}")
                    cursor.close()
                    conn.close()
                    return [test_id]
                else:
                    print("⚠️  No records found to delete")
                    cursor.close()
                    conn.close()
                    return []

        except Exception as e:
            print(f"⚠️  Could not delete test data: {e}")
            print("   This might be due to permission restrictions or missing table")
            return []


def main():
    """Run the PostgreSQL replication E2E test suite."""
    test = PostgreSQLReplicationE2eTest(
        test_name="pg_replication_e2e_test",
        docker_config={
            "image": "quay.io/debezium/example-postgres:latest",
            "name": "postgres",
            "ports": {"5432/tcp": 5432},
            "environment": {
                "POSTGRES_USER": "postgres",
                "POSTGRES_PASSWORD": "postgres",
            },
        },
        source_ready_timeout=60,
        source_ready_delay=2,
    )

    success = test.run_test_suite()
    if not success:
        exit(1)


if __name__ == "__main__":
    main()
