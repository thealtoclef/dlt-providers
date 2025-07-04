import time

import dlt
import pymysql
from base_dlt_e2e import DltE2eTest
from dlt.sources.credentials import ConnectionStringCredentials

from dlt_providers.sources.mysql_replication import mysql_replication


class MySQLReplicationE2eTest(DltE2eTest):
    """E2E test for MySQL replication using the base DLT E2E framework."""

    # MySQL configuration
    mysql_credentials: ConnectionStringCredentials = ConnectionStringCredentials(
        "mysql://debezium:dbz@localhost:3306/inventory"
    )
    server_id: int = 123456
    schema_name: str = "inventory"
    test_table: str = "customers"

    def check_source_ready(self) -> bool:
        """Check if MySQL is ready to accept connections."""
        try:
            conn = self._get_mysql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False

    def test_phase(self) -> bool:
        """Run the main test phase with comprehensive replication testing."""
        print("\n🧪 Starting MySQL replication test phase...")

        try:
            # Configure DLT
            dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True
            dlt.config["normalize.parquet_normalizer.add_dlt_id"] = True

            # Run the sequential test flow
            return self._run_sequential_test_flow()

        except Exception as e:
            print(f"❌ Test phase failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    def _run_sequential_test_flow(self) -> bool:
        """Run the sequential procedural test flow as specified."""
        print("\n🔄 Starting sequential procedural test flow...")

        # Track row counts for validation
        baseline_count = 0

        try:
            # 1st run (snapshot) - assert by row count of source and destination
            print("\n📊 STEP 1: Initial snapshot")
            if not self._run_pipeline("append-only"):
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
            if not self._run_pipeline("append-only"):
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

            # Verify the updated data in append-only mode
            if not self._verify_latest_record_updated(updated_ids_append[0]):
                print("❌ STEP 6 FAILED: updated data not found in append-only mode")
                return False

            baseline_count = dest_count
            print(
                f"✅ STEP 6 PASSED: Update append-only validation (count increased to: {dest_count})"
            )

            # 7th run with delete (append-only mode) - assert by latest record and count increase
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

            # Verify the deleted data in append-only mode
            if not self._verify_latest_record_deleted(deleted_ids_append[0]):
                print("❌ STEP 7 FAILED: deleted record not marked in append-only mode")
                return False

            print(
                f"✅ STEP 7 PASSED: Delete append-only validation (count increased to: {dest_count})"
            )

            # If we got here, all tests passed
            print("\n🎉 All tests passed successfully!")
            return True

        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            import traceback

            traceback.print_exc()
            return False

        return True

    def _run_pipeline(self, write_mode: str) -> bool:
        """Run the DLT pipeline with specified write mode."""
        try:
            # Create source
            source = mysql_replication(
                credentials=self.mysql_credentials,
                server_id=self.server_id,
                schema_name=self.schema_name,
                write_mode=write_mode,
            )

            # Run pipeline using base class helper
            load_info = self.run_dlt_pipeline(
                pipeline_kwargs={
                    "pipeline_name": "mysql_replication_e2e_test",
                    "dataset_name": "mysql_replication",
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
            print(f"❌ Pipeline execution failed: {e}")
            return False

    def _get_source_count(self) -> int:
        """Get row count from MySQL source."""
        try:
            mysql_conn = self._get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(
                f"SELECT COUNT(*) FROM {self.schema_name}.{self.test_table}"
            )
            count = mysql_cursor.fetchone()[0]
            mysql_cursor.close()
            mysql_conn.close()
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
            mysql_conn = self._get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(
                f"SELECT email FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                (record_id,),
            )
            source_result = mysql_cursor.fetchone()
            mysql_cursor.close()
            mysql_conn.close()

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
            mysql_conn = self._get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(
                f"SELECT email FROM {self.schema_name}.{self.test_table} WHERE id = %s",
                (record_id,),
            )
            source_result = mysql_cursor.fetchone()
            mysql_cursor.close()
            mysql_conn.close()

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

    def _validate_data_consistency(
        self, write_mode: str, test_type: str, test_ids: list = None
    ) -> bool:
        """Validate data consistency with specialized validation based on test type."""
        print(
            f"🔍 Validating data consistency (write_mode: {write_mode}, test_type: {test_type}, test_ids: {test_ids})..."
        )

        try:
            # Check if table exists in DuckDB
            tables_result = self.query_duckdb("SHOW TABLES")
            tables = tables_result.fetchall()
            table_exists = any(self.test_table in str(table) for table in tables)
            if not table_exists:
                print(f"❌ Table '{self.test_table}' not found in DuckDB")
                return False

            # Dispatch to specialized validation methods based on test type
            if test_type == "snapshot":
                return self._validate_snapshot_data()
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

    def _validate_snapshot_data(self) -> bool:
        """Validate snapshot data using count-based comparison."""
        print("📊 Performing count-based validation for snapshot...")

        # Get MySQL data count
        mysql_conn = self._get_mysql_connection()
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute(f"SELECT COUNT(*) FROM {self.test_table}")
        mysql_count = mysql_cursor.fetchone()[0]
        mysql_cursor.close()
        mysql_conn.close()

        # Get DuckDB data count
        duckdb_result = self.query_duckdb(f"SELECT COUNT(*) FROM {self.test_table}")
        duckdb_count = duckdb_result.fetchone()[0]

        print(f"📊 MySQL rows: {mysql_count}, DuckDB rows: {duckdb_count}")
        if mysql_count != duckdb_count:
            print(f"❌ Row count mismatch: MySQL={mysql_count}, DuckDB={duckdb_count}")
            return False

        print("✅ Snapshot validation passed")
        return True

    def _validate_insert_data(self, test_ids: list) -> bool:
        """Validate inserted data by checking presence of specific IDs."""
        if not test_ids:
            print("⚠️  No test IDs provided for insert validation")
            return True

        print(f"🔍 Validating presence of inserted IDs: {test_ids}")

        for test_id in test_ids:
            duckdb_result = self.query_duckdb(
                f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id}"
            )
            count = duckdb_result.fetchone()[0]
            if count == 0:
                print(f"❌ Inserted ID {test_id} not found in DuckDB")
                return False
            print(f"✅ Found inserted ID {test_id} in DuckDB")

        print("✅ Insert validation passed")
        return True

    def _validate_update_data(self, write_mode: str, test_ids: list) -> bool:
        """Validate updated data based on write mode."""
        if not test_ids:
            print("⚠️  No test IDs provided for update validation")
            return True

        print(f"🔍 Validating updates for IDs: {test_ids} in {write_mode} mode")

        if write_mode == "append-only":
            # Check for _dlt_deleted_ts column and multiple rows for updated IDs
            try:
                # Check if _dlt_deleted_ts column exists
                duckdb_result = self.query_duckdb(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.test_table}' AND column_name = '_dlt_deleted_ts'"
                )
                if not duckdb_result.fetchall():
                    print("❌ _dlt_deleted_ts column not found in append-only mode")
                    return False
                print("✅ _dlt_deleted_ts column exists")

                # Check for multiple rows for updated IDs (old + new)
                for test_id in test_ids:
                    duckdb_result = self.query_duckdb(
                        f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id}"
                    )
                    count = duckdb_result.fetchone()[0]
                    if count < 2:
                        print(
                            f"❌ Expected multiple rows for updated ID {test_id}, found {count}"
                        )
                        return False
                    print(
                        f"✅ Found {count} rows for updated ID {test_id} (append-only)"
                    )

            except Exception as e:
                print(f"❌ Error validating append-only updates: {e}")
                return False

        elif write_mode == "merge":
            # Direct source-destination comparison
            for test_id in test_ids:
                # Get data from MySQL
                mysql_conn = self._get_mysql_connection()
                mysql_cursor = mysql_conn.cursor()
                mysql_cursor.execute(
                    f"SELECT id, first_name, last_name, email FROM {self.test_table} WHERE id = {test_id}"
                )
                mysql_row = mysql_cursor.fetchone()
                mysql_cursor.close()
                mysql_conn.close()

                # Get data from DuckDB
                duckdb_result = self.query_duckdb(
                    f"SELECT id, first_name, last_name, email FROM {self.test_table} WHERE id = {test_id}"
                )
                duckdb_row = duckdb_result.fetchone()

                if mysql_row != duckdb_row:
                    print(
                        f"❌ Data mismatch for updated ID {test_id}: MySQL={mysql_row}, DuckDB={duckdb_row}"
                    )
                    return False
                print(f"✅ Data matches for updated ID {test_id}")

        print("✅ Update validation passed")
        return True

    def _validate_delete_data(self, write_mode: str, test_ids: list) -> bool:
        """Validate deleted data based on write mode."""
        if not test_ids:
            print("⚠️  No test IDs provided for delete validation")
            return True

        print(f"🔍 Validating deletes for IDs: {test_ids} in {write_mode} mode")

        if write_mode == "append-only":
            # Check for _dlt_deleted_ts and presence of deleted records
            try:
                # Check if _dlt_deleted_ts column exists
                duckdb_result = self.query_duckdb(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.test_table}' AND column_name = '_dlt_deleted_ts'"
                )
                if not duckdb_result.fetchall():
                    print("❌ _dlt_deleted_ts column not found in append-only mode")
                    return False
                print("✅ _dlt_deleted_ts column exists")

                # Check for deleted records with _dlt_deleted_ts set
                for test_id in test_ids:
                    duckdb_result = self.query_duckdb(
                        f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id} AND _dlt_deleted_ts IS NOT NULL"
                    )
                    count = duckdb_result.fetchone()[0]
                    if count == 0:
                        print(
                            f"❌ No deleted record found for ID {test_id} with _dlt_deleted_ts set"
                        )
                        return False
                    print(
                        f"✅ Found deleted record for ID {test_id} with _dlt_deleted_ts"
                    )

            except Exception as e:
                print(f"❌ Error validating append-only deletes: {e}")
                return False

        elif write_mode == "merge":
            # Check absence of deleted IDs
            for test_id in test_ids:
                duckdb_result = self.query_duckdb(
                    f"SELECT COUNT(*) FROM {self.test_table} WHERE id = {test_id}"
                )
                count = duckdb_result.fetchone()[0]
                if count > 0:
                    print(f"❌ Deleted ID {test_id} still found in DuckDB (merge mode)")
                    return False
                print(f"✅ Deleted ID {test_id} not found in DuckDB (merge mode)")

        print("✅ Delete validation passed")
        return True

    def _get_mysql_connection(self):
        """Get a direct MySQL connection for testing (replication user)."""
        return pymysql.connect(
            host="localhost",
            port=3306,
            user="debezium",
            password="dbz",
            database="inventory",
            autocommit=True,
        )

    def _get_mysql_admin_connection(self):
        """Get a MySQL connection with admin privileges for data modifications."""
        return pymysql.connect(
            host="localhost",
            port=3306,
            user="mysqluser",
            password="mysqlpw",
            database="inventory",
            autocommit=True,
        )

    def _insert_test_data(self) -> list:
        """Insert test data to trigger replication using admin credentials."""
        print("📝 Attempting to insert test data...")
        test_ids = []

        try:
            conn = self._get_mysql_admin_connection()
            cursor = conn.cursor()

            # Insert two new customer records with high IDs to avoid conflicts
            base_id = int(time.time()) % 100000 + 10000  # Generate a base unique ID

            # First record
            test_id1 = base_id
            cursor.execute(
                f"INSERT INTO {self.test_table} (id, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
                (test_id1, "Test", "User", f"test.user.{test_id1}@example.com"),
            )
            test_ids.append(test_id1)
            print(f"✅ Inserted test record with ID {test_id1}")

            # Second record with different ID and email
            test_id2 = base_id + 1
            cursor.execute(
                f"INSERT INTO {self.test_table} (id, first_name, last_name, email) VALUES (%s, %s, %s, %s)",
                (test_id2, "Test", "User2", f"test.user2.{test_id2}@example.com"),
            )
            test_ids.append(test_id2)
            print(f"✅ Inserted test record with ID {test_id2}")

            cursor.close()
            conn.close()
            return test_ids

        except Exception as e:
            print(f"⚠️  Could not insert test data: {e}")
            print("   This might be due to permission restrictions")
            return []

    def _update_test_data(self) -> list:
        """Update test data to trigger replication using admin credentials."""
        print("✏️  Attempting to update test data...")

        try:
            conn = self._get_mysql_admin_connection()
            cursor = conn.cursor()

            # Try to update an existing record
            cursor.execute(f"SELECT id FROM {self.test_table} LIMIT 1")
            result = cursor.fetchone()
            if result:
                test_id = result[0]
                new_email = f"updated.{int(time.time())}.{test_id}@example.com"
                cursor.execute(
                    f"UPDATE {self.test_table} SET email = %s WHERE id = %s",
                    (new_email, test_id),
                )
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
            print("   This might be due to permission restrictions")
            return []

    def _delete_test_data(self) -> list:
        """Delete test data to trigger replication using admin credentials."""
        print("🗑️  Attempting to delete test data...")

        try:
            conn = self._get_mysql_admin_connection()
            cursor = conn.cursor()

            # Find records that we can safely delete (test records we inserted)
            cursor.execute(
                f"SELECT id FROM {self.test_table} WHERE first_name = 'Test' AND last_name = 'User' LIMIT 1"
            )
            result = cursor.fetchone()
            if result:
                test_id = result[0]
                cursor.execute(
                    f"DELETE FROM {self.test_table} WHERE id = %s", (test_id,)
                )
                print(f"✅ Deleted test record with ID {test_id}")
                cursor.close()
                conn.close()
                return [test_id]
            else:
                # If no test records found, delete the oldest record as a fallback
                cursor.execute(
                    f"SELECT id FROM {self.test_table} ORDER BY id DESC LIMIT 1"
                )
                result = cursor.fetchone()
                if result:
                    test_id = result[0]
                    cursor.execute(
                        f"DELETE FROM {self.test_table} WHERE id = %s", (test_id,)
                    )
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
            print("   This might be due to permission restrictions")
            return []

    def _check_all_tables_replicated(self) -> bool:
        """Check that all source tables exist in destination."""
        try:
            # Get source tables
            mysql_conn = self._get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(f"SHOW TABLES FROM {self.schema_name}")
            source_tables = [row[0] for row in mysql_cursor.fetchall()]
            mysql_cursor.close()
            mysql_conn.close()

            # Get destination tables
            dest_result = self.query_duckdb("SHOW TABLES")
            dest_tables = [row[0] for row in dest_result.fetchall()]

            print(f"📊 Source tables: {source_tables}")
            print(f"📊 Destination tables: {dest_tables}")

            # Check if all source tables exist in destination
            missing_tables = [
                table for table in source_tables if table not in dest_tables
            ]
            if missing_tables:
                print(f"❌ Missing tables in destination: {missing_tables}")
                return False

            print("✅ All source tables replicated to destination")
            return True

        except Exception as e:
            print(f"⚠️  Could not check table replication: {e}")
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


def main():
    """Run the MySQL replication E2E test suite."""
    test = MySQLReplicationE2eTest(
        test_name="mysql_replication_e2e_test",
        docker_config={
            "image": "quay.io/debezium/example-mysql:latest",
            "name": "mysql",
            "ports": {"3306/tcp": 3306},
            "environment": {
                "MYSQL_ROOT_PASSWORD": "debezium",
                "MYSQL_USER": "mysqluser",
                "MYSQL_PASSWORD": "mysqlpw",
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
