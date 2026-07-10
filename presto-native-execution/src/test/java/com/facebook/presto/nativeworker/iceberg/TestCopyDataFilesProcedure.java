/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestCopyDataFilesProcedure
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Test
    public void testCopyDataFileSucceeds()
    {
        String tableName = "copy_source_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, name varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')", 3);

            MaterializedResult filesResult = computeActual("SELECT file_path FROM \"" + tableName + "$files\"");
            assertEquals(filesResult.getRowCount(), 1,
                    "Expected exactly one data file for the freshly-written table");
            String sourceFilePath = (String) filesResult.getOnlyValue();
            assertNotNull(sourceFilePath, "file_path must not be null");

            String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1);
            String sourceLocation = sourceFilePath.substring(0, sourceFilePath.lastIndexOf('/'));
            String destLocation = sourceLocation + "/copy_dest";

            assertUpdate(
                    format("CALL system.copy_data_files(schema => '%s', table_name => '%s', source_location => '%s', dest_location => '%s', file_name => '%s')",
                            schemaName, tableName, sourceLocation, destLocation, fileName),
                    3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSourceTableUnchangedAfterCopy()
    {
        String tableName = "copy_source_unchanged_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (10, 'foo'), (20, 'bar')", 2);

            String sourceFilePath = (String) computeActual(
                    "SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1").getOnlyValue();
            String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1);
            String sourceLocation = sourceFilePath.substring(0, sourceFilePath.lastIndexOf('/'));
            String destLocation = sourceLocation + "/copy_dest2";

            assertUpdate(
                    format("CALL system.copy_data_files(schema => '%s', table_name => '%s', source_location => '%s', dest_location => '%s', file_name => '%s')",
                            schemaName, tableName, sourceLocation, destLocation, fileName),
                    2);

            assertQuery("SELECT * FROM " + tableName, "VALUES (10, 'foo'), (20, 'bar')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCopyDataFileFailsWhenSourceFileNotInSnapshot()
    {
        String tableName = "copy_missing_file_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'x'), (2, 'y')", 2);

            String sourceLocation = "file:///non/existent/path";
            String destLocation = "file:///tmp/copy_dest_bogus";
            String fileName = "bogus_file.parquet";

            assertQueryFails(
                    format("CALL system.copy_data_files(schema => '%s', table_name => '%s', source_location => '%s', dest_location => '%s', file_name => '%s')",
                            schemaName, tableName, sourceLocation, destLocation, fileName),
                    ".*No file was copied.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCopyDataFileFromPartitionedTable()
    {
        String tableName = "copy_partitioned_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 varchar) WITH (partitioning = ARRAY['c2'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'foo'), (2, 'foo'), (3, 'foo')", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'bar'), (5, 'bar')", 2);

            String sourceFilePath = (String) computeActual(
                    "SELECT file_path FROM \"" + tableName + "$files\" WHERE file_path LIKE '%foo%' LIMIT 1").getOnlyValue();
            assertNotNull(sourceFilePath, "Expected a data file for partition c2='foo'");

            String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1);
            String sourceLocation = sourceFilePath.substring(0, sourceFilePath.lastIndexOf('/'));
            String destLocation = sourceLocation + "/copy_part_dest";

            assertUpdate(
                    format("CALL system.copy_data_files(schema => '%s', table_name => '%s', source_location => '%s', dest_location => '%s', file_name => '%s')",
                            schemaName, tableName, sourceLocation, destLocation, fileName),
                    3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInvalidParameterCases()
    {
        String tableName = "copy_invalid_param_table";
        String schemaName = getSession().getSchema().get();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (a int, b varchar)");

            // Missing required argument 'schema'.
            assertQueryFails(
                    "CALL system.copy_data_files()",
                    ".*Required procedure argument 'schema' is missing.*");

            // Non-existent schema.
            assertQueryFails(
                    "CALL system.copy_data_files('nonexistent_schema', 'some_table', 'file:///src', 'file:///dest', 'f.parquet')",
                    "Schema nonexistent_schema does not exist");

            // Named + positional arguments mixed.
            assertQueryFails(
                    format("CALL system.copy_data_files('n', table_name => '%s', source_location => 'file:///src', dest_location => 'file:///dest', file_name => 'f.parquet')", tableName),
                    ".*Named and positional arguments cannot be mixed.*");
        }
        finally {
            dropTable(tableName);
        }
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }
}
