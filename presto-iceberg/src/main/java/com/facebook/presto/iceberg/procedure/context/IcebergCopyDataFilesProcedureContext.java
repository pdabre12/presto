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
package com.facebook.presto.iceberg.procedure.context;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplitSource;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergUtil.filterByFilePath;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;
import static java.util.Objects.requireNonNull;

public class IcebergCopyDataFilesProcedureContext
        implements IcebergCommonProcedureContext
{
    private final Table table;
    private final IcebergAbstractMetadata metadata;
    private final String sourceFileLocation;
    private final String destFileLocation;
    private final String fileName;

    public IcebergCopyDataFilesProcedureContext(
            Table table,
            IcebergAbstractMetadata metadata,
            String sourceLocation,
            String destLocation,
            String fileName)
    {
        this.table = requireNonNull(table, "table is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
        requireNonNull(sourceLocation, "sourceLocation is null");
        requireNonNull(destLocation, "sourceLocation is null");
        this.sourceFileLocation = buildFilePath(sourceLocation, fileName);
        this.destFileLocation = buildFilePath(destLocation, fileName);
    }

    public Table getTable()
    {
        return table;
    }

    public IcebergAbstractMetadata getMetadata()
    {
        return metadata;
    }

    public String getSourceFileLocation()
    {
        return sourceFileLocation;
    }

    public String getDestFileLocation()
    {
        return destFileLocation;
    }

    public String getFileName()
    {
        return fileName;
    }

    /**
     * Customize split source to filter to only the specified source file.
     * This is called by IcebergSplitManager.getSplits().
     */
    @Override
    public Optional<IcebergSplitSource> customizeSplitSource(
            ConnectorSession session,
            TableScan tableScan,
            TupleDomain<IcebergColumnHandle> metadataColumnConstraints)
    {
        // Get all file scan tasks from the table scan
        CloseableIterable<FileScanTask> allTasks = tableScan.planFiles();

        // Filter to only the specified source file
        CloseableIterable<FileScanTask> filteredTasks = filterByFilePath(allTasks, sourceFileLocation);

        // Create split source with filtered tasks
        return Optional.of(new IcebergSplitSource(
                session,
                getTargetSplitSize(session, tableScan).toBytes(),
                filteredTasks,
                metadataColumnConstraints));
    }

    private static String buildFilePath(String fileLocation, String fileName)
    {
        return fileLocation.endsWith("/")
                ? fileLocation + fileName
                : fileLocation + "/" + fileName;
    }
}
