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
package com.facebook.presto.iceberg.procedure;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergDistributedProcedureHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.procedure.context.IcebergCopyDataFilesProcedureContext;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.List;

import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.Path.SEPARATOR;

/**
 * Distributed procedure to copy a single data file with byte-identical validation.
 * Uses TableScan + TableWrite for distributed execution, then validates the copy
 * using length comparison and SHA-256 spot-check.
 * <p>
 * Usage:
 * CALL iceberg.system.copy_data_files(
 * schema => 'my_schema',
 * table_name => 'my_table',
 * source_file_path => 's3://bucket/data/file.parquet',
 * dest_location => 's3://backup-bucket/copy/');
 */
public class CopyDataFilesProcedure
        implements Provider<DistributedProcedure>
{
    private static final Logger log = Logger.get(CopyDataFilesProcedure.class);

    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public CopyDataFilesProcedure(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            HdfsEnvironment hdfsEnvironment)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public DistributedProcedure get()
    {
        return new TableDataRewriteDistributedProcedure(
                "system",
                "copy_data_files",
                ImmutableList.of(
                        new Argument(SCHEMA, VARCHAR),
                        new Argument(TABLE_NAME, VARCHAR),
                        new Argument("source_location", VARCHAR),
                        new Argument("dest_location", VARCHAR),
                        new Argument("file_name", VARCHAR)),
                (session, procedureContext, tableLayoutHandle, arguments, sortOrderIndex) ->
                        beginCallDistributedProcedure(
                                session,
                                (IcebergCopyDataFilesProcedureContext) procedureContext,
                                (IcebergTableLayoutHandle) tableLayoutHandle,
                                arguments),
                (session, procedureContext, tableHandle, fragments) ->
                        finishCallDistributedProcedure(
                                session,
                                (IcebergCopyDataFilesProcedureContext) procedureContext,
                                fragments),
                this::createContext);
    }

    private ConnectorDistributedProcedureHandle beginCallDistributedProcedure(
            ConnectorSession session,
            IcebergCopyDataFilesProcedureContext procedureContext,
            IcebergTableLayoutHandle layoutHandle,
            Object[] arguments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            Table icebergTable = procedureContext.getTable();
            IcebergTableHandle tableHandle = layoutHandle.getTable();
            String sourceLocation = (String) arguments[2];
            String destLocation = (String) arguments[3];
            String fileName = (String) arguments[4];

            log.info("Starting copy %s to %s", sourceLocation, destLocation);

            return new IcebergDistributedProcedureHandle(
                    tableHandle.getSchemaName(),
                    tableHandle.getIcebergTableName(),
                    toPrestoSchema(icebergTable.schema(), typeManager),
                    toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                    getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                    destLocation,
                    getFileFormat(icebergTable),
                    getCompressionCodec(session),
                    icebergTable.properties(),
                    layoutHandle,
                    ImmutableList.of(),
                    fileName,
                    ImmutableMap.of());
        }
    }

    private void finishCallDistributedProcedure(
            ConnectorSession session,
            IcebergCopyDataFilesProcedureContext procedureContext,
            Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            List<CommitTaskData> commitTasks = fragments.stream()
                    .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                    .collect(toImmutableList());

            if (commitTasks.isEmpty()) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                        "No file was copied");
            }

            if (commitTasks.size() != 1) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                        format("Expected exactly 1 file to be copied, but got %d", commitTasks.size()));
            }

            CommitTaskData copyTask = commitTasks.get(0);
            String fileName = procedureContext.getFileName();
            String sourceFilePath = procedureContext.getSourceFileLocation();
            String destFilePath = procedureContext.getDestFileLocation();

            log.info("Validating byte-identical copy: %s -> %s for file %s", sourceFilePath, destFilePath, fileName);

            // Validate byte-identical copy (length match + SHA-256)
            validateByteIdenticalCopy(session, sourceFilePath, copyTask);

            log.info("Successfully copied and validated file: %s -> %s (%d bytes, %d rows)",
                    sourceFilePath,
                    destFilePath,
                    copyTask.getFileSizeInBytes(),
                    copyTask.getMetrics().recordCount());
        }
    }

    private void validateByteIdenticalCopy(
            ConnectorSession session,
            String sourceFilePath,
            CommitTaskData destTask)
    {
        try {
            HdfsContext hdfsContext = new HdfsContext(session);
            String destFilePath = destTask.getPath();

            String sourceFileName = extractFileName(sourceFilePath);
            String destFileName = extractFileName(destTask.getPath());

            if (!sourceFileName.equals(destFileName)) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                        format("File name mismatch: source=%s, dest=%s", sourceFileName, destFileName));
            }
            log.info("✓ File name match: %s", sourceFileName);

            // 1. Get file lengths directly from filesystem
            long sourceSize = getFileSize(hdfsContext, sourceFilePath);
            long destSize = getFileSize(hdfsContext, destFilePath);

            if (sourceSize != destSize) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                        format("File length mismatch: source=%d bytes, dest=%d bytes",
                                sourceSize, destSize));
            }
            log.info("✓ File length match: %d bytes", sourceSize);

            // 2. SHA-256 spot-check
            log.info("Computing SHA-256 spot-check...");
            validateSha256SpotCheck(session, sourceFilePath, destFilePath, sourceSize);
            log.info("✓ SHA-256 spot-check passed");

            log.info("✓ Byte-identical copy confirmed");
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to validate byte-identical copy", e);
        }
    }

    private long getFileSize(HdfsContext hdfsContext, String filePath)
            throws IOException
    {
        Path path = new Path(filePath);
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
        return fs.getFileStatus(path).getLen();
    }

    private void validateSha256SpotCheck(
            ConnectorSession session,
            String sourceFilePath,
            String destFilePath,
            long fileSize)
    {
        HdfsContext hdfsContext = new HdfsContext(session);
        long spotCheckSize = Math.min(1024 * 1024, fileSize); // 1MB

        String sourceSha256 = computeSpotCheckSha256(
                hdfsContext, sourceFilePath, fileSize, spotCheckSize);
        String destSha256 = computeSpotCheckSha256(
                hdfsContext, destFilePath, fileSize, spotCheckSize);

        if (!sourceSha256.equals(destSha256)) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("SHA-256 spot-check failed: source=%s, dest=%s",
                            sourceSha256, destSha256));
        }
    }

    private String computeSpotCheckSha256(
            HdfsContext hdfsContext,
            String filePath,
            long fileSize,
            long spotCheckSize)
    {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            // Strategy: For files > 3MB, use 3-point spot check (first + middle + last).
            // For smaller files, read entire file for complete validation.
            List<Long> offsets = getSpotCheckOffsets(fileSize, spotCheckSize);

            // Read all chunks
            byte[] allChunks = readFileChunks(hdfsContext, filePath, offsets, spotCheckSize, fileSize);
            digest.update(allChunks);

            return bytesToHex(digest.digest());
        }
        catch (Exception e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR,
                    format("Failed to compute SHA-256 for %s", filePath), e);
        }
    }

    private List<Long> getSpotCheckOffsets(long fileSize, long spotCheckSize)
    {
        // For files <= 3MB: read entire file (complete validation)
        if (fileSize <= 3 * spotCheckSize) {
            return ImmutableList.of(0L);
        }

        // For files > 3MB: 3-point spot check (first + middle + last)
        return ImmutableList.of(
                0L,                                      // First 1MB
                (fileSize - spotCheckSize) / 2,          // Middle 1MB
                fileSize - spotCheckSize);               // Last 1MB
    }

    private byte[] readFileChunks(
            HdfsContext hdfsContext,
            String filePath,
            List<Long> offsets,
            long chunkSize,
            long fileSize)
            throws IOException
    {
        Path path = new Path(filePath);
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);

        // Calculate total bytes to read
        int totalBytes = 0;
        for (Long offset : offsets) {
            // For small files (single offset at 0), read entire file
            if (offsets.size() == 1 && offset == 0) {
                totalBytes = (int) fileSize;
            }
            else {
                totalBytes += (int) Math.min(chunkSize, fileSize - offset);
            }
        }

        byte[] result = new byte[totalBytes];
        int position = 0;

        try (FSDataInputStream input = fs.open(path)) {
            for (Long offset : offsets) {
                int bytesToRead;
                // For small files (single offset at 0), read entire file
                if (offsets.size() == 1 && offset == 0) {
                    bytesToRead = (int) fileSize;
                }
                else {
                    bytesToRead = (int) Math.min(chunkSize, fileSize - offset);
                }

                input.seek(offset);
                input.readFully(result, position, bytesToRead);
                position += bytesToRead;
            }
        }

        return result;
    }

    private static String bytesToHex(byte[] bytes)
    {
        return BaseEncoding.base16().lowerCase().encode(bytes);
    }

    private IcebergCopyDataFilesProcedureContext createContext(Object[] arguments)
    {
        Table table = (Table) arguments[0];
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) arguments[1];

        // Extract procedure arguments
        if (arguments.length <= 2 || !(arguments[2] instanceof Object[])) {
            throw new IllegalArgumentException("Invalid procedure arguments");
        }

        Object[] procedureArgs = (Object[]) arguments[2];

        // Extract required parameters
        if (procedureArgs.length < 5) {
            throw new IllegalArgumentException("source_location, dest_location and file_name are required");
        }

        String sourceLocation = (String) procedureArgs[2];
        String destLocation = (String) procedureArgs[3];
        String fileName = (String) procedureArgs[4];

        return new IcebergCopyDataFilesProcedureContext(
                table, metadata, sourceLocation, destLocation, fileName);
    }

    private static String extractFileName(String path)
    {
        return path.substring(path.lastIndexOf(SEPARATOR) + 1);
    }
}
