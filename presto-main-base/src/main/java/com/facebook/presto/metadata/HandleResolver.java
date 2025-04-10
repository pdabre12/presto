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
package com.facebook.presto.metadata;

import com.facebook.presto.connector.informationSchema.InformationSchemaHandleResolver;
import com.facebook.presto.connector.system.SystemHandleResolver;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionHandleResolver;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.TableFunctionSplitResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.split.EmptySplitHandleResolver;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HandleResolver
{
    private final ConcurrentMap<String, MaterializedHandleResolver> handleResolvers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MaterializedFunctionHandleResolver> functionHandleResolvers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MaterializedTableFunctionHandleResolver> tableFunctionHandleResolvers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MaterializedTableFunctionSplitResolver> tableFunctionSplitResolvers = new ConcurrentHashMap<>();

    @Inject
    public HandleResolver()
    {
        handleResolvers.put(REMOTE_CONNECTOR_ID.toString(), new MaterializedHandleResolver(new RemoteHandleResolver()));
        handleResolvers.put("$system", new MaterializedHandleResolver(new SystemHandleResolver()));
        handleResolvers.put("$info_schema", new MaterializedHandleResolver(new InformationSchemaHandleResolver()));
        handleResolvers.put("$empty", new MaterializedHandleResolver(new EmptySplitHandleResolver()));

        functionHandleResolvers.put("$static", new MaterializedFunctionHandleResolver(new BuiltInFunctionNamespaceHandleResolver()));
        functionHandleResolvers.put("$session", new MaterializedFunctionHandleResolver(new SessionFunctionHandleResolver()));
    }

    public void addConnectorName(String name, ConnectorHandleResolver resolver)
    {
        requireNonNull(name, "name is null");
        requireNonNull(resolver, "resolver is null");
        MaterializedHandleResolver existingResolver = handleResolvers.putIfAbsent(name, new MaterializedHandleResolver(resolver));
        checkState(existingResolver == null || existingResolver.equals(resolver),
                "Connector '%s' is already assigned to resolver: %s", name, existingResolver);
    }

    public void addFunctionNamespace(String name, FunctionHandleResolver resolver)
    {
        requireNonNull(name, "name is null");
        requireNonNull(resolver, "resolver is null");
        MaterializedFunctionHandleResolver existingResolver = functionHandleResolvers.putIfAbsent(name, new MaterializedFunctionHandleResolver(resolver));
        checkState(existingResolver == null || existingResolver.equals(resolver), "Name %s is already assigned to function resolver: %s", name, existingResolver);
    }

    public void addTableFunctionNamespace(String name, TableFunctionHandleResolver resolver)
    {
        requireNonNull(name, "name is null");
        requireNonNull(resolver, "resolver is null");
        MaterializedTableFunctionHandleResolver existingResolver = tableFunctionHandleResolvers.putIfAbsent(name, new MaterializedTableFunctionHandleResolver(resolver));
        checkState(existingResolver == null || existingResolver.equals(resolver), "Name %s is already assigned to table function resolver: %s", name, existingResolver);
    }

    public void addTableFunctionSplitNamespace(String name, TableFunctionSplitResolver resolver)
    {
        requireNonNull(name, "name is null");
        requireNonNull(resolver, "resolver is null");
        MaterializedTableFunctionSplitResolver existingResolver = tableFunctionSplitResolvers.putIfAbsent(name, new MaterializedTableFunctionSplitResolver(resolver));
        checkState(existingResolver == null || existingResolver.equals(resolver), "Name %s is already assigned to table function resolver: %s", name, existingResolver);
    }

    public String getId(ConnectorTableHandle tableHandle)
    {
        return getId(tableHandle, MaterializedHandleResolver::getTableHandleClass);
    }

    public String getId(ConnectorTableLayoutHandle handle)
    {
        return getId(handle, MaterializedHandleResolver::getTableLayoutHandleClass);
    }

    public String getId(ColumnHandle columnHandle)
    {
        return getId(columnHandle, MaterializedHandleResolver::getColumnHandleClass);
    }

    public String getId(ConnectorSplit split)
    {
        // TODO: Clean up all connectors and make MaterializedTableFunctionSplitResolver
        // just a MaterializedSplitResolver and add Connector's split classes to it.
        // This was added as different table functions in the connector can have different
        // split classes, but be part of the same Connector.
        try {
            return getId(split, MaterializedHandleResolver::getSplitClass);
        }
        catch (Exception e) {
            return getTableFunctionSplitId(split, MaterializedTableFunctionSplitResolver::getTableFunctionSplitClasses);
        }
    }

    public String getId(ConnectorIndexHandle indexHandle)
    {
        return getId(indexHandle, MaterializedHandleResolver::getIndexHandleClass);
    }

    public String getId(ConnectorOutputTableHandle outputHandle)
    {
        return getId(outputHandle, MaterializedHandleResolver::getOutputTableHandleClass);
    }

    public String getId(ConnectorInsertTableHandle insertHandle)
    {
        return getId(insertHandle, MaterializedHandleResolver::getInsertTableHandleClass);
    }

    public String getId(ConnectorDeleteTableHandle deleteHandle)
    {
        return getId(deleteHandle, MaterializedHandleResolver::getDeleteTableHandleClass);
    }

    public String getId(ConnectorPartitioningHandle partitioningHandle)
    {
        return getId(partitioningHandle, MaterializedHandleResolver::getPartitioningHandleClass);
    }

    public String getId(ConnectorTransactionHandle transactionHandle)
    {
        return getId(transactionHandle, MaterializedHandleResolver::getTransactionHandleClass);
    }

    public String getId(FunctionHandle functionHandle)
    {
        return getFunctionNamespaceId(functionHandle, MaterializedFunctionHandleResolver::getFunctionHandleClass);
    }

    public String getId(ConnectorMetadataUpdateHandle metadataUpdateHandle)
    {
        return getId(metadataUpdateHandle, MaterializedHandleResolver::getMetadataUpdateHandleClass);
    }

    public String getId(ConnectorTableFunctionHandle tableFunctionHandle)
    {
        return getTableFunctionId(tableFunctionHandle, MaterializedTableFunctionHandleResolver::getTableFunctionHandleClasses);
    }

    public Class<? extends ConnectorTableHandle> getTableHandleClass(String id)
    {
        return resolverFor(id).getTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass(String id)
    {
        return resolverFor(id).getTableLayoutHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ColumnHandle> getColumnHandleClass(String id)
    {
        return resolverFor(id).getColumnHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorSplit> getSplitClass(String id)
    {
        Optional<Class<? extends ConnectorSplit>> tableFunctionSplit;
        for (Entry<String, MaterializedTableFunctionSplitResolver> entry : tableFunctionSplitResolvers.entrySet()) {
            MaterializedTableFunctionSplitResolver resolver = entry.getValue();
            tableFunctionSplit = resolver.getTableFunctionSplitClasses().stream()
                    .filter(handle -> (entry.getKey() + ":" + handle.getName()).equals(id))
                    .findFirst();
            if (tableFunctionSplit.isPresent()) {
                return tableFunctionSplit.get();
            }
        }
        return resolverFor(id).getSplitClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorIndexHandle> getIndexHandleClass(String id)
    {
        return resolverFor(id).getIndexHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass(String id)
    {
        return resolverFor(id).getOutputTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass(String id)
    {
        return resolverFor(id).getInsertTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorDeleteTableHandle> getDeleteTableHandleClass(String id)
    {
        return resolverFor(id).getDeleteTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass(String id)
    {
        return resolverFor(id).getPartitioningHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass(String id)
    {
        return resolverFor(id).getTransactionHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends FunctionHandle> getFunctionHandleClass(String id)
    {
        return resolverForFunctionNamespace(id).getFunctionHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorMetadataUpdateHandle> getMetadataUpdateHandleClass(String id)
    {
        return resolverFor(id).getMetadataUpdateHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorTableFunctionHandle> getTableFunctionHandleClass(String id)
    {
        Optional<Class<? extends ConnectorTableFunctionHandle>> tableFunctionHandle;
        for (Entry<String, MaterializedTableFunctionHandleResolver> entry : tableFunctionHandleResolvers.entrySet()) {
            MaterializedTableFunctionHandleResolver resolver = entry.getValue();
            tableFunctionHandle = resolver.getTableFunctionHandleClasses().stream()
                    .filter(handle -> (entry.getKey() + ":" + handle.getName()).equals(id))
                    .findFirst();
            if (tableFunctionHandle.isPresent()) {
                return tableFunctionHandle.get();
            }
        }
        throw new IllegalArgumentException("No handle resolver for table function namespace: " + id);
    }

    private MaterializedHandleResolver resolverFor(String id)
    {
        MaterializedHandleResolver resolver = handleResolvers.get(id);
        checkArgument(resolver != null, "No handle resolver for connector: %s", id);
        return resolver;
    }

    private MaterializedFunctionHandleResolver resolverForFunctionNamespace(String id)
    {
        MaterializedFunctionHandleResolver resolver = functionHandleResolvers.get(id);
        checkArgument(resolver != null, "No handle resolver for function namespace: %s", id);
        return resolver;
    }

    private <T> String getId(T handle, Function<MaterializedHandleResolver, Optional<Class<? extends T>>> getter)
    {
        for (Entry<String, MaterializedHandleResolver> entry : handleResolvers.entrySet()) {
            try {
                if (getter.apply(entry.getValue()).map(clazz -> clazz.isInstance(handle)).orElse(false)) {
                    return entry.getKey();
                }
            }
            catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No connector for handle: " + handle + " of type " + handle.getClass());
    }

    private <T> String getFunctionNamespaceId(T handle, Function<MaterializedFunctionHandleResolver, Optional<Class<? extends T>>> getter)
    {
        for (Entry<String, MaterializedFunctionHandleResolver> entry : functionHandleResolvers.entrySet()) {
            try {
                if (getter.apply(entry.getValue()).map(clazz -> clazz.isInstance(handle)).orElse(false)) {
                    return entry.getKey();
                }
            }
            catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No function namespace for handle: " + handle);
    }

    private <T> String getTableFunctionId(ConnectorTableFunctionHandle handle, Function<MaterializedTableFunctionHandleResolver, Set<Class<? extends ConnectorTableFunctionHandle>>> getters)
    {
        for (Entry<String, MaterializedTableFunctionHandleResolver> entry : tableFunctionHandleResolvers.entrySet()) {
            try {
                Optional<String> id = getters.apply(entry.getValue()).stream()
                        .filter(clazz -> clazz.isInstance(handle))
                        .map(Class::getName)
                        .findFirst();
                if (id.isPresent()) {
                    return entry.getKey() + ":" + id.get();
                }
            }
            catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No function namespace for handle: " + handle);
    }

    private <T> String getTableFunctionSplitId(ConnectorSplit split, Function<MaterializedTableFunctionSplitResolver, Set<Class<? extends ConnectorSplit>>> getters)
    {
        for (Entry<String, MaterializedTableFunctionSplitResolver> entry : tableFunctionSplitResolvers.entrySet()) {
            try {
                Optional<String> id = getters.apply(entry.getValue()).stream()
                        .filter(clazz -> clazz.isInstance(split))
                        .map(Class::getName)
                        .findFirst();
                if (id.isPresent()) {
                    return entry.getKey() + ":" + id.get();
                }
            }
            catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No function namespace for handle: " + split);
    }

    private static class MaterializedHandleResolver
    {
        private final Optional<Class<? extends ConnectorTableHandle>> tableHandle;
        private final Optional<Class<? extends ConnectorTableLayoutHandle>> layoutHandle;
        private final Optional<Class<? extends ColumnHandle>> columnHandle;
        private final Optional<Class<? extends ConnectorSplit>> split;
        private final Optional<Class<? extends ConnectorIndexHandle>> indexHandle;
        private final Optional<Class<? extends ConnectorOutputTableHandle>> outputTableHandle;
        private final Optional<Class<? extends ConnectorInsertTableHandle>> insertTableHandle;
        private final Optional<Class<? extends ConnectorDeleteTableHandle>> deleteTableHandle;
        private final Optional<Class<? extends ConnectorPartitioningHandle>> partitioningHandle;
        private final Optional<Class<? extends ConnectorTransactionHandle>> transactionHandle;
        private final Optional<Class<? extends ConnectorMetadataUpdateHandle>> metadataUpdateHandle;

        public MaterializedHandleResolver(ConnectorHandleResolver resolver)
        {
            tableHandle = getHandleClass(resolver::getTableHandleClass);
            layoutHandle = getHandleClass(resolver::getTableLayoutHandleClass);
            columnHandle = getHandleClass(resolver::getColumnHandleClass);
            split = getHandleClass(resolver::getSplitClass);
            indexHandle = getHandleClass(resolver::getIndexHandleClass);
            outputTableHandle = getHandleClass(resolver::getOutputTableHandleClass);
            insertTableHandle = getHandleClass(resolver::getInsertTableHandleClass);
            deleteTableHandle = getHandleClass(resolver::getDeleteTableHandleClass);
            partitioningHandle = getHandleClass(resolver::getPartitioningHandleClass);
            transactionHandle = getHandleClass(resolver::getTransactionHandleClass);
            metadataUpdateHandle = getHandleClass(resolver::getMetadataUpdateHandleClass);
        }

        private static <T> Optional<Class<? extends T>> getHandleClass(Supplier<Class<? extends T>> callable)
        {
            try {
                return Optional.of(callable.get());
            }
            catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        }

        public Optional<Class<? extends ConnectorTableHandle>> getTableHandleClass()
        {
            return tableHandle;
        }

        public Optional<Class<? extends ConnectorTableLayoutHandle>> getTableLayoutHandleClass()
        {
            return layoutHandle;
        }

        public Optional<Class<? extends ColumnHandle>> getColumnHandleClass()
        {
            return columnHandle;
        }

        public Optional<Class<? extends ConnectorSplit>> getSplitClass()
        {
            return split;
        }

        public Optional<Class<? extends ConnectorIndexHandle>> getIndexHandleClass()
        {
            return indexHandle;
        }

        public Optional<Class<? extends ConnectorOutputTableHandle>> getOutputTableHandleClass()
        {
            return outputTableHandle;
        }

        public Optional<Class<? extends ConnectorInsertTableHandle>> getInsertTableHandleClass()
        {
            return insertTableHandle;
        }

        public Optional<Class<? extends ConnectorDeleteTableHandle>> getDeleteTableHandleClass()
        {
            return deleteTableHandle;
        }

        public Optional<Class<? extends ConnectorPartitioningHandle>> getPartitioningHandleClass()
        {
            return partitioningHandle;
        }

        public Optional<Class<? extends ConnectorTransactionHandle>> getTransactionHandleClass()
        {
            return transactionHandle;
        }

        public Optional<Class<? extends ConnectorMetadataUpdateHandle>> getMetadataUpdateHandleClass()
        {
            return metadataUpdateHandle;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MaterializedHandleResolver that = (MaterializedHandleResolver) o;
            return Objects.equals(tableHandle, that.tableHandle) &&
                    Objects.equals(layoutHandle, that.layoutHandle) &&
                    Objects.equals(columnHandle, that.columnHandle) &&
                    Objects.equals(split, that.split) &&
                    Objects.equals(indexHandle, that.indexHandle) &&
                    Objects.equals(outputTableHandle, that.outputTableHandle) &&
                    Objects.equals(insertTableHandle, that.insertTableHandle) &&
                    Objects.equals(deleteTableHandle, that.deleteTableHandle) &&
                    Objects.equals(partitioningHandle, that.partitioningHandle) &&
                    Objects.equals(transactionHandle, that.transactionHandle) &&
                    Objects.equals(metadataUpdateHandle, that.metadataUpdateHandle);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableHandle, layoutHandle, columnHandle, split, indexHandle, outputTableHandle, insertTableHandle, deleteTableHandle, partitioningHandle, transactionHandle, metadataUpdateHandle);
        }
    }

    private static class MaterializedFunctionHandleResolver
    {
        private final Optional<Class<? extends FunctionHandle>> functionHandle;

        public MaterializedFunctionHandleResolver(FunctionHandleResolver resolver)
        {
            functionHandle = getHandleClass(resolver::getFunctionHandleClass);
        }

        private static <T> Optional<Class<? extends T>> getHandleClass(Supplier<Class<? extends T>> callable)
        {
            try {
                return Optional.of(callable.get());
            }
            catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        }

        public Optional<Class<? extends FunctionHandle>> getFunctionHandleClass()
        {
            return functionHandle;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MaterializedFunctionHandleResolver that = (MaterializedFunctionHandleResolver) o;
            return Objects.equals(functionHandle, that.functionHandle);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionHandle);
        }
    }

    private static class MaterializedTableFunctionHandleResolver
    {
        private final Set<Class<? extends ConnectorTableFunctionHandle>> tableFunctionHandles;

        public MaterializedTableFunctionHandleResolver(TableFunctionHandleResolver resolver)
        {
            tableFunctionHandles = getHandleClass(resolver::getTableFunctionHandleClasses);
        }

        private static Set<Class<? extends ConnectorTableFunctionHandle>> getHandleClass(Supplier<Set<Class<? extends ConnectorTableFunctionHandle>>> callable)
        {
            try {
                return callable.get();
            }
            catch (UnsupportedOperationException e) {
                return ImmutableSet.of();
            }
        }

        public Set<Class<? extends ConnectorTableFunctionHandle>> getTableFunctionHandleClasses()
        {
            return tableFunctionHandles;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MaterializedTableFunctionHandleResolver that = (MaterializedTableFunctionHandleResolver) o;
            return Objects.equals(tableFunctionHandles, that.tableFunctionHandles);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableFunctionHandles);
        }
    }

    private static class MaterializedTableFunctionSplitResolver
    {
        private final Set<Class<? extends ConnectorSplit>> tableFunctionSplits;

        public MaterializedTableFunctionSplitResolver(TableFunctionSplitResolver resolver)
        {
            tableFunctionSplits = getSplitClass(resolver::getTableFunctionSplitClasses);
        }

        private static Set<Class<? extends ConnectorSplit>> getSplitClass(Supplier<Set<Class<? extends ConnectorSplit>>> callable)
        {
            try {
                return callable.get();
            }
            catch (UnsupportedOperationException e) {
                return ImmutableSet.of();
            }
        }

        public Set<Class<? extends ConnectorSplit>> getTableFunctionSplitClasses()
        {
            return tableFunctionSplits;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MaterializedTableFunctionSplitResolver that = (MaterializedTableFunctionSplitResolver) o;
            return Objects.equals(tableFunctionSplits, that.tableFunctionSplits);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableFunctionSplits);
        }
    }
}
