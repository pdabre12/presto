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
package com.facebook.presto.connector.tvf;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.DescriptorArgumentSpecification;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableArgument;
import com.facebook.presto.spi.function.table.TableArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.facebook.presto.spi.function.table.TableFunctionProcessor;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.DescribedTable;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static com.facebook.presto.spi.function.table.ReturnTypeSpecification.OnlyPassThrough.ONLY_PASS_THROUGH;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.usedInput;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class TestingTableFunctions
{
    private static final String SCHEMA_NAME = "system";
    private static final String TABLE_NAME = "table";
    private static final String COLUMN_NAME = "column";
    private static final ConnectorTableFunctionHandle HANDLE = new TestingTableFunctionHandle();
    private static final TableFunctionAnalysis ANALYSIS = TableFunctionAnalysis.builder()
            .handle(HANDLE)
            .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
            .build();
    private static final TableFunctionAnalysis NO_DESCRIPTOR_ANALYSIS = TableFunctionAnalysis.builder()
            .handle(HANDLE)
            .requiredColumns("INPUT", ImmutableList.of(0))
            .build();

    /**
     * A table function returning a table with single empty column of type BOOLEAN.
     * The argument `COLUMN` is the column name.
     * The argument `IGNORED` is ignored.
     * Both arguments are optional.
     */
    public static class SimpleTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String SCHEMA_NAME = "system";
        private static final String FUNCTION_NAME = "simple_table_function";
        private static final String TABLE_NAME = "simple_table";

        public SimpleTableFunction()
        {
            super(
                    SCHEMA_NAME,
                    FUNCTION_NAME,
                    Arrays.asList(
                            ScalarArgumentSpecification.builder()
                                    .name("COLUMN")
                                    .type(VARCHAR)
                                    .defaultValue(utf8Slice("col"))
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("IGNORED")
                                    .type(BIGINT)
                                    .defaultValue(0L)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) arguments.get("COLUMN");
            String columnName = ((Slice) argument.getValue()).toStringUtf8();

            return TableFunctionAnalysis.builder()
                    .handle(new SimpleTableFunctionHandle(getSchema(), TABLE_NAME, columnName))
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(columnName, Optional.of(BOOLEAN)))))
                    .build();
        }

        public static class SimpleTableFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final MockConnectorTableHandle tableHandle;

            public SimpleTableFunctionHandle(String schema, String table, String column)
            {
                this.tableHandle = new MockConnectorTableHandle(
                        new SchemaTableName(schema, table),
                        TupleDomain.all(),
                        Optional.of(ImmutableList.of(new MockConnectorColumnHandle(column, BOOLEAN))));
            }

            public MockConnectorTableHandle getTableHandle()
            {
                return tableHandle;
            }
        }
    }

    public static class TwoScalarArgumentsFunction
            extends AbstractConnectorTableFunction
    {
        public TwoScalarArgumentsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "two_arguments_function",
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("TEXT")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("NUMBER")
                                    .type(BIGINT)
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return ANALYSIS;
        }
    }

    public static class TableArgumentFunction
            extends AbstractConnectorTableFunction
    {
        public TableArgumentFunction()
        {
            super(
                    SCHEMA_NAME,
                    "table_argument_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0))
                    .build();
        }
    }

    public static class TableArgumentRowSemanticsFunction
            extends AbstractConnectorTableFunction
    {
        public TableArgumentRowSemanticsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "table_argument_row_semantics_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .rowSemantics()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0))
                    .build();
        }
    }

    public static class DescriptorArgumentFunction
            extends AbstractConnectorTableFunction
    {
        public DescriptorArgumentFunction()
        {
            super(
                    SCHEMA_NAME,
                    "descriptor_argument_function",
                    ImmutableList.of(
                            DescriptorArgumentSpecification.builder()
                                    .name("SCHEMA")
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return ANALYSIS;
        }
    }

    public static class TwoTableArgumentsFunction
            extends AbstractConnectorTableFunction
    {
        public TwoTableArgumentsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "two_table_arguments_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT1")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT2")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT1", ImmutableList.of(0))
                    .requiredColumns("INPUT2", ImmutableList.of(0))
                    .build();
        }
    }

    public static class OnlyPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public OnlyPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "only_pass_through_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class MonomorphicStaticReturnTypeFunction
            extends AbstractConnectorTableFunction
    {
        public MonomorphicStaticReturnTypeFunction()
        {
            super(
                    SCHEMA_NAME,
                    "monomorphic_static_return_type_function",
                    ImmutableList.of(),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(BOOLEAN, INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .build();
        }
    }

    public static class PolymorphicStaticReturnTypeFunction
            extends AbstractConnectorTableFunction
    {
        public PolymorphicStaticReturnTypeFunction()
        {
            super(
                    SCHEMA_NAME,
                    "polymorphic_static_return_type_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("a", "b"),
                            ImmutableList.of(BOOLEAN, INTEGER))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class PassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public PassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "pass_through_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .passThroughColumns()
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(Descriptor.descriptor(
                            ImmutableList.of("x"),
                            ImmutableList.of(BOOLEAN))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return NO_DESCRIPTOR_ANALYSIS;
        }
    }

    public static class DifferentArgumentTypesFunction
            extends AbstractConnectorTableFunction
    {
        public DifferentArgumentTypesFunction()
        {
            super(
                    SCHEMA_NAME,
                    "different_arguments_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT_1")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            DescriptorArgumentSpecification.builder()
                                    .name("LAYOUT")
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .rowSemantics()
                                    .passThroughColumns()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ID")
                                    .type(BIGINT)
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_3")
                                    .pruneWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field(COLUMN_NAME, Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT_1", ImmutableList.of(0))
                    .requiredColumns("INPUT_2", ImmutableList.of(0))
                    .requiredColumns("INPUT_3", ImmutableList.of(0))
                    .build();
        }
    }

    public static class RequiredColumnsFunction
            extends AbstractConnectorTableFunction
    {
        public RequiredColumnsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "required_columns_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(HANDLE)
                    .returnedType(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN)))))
                    .requiredColumns("INPUT", ImmutableList.of(0, 1))
                    .build();
        }
    }

    public static class TestingTableFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final MockConnectorTableHandle tableHandle;

        public TestingTableFunctionHandle()
        {
            this.tableHandle = new MockConnectorTableHandle(
                    new SchemaTableName(SCHEMA_NAME, TABLE_NAME),
                    TupleDomain.all(),
                    Optional.of(ImmutableList.of(new MockConnectorColumnHandle(COLUMN_NAME, BOOLEAN))));
        }

        public MockConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }

    // for testing execution by operator

    public static class IdentityFunction
            extends AbstractConnectorTableFunction
    {
        public IdentityFunction()
        {
            super(
                    SCHEMA_NAME,
                    "identity_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .keepWhenEmpty()
                                    .build()),
                    GENERIC_TABLE);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            List<RowType.Field> inputColumns = ((TableArgument) arguments.get("INPUT")).getRowType().getFields();
            Descriptor returnedType = new Descriptor(inputColumns.stream()
                    .map(field -> new Descriptor.Field(field.getName().orElse("anonymous_column"), Optional.of(field.getType())))
                    .collect(toImmutableList()));
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .returnedType(returnedType)
                    .requiredColumns("INPUT", IntStream.range(0, inputColumns.size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class IdentityFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return input -> {
                    if (input == null) {
                        return FINISHED;
                    }
                    Optional<Page> inputPage = getOnlyElement(input);
                    return inputPage.map(TableFunctionProcessorState.Processed::usedInputAndProduced).orElseThrow(NoSuchElementException::new);
                };
            }
        }
    }

    public static class IdentityPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public IdentityPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "identity_pass_through_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", ImmutableList.of(0)) // per spec, function must require at least one column
                    .build();
        }

        public static class IdentityPassThroughFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return new IdentityPassThroughFunctionProcessor();
            }
        }

        public static class IdentityPassThroughFunctionProcessor
                implements TableFunctionProcessor
        {
            private long processedPositions; // stateful

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    return FINISHED;
                }

                Page page = getOnlyElement(input).orElseThrow(NoSuchElementException::new);
                BlockBuilder builder = BIGINT.createBlockBuilder(null, page.getPositionCount());
                for (long index = processedPositions; index < processedPositions + page.getPositionCount(); index++) {
                    // TODO check for long overflow
                    builder.writeLong(index);
                }
                processedPositions = processedPositions + page.getPositionCount();
                return usedInputAndProduced(new Page(builder.build()));
            }
        }
    }

    public static class RepeatFunction
            extends AbstractConnectorTableFunction
    {
        public RepeatFunction()
        {
            super(
                    SCHEMA_NAME,
                    "repeat",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("N")
                                    .type(INTEGER)
                                    .defaultValue(2L)
                                    .build()),
                    ONLY_PASS_THROUGH);
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument count = (ScalarArgument) arguments.get("N");
            requireNonNull(count.getValue(), "count value for function repeat() is null");
            checkArgument((long) count.getValue() > 0, "count value for function repeat() must be positive");

            return TableFunctionAnalysis.builder()
                    .handle(new RepeatFunctionHandle((long) count.getValue()))
                    .requiredColumns("INPUT", ImmutableList.of(0)) // per spec, function must require at least one column
                    .build();
        }

        public static class RepeatFunctionHandle
                implements ConnectorTableFunctionHandle
        {
            private final long count;

            @JsonCreator
            public RepeatFunctionHandle(@JsonProperty("count") long count)
            {
                this.count = count;
            }

            @JsonProperty
            public long getCount()
            {
                return count;
            }
        }

        public static class RepeatFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return new RepeatFunctionProcessor(((RepeatFunctionHandle) handle).getCount());
            }
        }

        public static class RepeatFunctionProcessor
                implements TableFunctionProcessor
        {
            private final long count;

            // stateful
            private long processedPositions;
            private long processedRounds;
            private Block indexes;
            boolean usedData;

            public RepeatFunctionProcessor(long count)
            {
                this.count = count;
            }

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    if (processedRounds < count && indexes != null) {
                        processedRounds++;
                        return produced(new Page(indexes));
                    }
                    return FINISHED;
                }

                Page page = getOnlyElement(input).orElseThrow(NoSuchElementException::new);
                if (processedRounds == 0) {
                    BlockBuilder builder = BIGINT.createBlockBuilder(null, page.getPositionCount());
                    for (long index = processedPositions; index < processedPositions + page.getPositionCount(); index++) {
                        // TODO check for long overflow
                        builder.writeLong(index);
                    }
                    processedPositions = processedPositions + page.getPositionCount();
                    indexes = builder.build();
                    usedData = true;
                }
                else {
                    usedData = false;
                }
                processedRounds++;

                Page result = new Page(indexes);

                if (processedRounds == count) {
                    processedRounds = 0;
                    indexes = null;
                }

                if (usedData) {
                    return usedInputAndProduced(result);
                }
                return produced(result);
            }
        }
    }

    public static class EmptyOutputFunction
            extends AbstractConnectorTableFunction
    {
        public EmptyOutputFunction()
        {
            super(
                    SCHEMA_NAME,
                    "empty_output",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class EmptyOutputProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return new EmptyOutputProcessor();
            }
        }

        // returns an empty Page (one column, zero rows) for each Page of input
        private static class EmptyOutputProcessor
                implements TableFunctionProcessor
        {
            private static final Page EMPTY_PAGE = new Page(BOOLEAN.createBlockBuilder(null, 0).build());

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    return FINISHED;
                }
                return usedInputAndProduced(EMPTY_PAGE);
            }
        }
    }

    public static class EmptyOutputWithPassThroughFunction
            extends AbstractConnectorTableFunction
    {
        public EmptyOutputWithPassThroughFunction()
        {
            super(
                    SCHEMA_NAME,
                    "empty_output_with_pass_through",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .passThroughColumns()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("column", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class EmptyOutputWithPassThroughProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return new EmptyOutputWithPassThroughProcessor();
            }
        }

        // returns an empty Page (one proper column and pass-through, zero rows) for each Page of input
        private static class EmptyOutputWithPassThroughProcessor
                implements TableFunctionProcessor
        {
            // one proper channel, and one pass-through index channel
            private static final Page EMPTY_PAGE = new Page(
                    BOOLEAN.createBlockBuilder(null, 0).build(),
                    BIGINT.createBlockBuilder(null, 0).build());

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (input == null) {
                    return FINISHED;
                }
                return usedInputAndProduced(EMPTY_PAGE);
            }
        }
    }

    public static class TestInputsFunction
            extends AbstractConnectorTableFunction
    {
        public TestInputsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "test_inputs_function",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .rowSemantics()
                                    .name("INPUT_1")
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_3")
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_4")
                                    .keepWhenEmpty()
                                    .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("boolean_result", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT_1", IntStream.range(0, ((TableArgument) arguments.get("INPUT_1")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_2", IntStream.range(0, ((TableArgument) arguments.get("INPUT_2")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_3", IntStream.range(0, ((TableArgument) arguments.get("INPUT_3")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .requiredColumns("INPUT_4", IntStream.range(0, ((TableArgument) arguments.get("INPUT_4")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class TestInputsFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                BlockBuilder resultBuilder = BOOLEAN.createBlockBuilder(null, 1);
                BOOLEAN.writeBoolean(resultBuilder, true);

                Page result = new Page(resultBuilder.build());

                return input -> {
                    if (input == null) {
                        return FINISHED;
                    }
                    return usedInputAndProduced(result);
                };
            }
        }
    }

    public static class PassThroughInputFunction
            extends AbstractConnectorTableFunction
    {
        public PassThroughInputFunction()
        {
            super(
                    SCHEMA_NAME,
                    "pass_through",
                    ImmutableList.of(
                            TableArgumentSpecification.builder()
                                    .name("INPUT_1")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build(),
                            TableArgumentSpecification.builder()
                                    .name("INPUT_2")
                                    .passThroughColumns()
                                    .keepWhenEmpty()
                                    .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(
                            new Descriptor.Field("input_1_present", Optional.of(BOOLEAN)),
                            new Descriptor.Field("input_2_present", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT_1", ImmutableList.of(0))
                    .requiredColumns("INPUT_2", ImmutableList.of(0))
                    .build();
        }

        public static class PassThroughInputProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return new PassThroughInputProcessor();
            }
        }

        private static class PassThroughInputProcessor
                implements TableFunctionProcessor
        {
            private boolean input1Present;
            private boolean input2Present;
            private int input1EndIndex;
            private int input2EndIndex;
            private boolean finished;

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (finished) {
                    return FINISHED;
                }
                if (input == null) {
                    finished = true;

                    // proper column input_1_present
                    BlockBuilder input1Builder = BOOLEAN.createBlockBuilder(null, 1);
                    BOOLEAN.writeBoolean(input1Builder, input1Present);

                    // proper column input_2_present
                    BlockBuilder input2Builder = BOOLEAN.createBlockBuilder(null, 1);
                    BOOLEAN.writeBoolean(input2Builder, input2Present);

                    // pass-through index for input_1
                    BlockBuilder input1PassThroughBuilder = BIGINT.createBlockBuilder(null, 1);
                    if (input1Present) {
                        input1PassThroughBuilder.writeLong(input1EndIndex - 1);
                    }
                    else {
                        input1PassThroughBuilder.appendNull();
                    }

                    // pass-through index for input_2
                    BlockBuilder input2PassThroughBuilder = BIGINT.createBlockBuilder(null, 1);
                    if (input2Present) {
                        input2PassThroughBuilder.writeLong(input2EndIndex - 1);
                    }
                    else {
                        input2PassThroughBuilder.appendNull();
                    }

                    return produced(new Page(input1Builder.build(), input2Builder.build(), input1PassThroughBuilder.build(), input2PassThroughBuilder.build()));
                }
                input.get(0).ifPresent(page -> {
                    input1Present = true;
                    input1EndIndex += page.getPositionCount();
                });
                input.get(1).ifPresent(page -> {
                    input2Present = true;
                    input2EndIndex += page.getPositionCount();
                });
                return usedInput();
            }
        }
    }

    public static class TestInputFunction
            extends AbstractConnectorTableFunction
    {
        public TestInputFunction()
        {
            super(
                    SCHEMA_NAME,
                    "test_input",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .name("INPUT")
                            .keepWhenEmpty()
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("got_input", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class TestInputProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                return new TestInputProcessor();
            }
        }

        private static class TestInputProcessor
                implements TableFunctionProcessor
        {
            private boolean processorGotInput;
            private boolean finished;

            @Override
            public TableFunctionProcessorState process(List<Optional<Page>> input)
            {
                if (finished) {
                    return FINISHED;
                }
                if (input == null) {
                    finished = true;
                    BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 1);
                    BOOLEAN.writeBoolean(builder, processorGotInput);
                    return produced(new Page(builder.build()));
                }
                processorGotInput = true;
                return usedInput();
            }
        }
    }

    public static class TestSingleInputRowSemanticsFunction
            extends AbstractConnectorTableFunction
    {
        public TestSingleInputRowSemanticsFunction()
        {
            super(
                    SCHEMA_NAME,
                    "test_single_input_function",
                    ImmutableList.of(TableArgumentSpecification.builder()
                            .rowSemantics()
                            .name("INPUT")
                            .build()),
                    new DescribedTable(new Descriptor(ImmutableList.of(new Descriptor.Field("boolean_result", Optional.of(BOOLEAN))))));
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            return TableFunctionAnalysis.builder()
                    .handle(new EmptyTableFunctionHandle())
                    .requiredColumns("INPUT", IntStream.range(0, ((TableArgument) arguments.get("INPUT")).getRowType().getFields().size()).boxed().collect(toImmutableList()))
                    .build();
        }

        public static class TestSingleInputFunctionProcessorProvider
                implements TableFunctionProcessorProvider
        {
            @Override
            public TableFunctionProcessor get(ConnectorTableFunctionHandle handle)
            {
                BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 1);
                BOOLEAN.writeBoolean(builder, true);
                Page result = new Page(builder.build());

                return input -> {
                    if (input == null) {
                        return FINISHED;
                    }
                    return usedInputAndProduced(result);
                };
            }
        }
    }

    public static class EmptyTableFunctionHandle
            implements ConnectorTableFunctionHandle
    {
    }
}
