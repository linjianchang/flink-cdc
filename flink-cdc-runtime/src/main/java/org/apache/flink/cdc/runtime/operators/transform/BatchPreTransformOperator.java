/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.operators.transform.exceptions.TransformException;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A data process function that filters out columns which aren't (directly & indirectly) referenced
 * in batch mode. There is no need to consider the task failover scenario in batch mode, as it
 * should be re - emitted from the source.
 */
public class BatchPreTransformOperator extends AbstractStreamOperator<Event>
        implements OneInputStreamOperator<Event, Event>, Serializable {

    private static final long serialVersionUID = 1L;

    private final List<TransformRule> transformRules;
    private transient List<PreTransformer> transforms;
    private final Map<TableId, PreTransformChangeInfo> preTransformChangeInfoMap;
    private final List<Tuple2<Selectors, SchemaMetadataTransform>> schemaMetadataTransformers;
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions;
    private List<UserDefinedFunctionDescriptor> udfDescriptors;
    private Map<TableId, PreTransformProcessor> preTransformProcessorMap;
    private Map<TableId, Boolean> hasAsteriskMap;

    public static BatchPreTransformOperator.Builder newBuilder() {
        return new BatchPreTransformOperator.Builder();
    }

    /** Builder of {@link BatchPreTransformOperator}. */
    public static class Builder {
        private final List<TransformRule> transformRules = new ArrayList<>();

        private final List<Tuple3<String, String, Map<String, String>>> udfFunctions =
                new ArrayList<>();

        public BatchPreTransformOperator.Builder addTransform(
                String tableInclusions, @Nullable String projection, @Nullable String filter) {
            transformRules.add(
                    new TransformRule(
                            tableInclusions,
                            projection,
                            filter,
                            "",
                            "",
                            "",
                            null,
                            new SupportedMetadataColumn[0]));
            return this;
        }

        public BatchPreTransformOperator.Builder addTransform(
                String tableInclusions,
                @Nullable String projection,
                @Nullable String filter,
                String primaryKey,
                String partitionKey,
                String tableOption,
                @Nullable String postTransformConverter,
                SupportedMetadataColumn[] supportedMetadataColumns) {
            transformRules.add(
                    new TransformRule(
                            tableInclusions,
                            projection,
                            filter,
                            primaryKey,
                            partitionKey,
                            tableOption,
                            postTransformConverter,
                            supportedMetadataColumns));
            return this;
        }

        public BatchPreTransformOperator.Builder addUdfFunctions(
                List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
            this.udfFunctions.addAll(udfFunctions);
            return this;
        }

        public BatchPreTransformOperator build() {
            return new BatchPreTransformOperator(transformRules, udfFunctions);
        }
    }

    private BatchPreTransformOperator(
            List<TransformRule> transformRules,
            List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
        this.preTransformChangeInfoMap = new ConcurrentHashMap<>();
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.schemaMetadataTransformers = new ArrayList<>();
        this.chainingStrategy = ChainingStrategy.ALWAYS;

        this.transformRules = transformRules;
        this.udfFunctions = udfFunctions;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Event>> output) {
        super.setup(containingTask, config, output);
        this.udfDescriptors =
                this.udfFunctions.stream()
                        .map(udf -> new UserDefinedFunctionDescriptor(udf.f0, udf.f1, udf.f2))
                        .collect(Collectors.toList());

        // Initialize data fields in advance because they might be accessed in
        // `::initializeState` function when restoring from a previous state.
        this.transforms = new ArrayList<>();
        for (TransformRule transformRule : transformRules) {
            String tableInclusions = transformRule.getTableInclusions();
            String projection = transformRule.getProjection();
            String filter = transformRule.getFilter();
            String primaryKeys = transformRule.getPrimaryKey();
            String partitionKeys = transformRule.getPartitionKey();
            String tableOptions = transformRule.getTableOption();
            Selectors selectors =
                    new Selectors.SelectorsBuilder().includeTables(tableInclusions).build();
            transforms.add(
                    new PreTransformer(
                            selectors,
                            TransformProjection.of(projection).orElse(null),
                            TransformFilter.of(filter, udfDescriptors).orElse(null)));
            schemaMetadataTransformers.add(
                    new Tuple2<>(
                            selectors,
                            new SchemaMetadataTransform(primaryKeys, partitionKeys, tableOptions)));
        }
        this.preTransformProcessorMap = new ConcurrentHashMap<>();
        this.hasAsteriskMap = new ConcurrentHashMap<>();
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        clearOperator();
    }

    @Override
    public void close() throws Exception {
        super.close();
        clearOperator();
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        try {
            processEvent(event);
        } catch (Exception e) {
            TableId tableId = null;
            Schema schemaBefore = null;
            Schema schemaAfter = null;

            if (event instanceof ChangeEvent) {
                tableId = ((ChangeEvent) event).tableId();
                PreTransformChangeInfo info = preTransformChangeInfoMap.get(tableId);
                if (info != null) {
                    schemaBefore = info.getSourceSchema();
                    schemaAfter = info.getPreTransformedSchema();
                }
            }

            throw new TransformException(
                    "batch-pre-transform", event, tableId, schemaBefore, schemaAfter, e);
        }
    }

    private void processEvent(Event event) {
        if (event instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) event;
            preTransformProcessorMap.remove(createTableEvent.tableId());
            output.collect(new StreamRecord<>(cacheCreateTable(createTableEvent)));
        } else if (event instanceof DataChangeEvent) {
            output.collect(new StreamRecord<>(processDataChangeEvent(((DataChangeEvent) event))));
        }
    }

    private SchemaChangeEvent cacheCreateTable(CreateTableEvent event) {
        TableId tableId = event.tableId();
        Schema originalSchema = event.getSchema();
        event = transformCreateTableEvent(event);
        Schema newSchema = (event).getSchema();
        preTransformChangeInfoMap.put(
                tableId, PreTransformChangeInfo.of(tableId, originalSchema, newSchema));
        return event;
    }

    private CreateTableEvent transformCreateTableEvent(CreateTableEvent createTableEvent) {
        TableId tableId = createTableEvent.tableId();
        for (Tuple2<Selectors, SchemaMetadataTransform> transform : schemaMetadataTransformers) {
            Selectors selectors = transform.f0;
            if (selectors.isMatch(tableId)) {
                createTableEvent =
                        new CreateTableEvent(
                                tableId,
                                transformSchemaMetaData(
                                        createTableEvent.getSchema(), transform.f1));
            }
        }

        cachePreTransformProcessor(tableId, createTableEvent.getSchema());
        if (preTransformProcessorMap.containsKey(tableId)) {
            return preTransformProcessorMap
                    .get(tableId)
                    .preTransformCreateTableEvent(createTableEvent);
        }
        return createTableEvent;
    }

    private void cachePreTransformProcessor(TableId tableId, Schema tableSchema) {
        LinkedHashSet<Column> referencedColumnsSet = new LinkedHashSet<>();
        boolean hasMatchTransform = false;
        for (PreTransformer transform : transforms) {
            if (!transform.getSelectors().isMatch(tableId)) {
                continue;
            }
            processProjectionTransform(tableId, tableSchema, referencedColumnsSet, transform);
            hasMatchTransform = true;
        }
        if (!hasMatchTransform) {
            processProjectionTransform(tableId, tableSchema, referencedColumnsSet, null);
        }
    }

    public void processProjectionTransform(
            TableId tableId,
            Schema tableSchema,
            LinkedHashSet<Column> referencedColumnsSet,
            @Nullable PreTransformer transform) {
        // If this TableId isn't presented in any transform block, it should behave like a "*"
        // projection and should be regarded as asterisk-ful.
        if (transform == null) {
            referencedColumnsSet.addAll(tableSchema.getColumns());
            hasAsteriskMap.put(tableId, true);
        } else {
            TransformProjection transformProjection = transform.getProjection().get();
            boolean hasAsterisk = TransformParser.hasAsterisk(transformProjection.getProjection());
            if (hasAsterisk) {
                referencedColumnsSet.addAll(tableSchema.getColumns());
                hasAsteriskMap.put(tableId, true);
            } else {
                TransformFilter transformFilter = transform.getFilter().orElse(null);
                List<Column> referencedColumns =
                        TransformParser.generateReferencedColumns(
                                transformProjection.getProjection(),
                                transformFilter != null ? transformFilter.getExpression() : null,
                                tableSchema.getColumns());
                // update referenced columns of other projections of the same tableId, if any
                referencedColumnsSet.addAll(referencedColumns);
                hasAsteriskMap.putIfAbsent(tableId, false);
            }
        }

        PreTransformChangeInfo tableChangeInfo =
                PreTransformChangeInfo.of(
                        tableId,
                        tableSchema,
                        tableSchema.copy(new ArrayList<>(referencedColumnsSet)));
        preTransformProcessorMap.put(tableId, new PreTransformProcessor(tableChangeInfo));
    }

    private Schema transformSchemaMetaData(
            Schema schema, SchemaMetadataTransform schemaMetadataTransform) {
        Schema.Builder schemaBuilder = Schema.newBuilder().setColumns(schema.getColumns());
        if (!schemaMetadataTransform.getPrimaryKeys().isEmpty()) {
            schemaBuilder.primaryKey(schemaMetadataTransform.getPrimaryKeys());
        } else {
            schemaBuilder.primaryKey(schema.primaryKeys());
        }
        if (!schemaMetadataTransform.getPartitionKeys().isEmpty()) {
            schemaBuilder.partitionKey(schemaMetadataTransform.getPartitionKeys());
        } else {
            schemaBuilder.partitionKey(schema.partitionKeys());
        }
        if (!schemaMetadataTransform.getOptions().isEmpty()) {
            schemaBuilder.options(schemaMetadataTransform.getOptions());
        } else {
            schemaBuilder.options(schema.options());
        }
        return schemaBuilder.build();
    }

    private DataChangeEvent processDataChangeEvent(DataChangeEvent dataChangeEvent) {
        if (!transforms.isEmpty()) {
            TableId tableId = dataChangeEvent.tableId();
            PreTransformProcessor processor = preTransformProcessorMap.get(tableId);
            Preconditions.checkArgument(
                    processor != null,
                    "Transform operator receives a data change event from table %s without a full schema view. "
                            + "This might happen if source with distributed tables doesn't emit CreateTableEvent first after fail-over. "
                            + "This is likely a bug, please consider filing an issue.",
                    tableId);

            BinaryRecordData before = (BinaryRecordData) dataChangeEvent.before();
            BinaryRecordData after = (BinaryRecordData) dataChangeEvent.after();
            if (before != null) {
                BinaryRecordData projectedBefore = processor.processFillDataField(before);
                dataChangeEvent = DataChangeEvent.projectBefore(dataChangeEvent, projectedBefore);
            }
            if (after != null) {
                BinaryRecordData projectedAfter = processor.processFillDataField(after);
                dataChangeEvent = DataChangeEvent.projectAfter(dataChangeEvent, projectedAfter);
            }
        }
        return dataChangeEvent;
    }

    private void clearOperator() {
        this.transforms = null;
        this.preTransformProcessorMap = null;
    }
}
