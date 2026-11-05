/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.table;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.Nullable;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.sink.common.OdpsWriteOperatorFactory;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.datastream.MaxComputeSink;
import org.apache.flink.odps.sink.datastream.RowDataArrowWriterFactory;
import org.apache.flink.odps.sink.partition.BucketIndexPartitioner;
import org.apache.flink.odps.sink.partition.PartitionSelector;
import org.apache.flink.odps.sink.partition.PrimaryKeySelector;
import org.apache.flink.odps.sink.shuffle.BucketOnlyKeyExtractor;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKeyExtractor;
import org.apache.flink.odps.sink.shuffle.PartitionBucketPartitioner;
import org.apache.flink.odps.sink.shuffle.PartitionBucketStatisticsOperatorFactory;
import org.apache.flink.odps.sink.shuffle.RegistryAssignmentSource;
import org.apache.flink.odps.sink.shuffle.StaticHashAssignmentSource;
import org.apache.flink.odps.sink.utils.Pipelines;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OdpsDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsDynamicTableSink.class);

    private final OdpsConf odpsConf;
    private final ReadableConfig flinkConf;
    private final OdpsTablePath identifier;
    private final TableSchema tableSchema;
    private final List<String> partitionKeys;
    private final WriteOperationType writeOperationType;
    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();

    private boolean dynamicGrouping = false;
    // TODO: support overwrite
    private boolean overwrite = false;

    @Nullable
    private final Integer configuredParallelism;

    public OdpsDynamicTableSink(
            ReadableConfig flinkConf,
            OdpsConf odpsConf,
            OdpsTablePath identifier,
            TableSchema schema,
            List<String> partitionKeys,
            WriteOperationType writeOperation,
            @Nullable Integer configuredParallelism) {
        this.flinkConf = flinkConf;
        this.odpsConf = odpsConf;
        this.identifier = identifier;
        this.tableSchema = schema;
        this.partitionKeys = partitionKeys;
        this.writeOperationType = writeOperation;
        this.configuredParallelism = configuredParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        if (writeOperationType.equals(WriteOperationType.UPSERT)) {
            return ChangelogMode.upsert();
        } else {
            return ChangelogMode.insertOnly();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (!flinkConf.get(OdpsOptions.SINK_COORDINATOR_ENABLE)
                && flinkConf.get(OdpsOptions.UPSERT_PARTIAL_COLUMN_ENABLE)) {
            throw new IllegalArgumentException("Partial-column writes require sink.coordinator.enable=true; "
                    + "the Storage streaming writer does not implement partial-column mutations.");
        }

        DataStructureConverter converter =
                context.createDataStructureConverter(tableSchema.toRowDataType());
        return (DataStreamSinkProviderAdapter)
                (dataStream) -> consume(dataStream, context.isBounded(), converter);
    }

    public interface DataStreamSinkProviderAdapter extends DataStreamSinkProvider {
        DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream);

        @Override
        default DataStreamSink<?> consumeDataStream(ProviderContext providerContext, DataStream<RowData> dataStream) {
            return consumeDataStream(dataStream);
        }
    }

    private DataStreamSink<?> consume(
            DataStream<RowData> dataStream, boolean isBounded, DataStructureConverter converter) {
        final int parallelism =
                Optional.ofNullable(configuredParallelism).orElse(dataStream.getParallelism());

        if (overwrite) {
            throw new IllegalStateException("Not support overwrite.");
        }
        RowType flinkSchema = (RowType)tableSchema.toPhysicalRowDataType().getLogicalType();
        Odps odps = OdpsUtils.getOdps(odpsConf);
        Table table = odps.tables().get(identifier.getProjectName(), identifier.getSchemaName(),
                                        identifier.getTableName());
        com.aliyun.odps.TableSchema odpsSchema = table.getSchema();

        if (staticPartitionSpec.isEmpty()
            && flinkConf.getOptional(OdpsOptions.SINK_PARTITION).isPresent()) {
            String partition = flinkConf.get(OdpsOptions.SINK_PARTITION);
            if (!StringUtils.isNullOrWhitespaceOnly(partition))  {
                PartitionSpec spec = new PartitionSpec(partition);
                staticPartitionSpec = OdpsUtils.getPartitionSpecKVMap(spec);
            }
        }

        OdpsUtils.validatePartitionSpec(staticPartitionSpec, partitionKeys);

        boolean dynamicPartition = partitionKeys != null && partitionKeys.size() != 0
                && partitionKeys.size() > staticPartitionSpec.size();
        boolean enableCoordinator = flinkConf.get(OdpsOptions.SINK_COORDINATOR_ENABLE);

        if (!enableCoordinator) {

            if (dynamicPartition) {
                List<String> dynamicNames = partitionKeys.stream()
                        .filter(name -> !staticPartitionSpec.containsKey(name))
                        .collect(java.util.stream.Collectors.toList());
                dataStream = dataStream.map(new org.apache.flink.odps.sink.partition.DefaultPartitionValueMapper(
                                flinkSchema, dynamicNames, flinkConf.get(OdpsOptions.PARTITION_DEFAULT_VALUE)))
                        .returns(org.apache.flink.table.runtime.typeutils.InternalTypeInfo.of(flinkSchema));
            }

            TableIdentifier tableIdentifier = TableIdentifier.of(identifier.getProjectName(),
                                                                 identifier.getSchemaName() == null ? "default" : identifier.getSchemaName(),
                                                                 identifier.getTableName());
            String staticPartitionStr = OdpsUtils.generatePartition(staticPartitionSpec);

            List<String> primaryKeys = table.getPrimaryKey();

            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                int bucketNum = (int) table.getClusterInfo().getBucketNum();
                List<Column> odpsColumns = table.getSchema().getColumns();
                boolean bucketOnlyShuffle = flinkConf.get(OdpsOptions.SINK_SHUFFLE_BUCKET_ONLY_ENABLE);

                // Bucket-only shuffle: skips partition spec extraction, routes by bucketId alone.
                // For performance A/B testing only — does NOT guarantee PK correctness on
                // partitioned tables (same bucketId in different partitions may be split).
                if (bucketOnlyShuffle) {
                    Sink<RowData> pkSink = new MaxComputeSink<>(
                            (Configuration) flinkConf, odpsConf, tableIdentifier, null,
                            new RowDataArrowWriterFactory(), flinkSchema);
                    BucketOnlyKeyExtractor bucketKeyExtractor = new BucketOnlyKeyExtractor(
                            odpsColumns, primaryKeys, bucketNum, flinkSchema);
                    return dataStream
                            .partitionCustom(new BucketIndexPartitioner(), bucketKeyExtractor)
                            .sinkTo(pkSink)
                            .setParallelism(parallelism)
                            .name("MaxCompute Bucket-Only Upsert Sink");
                }

                // Partition-column info only needed for (PARTITION, BUCKET) shuffle
                List<Column> partitionColumns = odpsSchema.getPartitionColumns();
                List<String> partitionColumnNames = partitionColumns.stream()
                        .map(Column::getName)
                        .collect(java.util.stream.Collectors.toList());

                // Reuse MaxComputeSink without a static partition: the per-subtask
                // TableWriteSession is built at table-level, the Arrow VSR carries the
                // partition columns, and the server demuxes rows to per-partition files.
                // One stream per sink subtask covers all partitions that subtask owns.
                Sink<RowData> pkSink = new MaxComputeSink<>(
                        (Configuration) flinkConf, odpsConf, tableIdentifier, null,
                        new RowDataArrowWriterFactory(), flinkSchema);

                PartitionBucketKeyExtractor keyExtractor = new PartitionBucketKeyExtractor(
                        odpsColumns, primaryKeys, partitionColumnNames, bucketNum,
                        staticPartitionSpec,
                        flinkConf.get(OdpsOptions.PARTITION_DEFAULT_VALUE),
                        flinkSchema);

                // PK tables always shuffle by (partitionSpec, bucketId). The
                // sink.dynamic-shuffle.enable switch only decides whether the stats
                // operator + coordinator are installed (RegistryAssignmentSource) or
                // routing uses a stateless hash (StaticHashAssignmentSource).
                boolean dynamicShuffleEnabled = flinkConf.get(OdpsOptions.SINK_DYNAMIC_SHUFFLE_ENABLE);

                if (dynamicShuffleEnabled && dynamicPartition) {
                    // Traffic-aware mode: install stats operator + coordinator;
                    // partitioner reads the broadcast assignment from the registry.
                    // Topology:
                    //   Source → [StatisticsOperator] → partitionCustom(PartitionBucketPartitioner) → Sink
                    //                ↕ OperatorEvent
                    //           [ShuffleCoordinator]
                    String operatorUid = "maxcompute-dynamic-shuffle-" + identifier.getTableName()
                            + "-" + UUID.randomUUID().toString().substring(0, 8);

                    // Two parallelisms passed separately:
                    //   - statsParallelism sizes the coordinator's gateway[] (one slot per
                    //     stats subtask). Must equal the parallelism set on the stats vertex.
                    //   - sinkParallelism is the assignment value range fed to AssignmentComputer.
                    //     The downstream partitionCustom only routes to 0..sinkParallelism-1; any
                    //     subtask index assigned beyond that would silently fall back to static
                    //     routing (and split PK groups across sink subtasks).
                    // These can differ when configuredParallelism (e.g. table.exec.sink.parallelism)
                    // is set independently of the upstream stream parallelism.
                    int statsParallelism = dataStream.getParallelism();
                    PartitionBucketStatisticsOperatorFactory statsOpFactory =
                            new PartitionBucketStatisticsOperatorFactory(
                                    keyExtractor, operatorUid, statsParallelism, parallelism);

                    // Force a new chain so the stats operator runs in its own JobVertex
                    // instead of being chained into the upstream source. Stats op is a
                    // CoordinatedOperator and so is most modern sources (FLIP-27 with
                    // SourceCoordinator); chaining the two puts two OperatorCoordinators
                    // + their event valves into the same task, which has hit first-checkpoint
                    // deadlocks where the chained vertex never acks barrier 1 even though the
                    // downstream sink (separate vertex) acks fine. Independent vertices also
                    // give per-operator backpressure / snapshot-duration metrics and keep
                    // stats observability from being hidden inside the source task.
                    DataStream<RowData> withStats = dataStream
                            .transform("MaxCompute Dynamic Shuffle Statistics",
                                    TypeInformation.of(RowData.class),
                                    statsOpFactory)
                            .setParallelism(statsParallelism)
                            .startNewChain();

                    return withStats
                            .partitionCustom(
                                    new PartitionBucketPartitioner(new RegistryAssignmentSource(operatorUid)),
                                    keyExtractor)
                            .sinkTo(pkSink)
                            .setParallelism(parallelism)
                            .name("MaxCompute Dynamic Shuffle Upsert Sink");
                }

                // Static mode: stateless (partition, bucket) hash. No stats operator,
                // no coordinator.
                return dataStream
                        .partitionCustom(
                                new PartitionBucketPartitioner(new StaticHashAssignmentSource()),
                                keyExtractor)
                        .sinkTo(pkSink)
                        .setParallelism(parallelism)
                        .name("MaxCompute Upsert Sink");
            }

            // Non-PK append path.
            Sink<RowData> appendSink = new MaxComputeSink<>(
                    (Configuration) flinkConf, odpsConf, tableIdentifier, staticPartitionStr,
                    new RowDataArrowWriterFactory(), flinkSchema);
            return dataStream.sinkTo(appendSink)
                    .setParallelism(parallelism)
                    .name("MaxCompute Append Sink");
        }

        boolean groupPartition = flinkConf.get(OdpsOptions.SINK_GROUP_PARTITION);

        boolean fileCached = flinkConf.get(OdpsOptions.SINK_FILE_CACHED_ENABLE);
        boolean standalone = flinkConf.get(OdpsOptions.SINK_STANDALONE_ENABLE);
        
        // Auto-enable standalone mode when streaming tunnel is enabled for better performance
        boolean useStreamingTunnel = flinkConf.get(OdpsOptions.INSERT_STREAMING_TUNNEL_ENABLE);
        if (useStreamingTunnel && !standalone) {
            LOG.info("Auto-enabling standalone mode for streaming tunnel optimization");
            standalone = true;
        }

        OdpsWriteOperatorFactory.OdpsWriteOperatorFactoryBuilder builder =
                new OdpsWriteOperatorFactory.OdpsWriteOperatorFactoryBuilder(identifier.getProjectName(), identifier.getTableName());

        builder.setConf((Configuration) flinkConf);
        builder.setOdpsConf(odpsConf);
        builder.setTableSchema(odpsSchema);

        DataStream<RowData> input = dataStream;
        if (writeOperationType.equals(WriteOperationType.UPSERT)
                && !groupPartition) {
            List<String> primaryKeys = table.getClusterInfo().getClusterCols();
            int bucketNum = (int)table.getClusterInfo().getBucketNum();
            List<Column> odpsColumns = table.getSchema().getColumns();
            PrimaryKeySelector keySelector = new PrimaryKeySelector(odpsColumns, primaryKeys, bucketNum, flinkSchema);
            input = input.partitionCustom(new BucketIndexPartitioner(), keySelector);
        }


        if (dynamicPartition && groupPartition) {
            List<Column> odpsColumns = odpsSchema.getColumns();
            odpsColumns.addAll(odpsSchema.getPartitionColumns());
            PartitionSelector keySelector = new PartitionSelector(odpsColumns,
                    odpsSchema.getPartitionColumns().stream().map(Column::getName).collect(
                            java.util.stream.Collectors.toList()),
                    OdpsUtils.generatePartition(staticPartitionSpec),
                    flinkConf.get(OdpsOptions.PARTITION_DEFAULT_VALUE),
                    flinkSchema);
            Partitioner<String> partitioner = (key, channels) -> KeyGroupRangeAssignment.assignKeyToParallelOperator(key,
                    KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism), channels);
            input = input.partitionCustom(partitioner, keySelector);
        }

        if (!staticPartitionSpec.isEmpty()) {
            builder.setPartition(OdpsUtils.generatePartition(staticPartitionSpec));
        }

        builder.setDynamicPartition(dynamicPartition);
        builder.setSchemaName(identifier.getSchemaName());
        builder.setWriteOperationType(writeOperationType);
        builder.setGroupPartition(groupPartition);
        builder.setFileCached(fileCached);
        builder.setStandAlone(standalone);
        builder.setWriteOptions(OdpsWriteOptions.builder().build());
        builder.setRowType(flinkSchema);

        OdpsWriteOperatorFactory operatorFactory = builder.build();

        DataStream<Object> out = input.transform(  "stream_write: " +  identifier.getTableName(),
                TypeInformation.of(Object.class), operatorFactory)
                .setParallelism(parallelism);

        return out.addSink(Pipelines.DummySink0.INSTANCE)
                .setParallelism(1)
                .name("Odps sink");
    }

    @Override
    public DynamicTableSink copy() {
        OdpsDynamicTableSink sink =
                new OdpsDynamicTableSink(
                        flinkConf,
                        odpsConf,
                        identifier,
                        tableSchema,
                        partitionKeys,
                        writeOperationType,
                        configuredParallelism);
        sink.staticPartitionSpec = staticPartitionSpec;
        sink.overwrite = overwrite;
        sink.dynamicGrouping = dynamicGrouping;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "OdpsSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // make it a LinkedHashMap to maintain partition column order
        if (partitionKeys != null
                && partitionKeys.size() != 0) {
            staticPartitionSpec = new LinkedHashMap<>();
            for (String partitionCol : partitionKeys) {
                if (partition.containsKey(partitionCol)) {
                    staticPartitionSpec.put(partitionCol, partition.get(partitionCol));
                }
            }
        } else {
            throw new UnsupportedOperationException(
                    "Should not apply partitions to a non-partitioned table.");
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return supportsGrouping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OdpsDynamicTableSink)) {
            return false;
        }
        OdpsDynamicTableSink that = (OdpsDynamicTableSink) o;
        return Objects.equals(odpsConf, that.odpsConf)
                && Objects.equals(flinkConf, that.flinkConf)
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(staticPartitionSpec, that.staticPartitionSpec)
                && overwrite == that.overwrite
                && dynamicGrouping == that.dynamicGrouping
                && Objects.equals(configuredParallelism, that.configuredParallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                odpsConf,
                flinkConf,
                identifier,
                tableSchema,
                partitionKeys,
                staticPartitionSpec,
                overwrite,
                dynamicGrouping,
                configuredParallelism);
    }
}
