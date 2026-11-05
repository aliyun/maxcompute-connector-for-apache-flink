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

import org.apache.flink.odps.source.partition.LatestPartitionOptions;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.aliyun.odps.Table;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.source.*;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.Constraint;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.TableIdentifier;

import static org.apache.flink.odps.table.OdpsOptions.SCAN_PARTITION;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_PARTITION_LATEST;
import static org.apache.flink.odps.table.OdpsOptions.DYNAMIC_LOAD_BALANCING;
import static org.apache.flink.odps.table.OdpsOptions.COMPRESS_CODEC;
import static org.apache.flink.odps.table.OdpsOptions.SPLIT_BYTE_SIZE;
import static org.apache.flink.odps.table.OdpsOptions.SPLIT_STRATEGY;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_MODE;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_TIMESTAMP;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_SNAPSHOT_VERSION;
import static org.apache.flink.odps.table.OdpsOptions.SPLIT_BATCH_READ_SIZE;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_INCREMENTAL_MODE;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_MAX_SPLITS_PER_TASK;
import static org.apache.flink.odps.table.OdpsOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.flink.odps.table.OdpsOptions.INCREMENTAL_BETWEEN;
import static org.apache.flink.odps.table.OdpsOptions.INCREMENTAL_BETWEEN_TIMESTAMP;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_DISK_SPILL_ENABLED;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_DISK_SPILL_MEMORY_CAPACITY;
import static org.apache.flink.odps.table.OdpsOptions.SCAN_DISK_SPILL_MAX_BYTES;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class OdpsDynamicTableSource
        implements ScanTableSource,
        LookupTableSource,
        SupportsPartitionPushDown,
        SupportsProjectionPushDown,
        SupportsLimitPushDown,
        SupportsReadingMetadata,
        SupportsWatermarkPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsDynamicTableSource.class);

    private final OdpsConf odpsConf;
    private final OdpsLookupOptions lookupOptions;
    private final ReadableConfig flinkConf;
    private final TableIdentifier identifier;
    private final OdpsMetaDataProvider metaDataProvider;

    private TableSchema tableSchema;
    private final List<String> partitionKeys;
    private final ResolvedSchema resolvedSchema;
    private boolean streaming;

    // Remaining partition specs after partition pruning is performed. Null if pruning is not pushed
    // down.
    @Nullable
    private List<Map<String, String>> remainingPartitions = null;
    @Nullable
    protected int[] projectedFields = null;
    @Nullable
    private Long limit = null;

    private List<String> metadataKeys = Collections.emptyList();
    @Nullable
    private DataType producedDataType = null;
    @Nullable
    private WatermarkStrategy<RowData> watermarkStrategy = null;

    public OdpsDynamicTableSource(
            ReadableConfig flinkConf,
            OdpsConf odpsConf,
            OdpsLookupOptions lookupOptions,
            TableIdentifier identifier,
            TableSchema schema,
            List<String> partitionKeys,
            ResolvedSchema resolvedSchema,
            boolean isStreamingMode) {
        LOG.info("OdpsDynamicTableSource " + schema);
        this.flinkConf = flinkConf == null ? new Configuration() : flinkConf;
        this.odpsConf = Preconditions.checkNotNull(odpsConf, "odpsConf cannot be null");
        this.lookupOptions = lookupOptions;
        this.identifier = Preconditions.checkNotNull(identifier);
        this.tableSchema = schema;
        this.partitionKeys = partitionKeys;
        // TODO: Singleton
        this.metaDataProvider = new OdpsMetaDataProvider(OdpsUtils.getOdps(this.odpsConf));
        this.resolvedSchema = resolvedSchema;
        this.streaming = isStreamingMode;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // only support non-nested look up keys
        int[] keyIndices = new int[context.getKeys().length];
        int i = 0;
        for (int[] key : context.getKeys()) {
            if (key.length > 1) {
                throw new UnsupportedOperationException(
                        "Hive lookup can not support nested key now.");
            }
            keyIndices[i] = key[0];
            i++;
        }
        // TODO:
        throw new UnsupportedOperationException();
//        return TableFunctionProvider.of(
//                new OdpsLookupFunction(
//                        getOdpsInputFormat(),
//                        lookupOptions,
//                        keyIndices,
//                        (RowType) tableSchema.toRowDataType().getLogicalType()));
    }


    @Override
    public Map<String, DataType> listReadableMetadata() {
        Map<String, DataType> metadata = new LinkedHashMap<>();
        metadata.put("bucket_id", DataTypes.INT().notNull());
        metadata.put("bucket_time", DataTypes.TIMESTAMP_LTZ(3).notNull());
        return metadata;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final TypeInformation<RowData> producedTypeInfo =
                InternalTypeInfo.of(tableSchema.toRowDataType().getLogicalType());

        Table table = OdpsUtils.getOdps(odpsConf).tables().get(identifier.getProject(),
                identifier.getSchema(),
                identifier.getTable());
        com.aliyun.odps.TableSchema odpsTableSchema = table.getSchema();

        OdpsUtils.checkFlinkTableSchema(odpsTableSchema, tableSchema);
        OdpsColumn[] selectedColumns = OdpsUtils.getSerializableSelectedColumns(odpsTableSchema, tableSchema);
        OdpsColumn[] selectedPtColumns = OdpsUtils.getSerializableSelectedPtColumns(odpsTableSchema, tableSchema);
        odpsConf.setProperty(SCAN_PARTITION_LATEST.key(), String.valueOf(flinkConf.get(SCAN_PARTITION_LATEST)));
        if (flinkConf.get(SCAN_PARTITION_LATEST)) {
            LatestPartitionOptions.validate(flinkConf,
                    streaming, isPartitionTable(), table.isTransactional(),
                    table.getPrimaryKey() != null && !table.getPrimaryKey().isEmpty(),
                    remainingPartitions != null);
            odpsConf.setProperty(OdpsOptions.SCAN_PARTITION_READY_MODE.key(),
                    flinkConf.get(OdpsOptions.SCAN_PARTITION_READY_MODE));
        }
        List<PartitionSpec> partitionSpecs = flinkConf.get(SCAN_PARTITION_LATEST)
                ? Collections.emptyList() : getPrunedPartitions();
        List<String> partitions = partitionSpecs.stream().map(
                PartitionSpec::toString
        ).collect(Collectors.toList());

        if (flinkConf.get(SCAN_DISK_SPILL_ENABLED)) {
            odpsConf.setProperty(SCAN_DISK_SPILL_ENABLED.key(),
                    String.valueOf(true));
            odpsConf.setProperty(SCAN_DISK_SPILL_MEMORY_CAPACITY.key(),
                    String.valueOf(flinkConf.get(SCAN_DISK_SPILL_MEMORY_CAPACITY)));
            odpsConf.setProperty(SCAN_DISK_SPILL_MAX_BYTES.key(),
                    String.valueOf(flinkConf.get(SCAN_DISK_SPILL_MAX_BYTES)));
        }

        if (streaming && !(table.isTransactional() &&
                table.getPrimaryKey() != null && !table.getPrimaryKey().isEmpty())) {
            LOG.warn("Streaming read is not supported for non-transactional tables. Fall-back to batch read mode instead.");
            streaming = false;
        }

        // BOUNDED mode: force batch read even in streaming execution mode
        if (flinkConf.get(SCAN_MODE) == OdpsStartupMode.BOUNDED) {
            LOG.info("BOUNDED mode specified, using BatchOdpsSource for bounded read.");
            streaming = false;
        }

        if (streaming) {
            IncrementalOptions.Builder builder = IncrementalOptions.newBuilder();

            // TODO: Not support FROM_SNAPSHOT_FULL now
            if (flinkConf.get(SCAN_MODE) == OdpsStartupMode.FROM_SNAPSHOT_FULL) {
                throw new UnsupportedOperationException("Streaming read is not supported for FROM_SNAPSHOT_FULL now");
            }

            if (flinkConf.get(SCAN_MODE) == OdpsStartupMode.FROM_SNAPSHOT
                    || flinkConf.get(SCAN_MODE) == OdpsStartupMode.FROM_SNAPSHOT_FULL) {
                if (!flinkConf.getOptional(SCAN_SNAPSHOT_VERSION).isPresent()) {
                    throw new IllegalArgumentException(String.format(
                            "%s can not be null when you use from-snapshot for %s",
                            SCAN_SNAPSHOT_VERSION.key(),
                            SCAN_MODE.key()));
                }
                builder.startVersion(flinkConf.get(SCAN_SNAPSHOT_VERSION));
            } else if (flinkConf.get(SCAN_MODE) == OdpsStartupMode.FROM_TIMESTAMP) {
                if (!flinkConf.getOptional(SCAN_TIMESTAMP).isPresent()) {
                    throw new IllegalArgumentException(String.format(
                            "%s can not be null when you use from-timestamp for %s",
                            SCAN_TIMESTAMP.key(),
                            SCAN_MODE.key()));
                }
                builder.startTimeStamp(flinkConf.get(SCAN_TIMESTAMP));
            } else if (flinkConf.get(SCAN_MODE) == OdpsStartupMode.INCREMENTAL) {
                throw new IllegalArgumentException(String.format(
                        "Unsupported %s value %s in streaming mode",
                        SCAN_MODE.key(), OdpsStartupMode.INCREMENTAL.toString()));
            } else {
                // for latest and latest-full
                builder.startVersion(-1L);
            }

            builder.withIncrementalMode(flinkConf.get(SCAN_INCREMENTAL_MODE));

            if (flinkConf.get(CONTINUOUS_DISCOVERY_INTERVAL).toMillis() < 30 * 1000L) {
                throw new IllegalArgumentException(String.format(
                        "The value for '%s' cannot be less than 30 seconds. " +
                                "Please adjust it to meet the required duration.",
                        CONTINUOUS_DISCOVERY_INTERVAL.key()));
            }
            return SourceProvider.of(new ContinuousOdpsSource(
                    odpsConf,
                    isPartitionTable(),
                    identifier,
                    partitions,
                    selectedColumns,
                    selectedPtColumns,
                    producedTypeInfo,
                    flinkConf.get(SPLIT_BATCH_READ_SIZE),
                    flinkConf.get(COMPRESS_CODEC),
                    OdpsSplitStrategy.BUCKET,
                    flinkConf.get(SPLIT_BYTE_SIZE),
                    flinkConf.get(SCAN_MAX_SPLITS_PER_TASK),
                    flinkConf.get(CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                    flinkConf.get(SCAN_MODE),
                    builder.build(),
                    ZoneId.systemDefault()
            ));
        } else {
            Optional<IncrementalOptions> incrementalOptions = Optional.empty();

            if (flinkConf.get(SCAN_MODE) == OdpsStartupMode.INCREMENTAL) {
                IncrementalOptions.Builder builder = IncrementalOptions.newBuilder();

                if (flinkConf.getOptional(INCREMENTAL_BETWEEN).isPresent()) {
                    String str = flinkConf.get(INCREMENTAL_BETWEEN);
                    String[] split = str.split(",");
                    if (split.length != 2) {
                        throw new IllegalArgumentException(
                                "The incremental-between must specific start(exclusive) and end snapshot,"
                                        + " for example, 'incremental-between'='5,10' means changes between snapshot 5 and snapshot 10. But is: "
                                        + str);
                    }
                    builder.startVersion(Long.parseLong(split[0]));
                    builder.endVersion(Long.parseLong(split[1]));
                } else if (flinkConf.getOptional(INCREMENTAL_BETWEEN_TIMESTAMP).isPresent()) {
                    String str = flinkConf.get(INCREMENTAL_BETWEEN_TIMESTAMP);
                    String[] split = str.split(",");
                    if (split.length != 2) {
                        throw new IllegalArgumentException(
                                "The incremental-between-timestamp must specific start(exclusive) and end timestamp. But is: "
                                        + str);
                    }
                    builder.startTimeStamp(split[0]);
                    builder.endTimeStamp(split[1]);
                } else {
                    throw new IllegalArgumentException(String.format(
                            "%s or %s can not be null when you use %s for %s",
                            INCREMENTAL_BETWEEN.key(), INCREMENTAL_BETWEEN_TIMESTAMP.key(),
                            OdpsStartupMode.INCREMENTAL.toString(),
                            SCAN_MODE.key()));
                }

                builder.withIncrementalMode(flinkConf.get(SCAN_INCREMENTAL_MODE));
                incrementalOptions = Optional.of(builder.build());
            }

            OdpsSplitStrategy odpsSplitStrategy = flinkConf.getOptional(SPLIT_STRATEGY).orElse(
                    OdpsSplitStrategy.BYTE_SIZE);
            boolean dynamicLoadBalance = flinkConf.get(DYNAMIC_LOAD_BALANCING);

            if (incrementalOptions.isPresent()) {
                odpsSplitStrategy = OdpsSplitStrategy.BUCKET;
                dynamicLoadBalance = false;
            }

            final TypeInformation<RowData> finalTypeInfo =
                    producedDataType != null
                            ? InternalTypeInfo.of(producedDataType.getLogicalType())
                            : producedTypeInfo;

            BatchOdpsSource source = new BatchOdpsSource(
                    odpsConf,
                    isPartitionTable(),
                    identifier,
                    partitions,
                    selectedColumns,
                    selectedPtColumns,
                    finalTypeInfo,
                    flinkConf.get(SPLIT_BATCH_READ_SIZE),
                    dynamicLoadBalance,
                    odpsSplitStrategy,
                    flinkConf.get(COMPRESS_CODEC),
                    flinkConf.get(SPLIT_BYTE_SIZE),
                    incrementalOptions.orElse(null),
                    ZoneId.systemDefault()
            );

            if (!metadataKeys.isEmpty()) {
                source.setMetadataKeys(metadataKeys);
            }

            if (watermarkStrategy != null) {
                return new DataStreamScanProvider() {
                    @Override
                    public DataStream<RowData> produceDataStream(
                            ProviderContext ctx, StreamExecutionEnvironment env) {
                        return env.fromSource(source, watermarkStrategy, "odps-source");
                    }

                    @Override
                    public boolean isBounded() {
                        return true;
                    }
                };
            }
            return SourceProvider.of(source);
        }
    }

    private List<PartitionSpec> getPrunedPartitions() {
        List<Partition> odpsPartitions = new ArrayList<>();
        if (isPartitionTable()) {
            if (flinkConf.getOptional(SCAN_PARTITION).isPresent()) {
                String partitionStr = flinkConf.get(SCAN_PARTITION);
                LOG.info("Using static partition from config: {}", partitionStr);
                PartitionSpec partitionSpec = new PartitionSpec(partitionStr);
                Partition partition = metaDataProvider.getPartition(
                        identifier.getProject(),
                        identifier.getSchema(),
                        identifier.getTable(),
                        partitionSpec.toString(),
                        false);
                if (partition != null) {
                    odpsPartitions.add(partition);
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Partition '%s' not found in table %s", partitionStr, identifier));
                }
            } else if (remainingPartitions != null) {
                for (Map<String, String> spec : remainingPartitions) {
                    PartitionSpec partitionSpec = new PartitionSpec();
                    spec.forEach(partitionSpec::set);
                    odpsPartitions.add(
                        metaDataProvider.getPartition(identifier.getProject(),
                                                      identifier.getSchema(),
                                                      identifier.getTable(),
                                                      partitionSpec.toString(),
                                                      false));
                }
            } else {
                odpsPartitions.addAll(metaDataProvider.getPartitions(identifier.getProject(),
                        identifier.getSchema(),
                        identifier.getTable(),
                        true));
            }
        }
        return odpsPartitions.stream().map(Partition::getPartitionSpec).collect(Collectors.toList());
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        if (flinkConf.get(SCAN_PARTITION_LATEST)) {
            // Keep SQL partition predicates above the dynamic source, never freeze discovery.
            return Optional.empty();
        }
        // TODO: cache partitions
        if (isPartitionTable()) {
            List<Map<String, String>> results = new ArrayList<>();
            List<Partition> partitions = metaDataProvider.getPartitions(
                    identifier.getProject(), identifier.getSchema(), identifier.getTable());
            partitions.forEach(p -> {
                Map<String, String> part = new LinkedHashMap<>();
                partitionKeys.forEach(key -> {
                    part.put(key, p.getPartitionSpec().get(key));
                });
                results.add(part);
            });
            return Optional.of(results);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        if (isPartitionTable()) {
            this.remainingPartitions = remainingPartitions;
        } else {
            throw new UnsupportedOperationException(
                    "Should not apply partitions to a non-partitioned table.");
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // TODO: Flink 1.16: for projection
        LOG.info("applyProjection " + Arrays.deepToString(projectedFields) + " " + producedDataType);
        this.tableSchema = projectSchema(tableSchema, projectedFields);
    }

    public static TableSchema projectSchema(TableSchema tableSchema, int[][] projectedFields) {
        Preconditions.checkArgument(org.apache.flink.table.utils.TableSchemaUtils.containsPhysicalColumnsOnly(tableSchema), "Projection is only supported for physical columns.");
        TableSchema.Builder builder = TableSchema.builder();
        FieldsDataType fields = (FieldsDataType) DataTypeUtils.projectRow(tableSchema.toRowDataType(), projectedFields);
        RowType topFields = (RowType) fields.getLogicalType();
        List<String> fieldNames = topFields.getFieldNames();

        for (int i = 0; i < topFields.getFieldCount(); ++i) {
            builder.field((String) fieldNames.get(i), (DataType) fields.getChildren().get(i));
        }

        Optional<UniqueConstraint> optionalUniqueConstraint = tableSchema.getPrimaryKey();
        if (optionalUniqueConstraint.isPresent()) {
            UniqueConstraint uniqueConstraint = (UniqueConstraint) optionalUniqueConstraint.get();
            if (uniqueConstraint.getType() == Constraint.ConstraintType.PRIMARY_KEY) {
                List<String> pkColumns = uniqueConstraint.getColumns();
                boolean projectFieldsHasAllPk = true;
                Iterator var10 = pkColumns.iterator();

                while (var10.hasNext()) {
                    String pkColumn = (String) var10.next();
                    if (!fieldNames.contains(pkColumn)) {
                        projectFieldsHasAllPk = false;
                        break;
                    }
                }

                if (projectFieldsHasAllPk) {
                    builder.primaryKey((String[]) pkColumns.toArray(new String[0]));
                }
            }
        }

        return builder.build();
    }

    @Override
    public String asSummaryString() {
        return "OdpsSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (!streaming || flinkConf.get(SCAN_MODE) == OdpsStartupMode.BOUNDED) {
            return ChangelogMode.insertOnly();
        }
        return ChangelogMode.all();
    }

    @Override
    public DynamicTableSource copy() {
        OdpsDynamicTableSource source = new OdpsDynamicTableSource(
                flinkConf, odpsConf, lookupOptions, identifier, tableSchema, partitionKeys, resolvedSchema, streaming);
        source.remainingPartitions = remainingPartitions;
        source.projectedFields = projectedFields;
        source.limit = limit;
        source.metadataKeys = metadataKeys;
        source.producedDataType = producedDataType;
        source.watermarkStrategy = watermarkStrategy;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OdpsDynamicTableSource)) {
            return false;
        }
        OdpsDynamicTableSource that = (OdpsDynamicTableSource) o;
        return Objects.equals(odpsConf, that.odpsConf)
                && Objects.equals(flinkConf, that.flinkConf)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(remainingPartitions, that.remainingPartitions)
                && Arrays.equals(projectedFields, that.projectedFields)
                && Objects.equals(limit, that.limit)
                && Objects.equals(metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                odpsConf,
                flinkConf,
                lookupOptions,
                identifier,
                tableSchema,
                partitionKeys,
                remainingPartitions,
                projectedFields,
                limit,
                metadataKeys);
    }

    private boolean isPartitionTable() {
        return partitionKeys != null && !partitionKeys.isEmpty();
    }
}
