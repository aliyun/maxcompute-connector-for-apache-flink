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

package org.apache.flink.odps.source.enumerator;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.source.OdpsSplitStrategy;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** Split enumerator for ODPS source in batch jobs. */
public class BatchOdpsSplitEnumerator extends OdpsSplitEnumeratorBase<BatchOdpsSplitEnumState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchOdpsSplitEnumerator.class);

    private final SplitEnumeratorContext<OdpsSourceSplit> context;
    private final List<String> prunedPartitions;

    private final List<LinkedList<OdpsSourceSplit>> assignedSplits;
    private final List<String> selectedColumns;
    private final List<String> selectedPtColumns;

    /**
     * if true, {@link #assignedSplits} will have only one list; else {@link #assignedSplits} will
     * have lists with amount of parallelism.
     */
    private final boolean dynamicLoadBalance;

    private final OdpsSplitStrategy odpsSplitStrategy;

    private final long splitByteSize;

    private final IncrementalOptions incrementalOptions;

    public BatchOdpsSplitEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context,
            OdpsConf odpsConf,
            TableIdentifier odpsTable,
            List<String> selectedColumns,
            List<String> selectedPtColumns,
            List<String> prunedPartitions,
            boolean dynamicLoadBalance,
            OdpsSplitStrategy odpsSplitStrategy,
            long splitByteSize,
            IncrementalOptions incrementalOptions) {
        super(odpsConf, odpsTable);
        this.context = context;
        this.selectedColumns = selectedColumns;
        this.selectedPtColumns = selectedPtColumns;
        this.prunedPartitions = prunedPartitions;
        this.assignedSplits = new ArrayList<>();
        this.dynamicLoadBalance = dynamicLoadBalance;
        this.splitByteSize = splitByteSize;
        this.incrementalOptions = incrementalOptions;
        this.odpsSplitStrategy = odpsSplitStrategy;
    }

    public BatchOdpsSplitEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context,
            OdpsConf odpsConf,
            TableIdentifier odpsTable,
            List<String> selectedColumns,
            List<String> selectedPtColumns,
            List<String> prunedPartitions,
            BatchOdpsSplitEnumState enumState,
            boolean dynamicLoadBalance,
            OdpsSplitStrategy odpsSplitStrategy,
            long splitByteSize,
            IncrementalOptions incrementalOptions) {
        super(odpsConf, odpsTable);
        this.context = context;
        this.selectedColumns = selectedColumns;
        this.selectedPtColumns = selectedPtColumns;
        this.prunedPartitions = prunedPartitions;
        this.assignedSplits = enumState.getAssignedSplits();
        this.dynamicLoadBalance = dynamicLoadBalance;
        this.splitByteSize = splitByteSize;
        this.incrementalOptions = incrementalOptions;
        this.odpsSplitStrategy = odpsSplitStrategy;
        LOGGER.info(
                "Recovering state in BatchOdpsSplitEnumerator. Number of remaining splits to read are: "
                        + assignedSplits.stream()
                                .map(l -> String.valueOf(l.size()))
                                .collect(Collectors.joining(", ")));
    }

    @Override
    public void start() {
        LOGGER.info("Starting the BatchOdpsSplitEnumerator for table: {}", odpsTable);
        if (assignedSplits.isEmpty()) {
            LOGGER.info("Assigning splits for readers.");
            context.callAsync(() -> prunedPartitions, this::assignSplits);
        }
    }

    private void assignSplits(List<String> partitions, Throwable t) {
        if (t != null) {
            LOGGER.error("Failed to list partitions.", t);
            return;
        }

        try {
            EnvironmentSettings settings = TableUtils.getEnvironmentSettings(odpsConf);
            TableSchema tableSchema = OdpsUtils.getTableSchema(odpsConf, odpsTable);

            if (!tableSchema.getPartitionColumns().isEmpty() && partitions.isEmpty()) {
                LOGGER.info("The specified partition table is empty.");
                return;
            }

            // requiredDataColumns must be in order.
            List<String> requiredDataColumns = new ArrayList<>();
            tableSchema.getColumns().forEach(column -> {
                        if (selectedColumns.contains(column.getName())) {
                            requiredDataColumns.add(column.getName());
                        }
                    }
            );
            List<String> requiredPartitionColumns = new ArrayList<>();
            tableSchema.getPartitionColumns().forEach(column -> {
                        if (selectedPtColumns.contains(column.getName())) {
                            requiredPartitionColumns.add(column.getName());
                        }
                    }
            );
            TableReadSessionBuilder scanBuilder =
                    new TableReadSessionBuilder()
                            .identifier(odpsTable)
                            .requiredDataColumns(requiredDataColumns)
                            .requiredPartitionColumns(requiredPartitionColumns)
                            .withSettings(settings);

            if (incrementalOptions != null) {
                scanBuilder.withIncrementalOptions(incrementalOptions);
                scanBuilder.withSplitOptions(
                        odpsSplitStrategy.init(splitByteSize).build());
            } else {
                scanBuilder.withSplitOptions(
                        odpsSplitStrategy.init(splitByteSize).withCrossPartition(false).build());
            }

            scanBuilder.withArrowOptions(
                    ArrowOptions.newBuilder()
                            .withDatetimeUnit(ArrowOptions.TimestampUnit.MILLI)
                            .withTimestampUnit(ArrowOptions.TimestampUnit.MICRO)
                            .build());

            List<PartitionSpec> partitionSpecs =
                    partitions.stream().map(PartitionSpec::new).collect(Collectors.toList());
            scanBuilder.requiredPartitions(partitionSpecs);

            TableBatchReadSession tableReadSession;
            if (incrementalOptions != null) {
                tableReadSession = scanBuilder.buildIncrementalReadSession();
            } else {
                tableReadSession = scanBuilder.buildBatchReadSession();
            }

            LOGGER.info("Created read session " + tableReadSession.getId());
            InputSplit[] inputSplits =
                    odpsSplitStrategy.getSplits(tableReadSession.getInputSplitAssigner());

            if (dynamicLoadBalance) {
                assignedSplits.add(new LinkedList<>());
            } else {
                for (int i = 0; i < context.currentParallelism(); i++) {
                    assignedSplits.add(new LinkedList<>());
                }
            }
            int count = 0;
            for (InputSplit split : odpsSplitStrategy.postProcessSplits(inputSplits, context)) {
                int assignSuggestedTask;
                if (split instanceof InputSplitWithBucket) {
                    assignSuggestedTask = (((InputSplitWithBucket)split).getBucketId()) % assignedSplits.size();
                } else {
                    assignSuggestedTask = count % assignedSplits.size();
                }
                assignedSplits
                        .get(assignSuggestedTask)
                        .add(
                                new OdpsSourceSplit(
                                        split.getSessionId() + " " + count,
                                        count,
                                        split,
                                        tableReadSession));
                count += 1;
            }
        } catch (Exception e) {
            throw new RuntimeException("create table read session failed", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String s) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        if (assignedSplits.isEmpty()) {
            context.signalNoMoreSplits(subtaskId);
            return;
        }

        OdpsSourceSplit split = assignedSplits.get(dynamicLoadBalance ? 0 : subtaskId).poll();
        if (split == null) {
            context.signalNoMoreSplits(subtaskId);
        } else {
            LOGGER.info(
                    "assignSplit "
                            + split
                            + " to "
                            + subtaskId
                            + " with dynamicLoadBalance:"
                            + dynamicLoadBalance);
            context.assignSplit(split, subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<OdpsSourceSplit> splits, int subtaskId) {
        if (!assignedSplits.isEmpty()) {
            assignedSplits.get(subtaskId % assignedSplits.size()).addAll(splits);
        }
    }

    @Override
    public void addReader(int reader) {
        // odps source is lazy-pull-based, nothing to do upon registration
    }

    @Override
    public BatchOdpsSplitEnumState snapshotState(long snapshotId) {
        LOGGER.info(
                "Snapshot state BatchOdpsSplitEnumerator. Number of remaining splits to read are: "
                        + assignedSplits.stream()
                                .map(l -> String.valueOf(l.size()))
                                .collect(Collectors.joining(", ")));
        return new BatchOdpsSplitEnumState(assignedSplits);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
