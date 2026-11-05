/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.sink.common;

import java.io.IOException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.sink.partition.PartitionComputer;
import org.apache.flink.odps.sink.partition.TablePartitionAssigner;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.operators.coordination.AcknowledgeCheckpointEvent;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OdpsWriteFunction<I>
    extends AbstractWriteFunction<I>
    implements CheckpointedFunction, CheckpointListener {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsWriteFunction.class);

    protected final OdpsConf odpsConf;
    protected final TableIdentifier tableIdentifier;
    protected final PartitionAssigner<RowData> partitionAssigner;
    protected final DataSchema dataSchema;
    protected final boolean isPartitioned;
    protected final boolean isDynamicPartition;
    protected final OdpsWriteOptions writeOptions;
    protected final WriteOperationType writeOperationType;

    protected String partition;

    protected int taskID;
    protected int taskParallel;

    protected transient Odps odps;
    protected transient OdpsMetaDataProvider tableMetaProvider;
    protected transient EnvironmentSettings settings;
    protected final RowType rowType;

    public OdpsWriteFunction(Configuration config,
                             OdpsConf odpsConf,
                             TableIdentifier identifier,
                             String partition,
                             DataSchema schema,
                             boolean isDynamicPartition,
                             OdpsWriteOptions writeOptions,
                             PartitionAssigner<RowData> partitionAssigner,
                             WriteOperationType writeOperationType,
                             RowType rowType) {
        super(config);

        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");

        this.tableIdentifier = identifier;
        this.dataSchema = schema;
        this.rowType = rowType;

        this.isDynamicPartition = isDynamicPartition;
        this.isPartitioned = !dataSchema.getPartitionKeys().isEmpty();

        this.writeOptions = writeOptions == null ?
                OdpsWriteOptions.builder().build() : writeOptions;

        if (isDynamicPartition && partitionAssigner == null) {
            this.partitionAssigner = new TablePartitionAssigner(
                    PartitionComputer.instance(dataSchema, rowType, partition, config.get(OdpsOptions.PARTITION_DEFAULT_VALUE)));
        } else {
            this.partitionAssigner = partitionAssigner;
        }

        this.writeOperationType = writeOperationType;

        if (!isDynamicPartition) {
            checkPartition(partition);
        }
        LOG.info("Create odps write, table:{}, partition:{}, isDynamicPartition:{}, writeOperationType: {}",
                tableIdentifier.toString(), partition, isDynamicPartition, writeOperationType);
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        this.settings = TableUtils.getEnvironmentSettings(getOdps(), odpsConf.getTunnelEndpoint(), config);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.taskID = getRuntimeContext().getIndexOfThisSubtask();
        this.taskParallel = getRuntimeContext().getNumberOfParallelSubtasks();
        this.inputEnded = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (inputEnded) {
            LOG.info("Snapshot {} completed for input ended", context.getCheckpointId());
        }
        snapshotState(context.getCheckpointId());
    }

    protected abstract void snapshotState(long checkpointId) throws IOException;

    private void checkPartition(String partitionSpec) {
        if (isPartitioned) {
            if (StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                LOG.error("The partition cannot be null or whitespace with partition table: " + tableIdentifier.toString());
                throw new FlinkOdpsException("Check partition failed.");
            } else {
                this.partition = new PartitionSpec(partitionSpec).toString();
            }
        } else {
            if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                throw new FlinkOdpsException(
                        "The partition spec should be null or whitespace with non partition odps table: " + tableIdentifier.toString());
            } else {
                this.partition = "";
            }
        }
    }

    protected Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }

    protected OdpsMetaDataProvider getTableMetaProvider() {
        if (tableMetaProvider == null) {
            tableMetaProvider = new OdpsMetaDataProvider(this.getOdps());
        }
        return tableMetaProvider;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // no operation
        LOG.info("Notify the checkpoint {} complete", checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        // no operation
        LOG.info("Notify the checkpoint {} aborted", checkpointId);
    }

    // For flink 1.16+, see [FLINK-28606]
    public void acknowledgeCheckpoint(long checkpointId) {
        eventGateway.sendEventToCoordinator(new AcknowledgeCheckpointEvent(checkpointId));
    }
}