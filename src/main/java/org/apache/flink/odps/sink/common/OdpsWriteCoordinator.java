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

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

public abstract class OdpsWriteCoordinator extends AbstractWriteOperatorCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsWriteCoordinator.class);

    protected final OdpsConf odpsConf;
    protected final TableIdentifier tableIdentifier;

    protected final boolean isDynamicPartition;
    protected final OdpsWriteOptions writeOptions;
    protected final DataSchema dataSchema;
    protected final boolean isPartitioned;
    protected String staticPartition;

    protected transient Odps odps;
    protected transient EnvironmentSettings settings;
    protected transient OdpsMetaDataProvider tableMetaProvider;

    protected final int metaCacheSize;
    protected final int metaCacheExpireTime;

    public OdpsWriteCoordinator(
            String operatorName,
            Configuration config,
            Context context,
            OdpsConf odpsConf,
            TableIdentifier identifier,
            String partition,
            DataSchema dataSchema,
            boolean isDynamicPartition,
            OdpsWriteOptions writeOptions) {
        super(operatorName, config, context);

        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }

        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");

        this.tableIdentifier = identifier;
        this.metaCacheExpireTime = config.get(OdpsOptions.SINK_META_EXPIRE_TIME);
        this.metaCacheSize = config.get(OdpsOptions.SINK_META_CACHE_SIZE);
        this.dataSchema = dataSchema;
        this.isDynamicPartition = isDynamicPartition;
        this.writeOptions = writeOptions == null ?
                OdpsWriteOptions.builder().build() : writeOptions;
        this.isPartitioned = !dataSchema.getPartitionKeys().isEmpty();
        this.tableMetaProvider = getTableMetaProvider();
        try {
            if (!isDynamicPartition) {
                checkPartition(partition);
            }
        } catch (IOException e) {
            throw new FlinkOdpsException(e);
        }
    }

    @Override
    protected void handleBootstrapEvent(SinkTaskEvent event) {
        if (isDynamicPartition) {
            if (StringUtils.isNullOrWhitespaceOnly(event.getPartitionSpec())) {
                throw new FlinkOdpsException("Invalid partition spec for dynamic partition!");
            }
            try {
                initDynamicWriteSession(event.getTaskID(), event.getPartitionSpec());
            } catch (Exception e) {
                throw new FlinkOdpsException(e);
            }
        } else {
            try {
                initStaticWriteSession(event.getTaskID());
            } catch (Exception e) {
                throw new FlinkOdpsException(e);
            }
        }
    }

    @Override
    protected void handleCommitEvent(SinkTaskEvent event) {
        this.eventBuffer[event.getTaskID()] = event;
        if (IntStream.range(0, this.parallelism).anyMatch(
                i -> eventBuffer[i] == null && !completedTaskIds.contains(i))) {
            return;
        }
        try {
            commitWriteSession();
            executor.execute(() -> {
                sendAllTaskAckEvents(event.getCheckpointID(), "", "", true, false);
            }, "Commit session response");
            this.eventBuffer = new SinkTaskEvent[this.parallelism];
        } catch (Exception e) {
            throw new FlinkOdpsException(e);
        }
    }

    @Override
    protected void handleEndInputEvent(SinkTaskEvent event) {
        this.completedTaskIds.add(event.getTaskID());
        handleCommitEvent(event);
    }

    abstract protected void initStaticWriteSession(int taskId) throws IOException;
    abstract protected void initDynamicWriteSession(int taskId, String partitionSpec) throws IOException;
    abstract protected boolean commitWriteSession() throws IOException;

    protected Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }

    protected OdpsMetaDataProvider getTableMetaProvider() {
        if (tableMetaProvider == null) {
            tableMetaProvider = new OdpsMetaDataProvider(this.getOdps(), metaCacheSize, metaCacheExpireTime);
        }
        return tableMetaProvider;
    }

    private void checkPartition(String partitionSpec) throws IOException {
        if (isPartitioned) {
            if (StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                LOG.error("The partition cannot be null or whitespace with partition table: " +
                        tableIdentifier.toString());
                throw new IOException("Check partition failed.");
            } else {
                this.staticPartition = new PartitionSpec(partitionSpec).toString();
                OdpsUtils.createPartitionIfNeeded(getTableMetaProvider(), tableIdentifier, staticPartition);
            }
        } else {
            if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                throw new IOException(
                        "The partition spec should be null or whitespace with non partition odps table: " +
                                tableIdentifier.toString());
            } else {
                this.staticPartition = "";
            }
        }
    }
}
