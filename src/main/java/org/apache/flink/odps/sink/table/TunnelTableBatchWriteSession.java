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

package org.apache.flink.odps.sink.table;

import com.aliyun.odps.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.table.write.impl.batch.TableBatchWriteSessionBase;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TunnelTableBatchWriteSession extends TableBatchWriteSessionBase {

    protected transient TableTunnel.UploadSession session;

    public TunnelTableBatchWriteSession(TableWriteSessionBuilder builder) throws IOException {
        super(builder);
        if (!StringUtils.isNullOrEmpty(builder.getSessionId())) {
            this.targetPartitionSpec = builder.getTargetPartitionSpec() == null ? new PartitionSpec() : builder.getTargetPartitionSpec();
            reloadSessionInternal();
        }
    }

    @Override
    protected void initSession() throws IOException {
        Preconditions.checkArgument(dynamicPartitionOptions.getInvalidStrategy().equals
                        (DynamicPartitionOptions.InvalidStrategy.EXCEPTION),
                "Strategy not supported:" + dynamicPartitionOptions.getInvalidStrategy());

        // TODO: check partition
        int retry = 0;
        long sleep = 2000;
        TableTunnel tunnel = TableUtils.getTableTunnel(settings);
        while (true) {
            try {
                if (targetPartitionSpec.isEmpty()) {
                    session = tunnel.createUploadSession(identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            overwrite);
                } else {
                    session = tunnel.createUploadSession(identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            targetPartitionSpec,
                            overwrite);
                }
                TableSchema tableSchema = session.getSchema();
                this.requiredSchema = DataSchema.newBuilder()
                        .columns(tableSchema.getColumns())
                        .build();
                this.sessionId = session.getId();
                this.sessionStatus = convertSessionStatus(session.getStatus());
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
    }

    @Override
    protected String reloadSession() throws IOException {
        // need partition spec, do nothing

        // TODO : hard code for reload
        // this.sessionStatus = SessionStatus.NORMAL;
        return "";
    }

    @Override
    public void commit(WriterCommitMessage[] messages) throws IOException {
        List<Long> blocks = new ArrayList<>();
        for (int i = 0; i < messages.length; i++) {
            if (messages[i] == null) {
                continue;
            }

            if (messages[i] instanceof TunnelCommitMessage) {
                TunnelCommitMessage message = (TunnelCommitMessage) messages[i];
                blocks.add(message.getBlockId());
            } else if (messages[i] instanceof TunnelMultiBlockCommitMessage) {
                TunnelMultiBlockCommitMessage message = (TunnelMultiBlockCommitMessage) messages[i];
                blocks.addAll(message.getBlockIds());
            }
        }
        try {
            if (blocks.size() > 0) {
                session.commit(blocks.toArray(new Long[0]));
            } else {
                // TODO: should us messages, remove it
                session.commit();
            }
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void cleanup() {
        // Do noting
    }

    @Override
    public BatchWriter<ArrayRecord> createRecordWriter(long writerId,
                                                       WriterAttemptId attemptId,
                                                       WriterOptions options) throws IOException {
        return new TunnelRecordWriter(sessionId, identifier, requiredSchema,
                writerId, (TunnelWriterOptions) options, targetPartitionSpec);
    }

    @Override
    public BatchWriter<VectorSchemaRoot> createArrowWriter(long writerId,
                                                           WriterAttemptId attemptId,
                                                           WriterOptions options) throws IOException {
        return new TunnelArrowBatchWriter(sessionId, session, identifier, requiredSchema,
                writerId, options, targetPartitionSpec, arrowOptions);
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        if (dataFormat.getType().equals(DataFormat.Type.RECORD)) {
            return true;
        }
        if (dataFormat.getType().equals(DataFormat.Type.ARROW)) {
            return requiredSchema.getColumns().stream()
                    .map(Column::getType)
                    .noneMatch(odpsType -> odpsType.equals(OdpsType.STRUCT)
                            || odpsType.equals(OdpsType.ARRAY)
                            || odpsType.equals(OdpsType.MAP));
        }
        return false;
    }

    private void reloadSessionInternal() throws IOException {
        TableTunnel tunnel = TableUtils.getTableTunnel(settings);
        // TODO: check partition
        int retry = 0;
        long sleep = 2000;
        while (true) {
            try {
                if (targetPartitionSpec.isEmpty()) {
                    session = tunnel.getUploadSession(
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            sessionId);
                } else {
                    session = tunnel.getUploadSession(
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            targetPartitionSpec,
                            sessionId);
                }
                TableSchema tableSchema = session.getSchema();
                this.requiredSchema = DataSchema.newBuilder()
                        .columns(tableSchema.getColumns())
                        .build();
                this.sessionStatus = convertSessionStatus(session.getStatus());
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
    }

    private SessionStatus convertSessionStatus(TableTunnel.UploadStatus status) {
        switch (status) {
            case CLOSED:
            case CLOSING:
            case EXPIRED:
                return SessionStatus.EXPIRED;
            case UNKNOWN:
                return SessionStatus.UNKNOWN;
            case NORMAL:
                return SessionStatus.NORMAL;
            case CRITICAL:
            case CANCELED:
                return SessionStatus.CRITICAL;
            case COMMITTING:
                return SessionStatus.COMMITTING;
        }
        return SessionStatus.UNKNOWN;
    }
}
