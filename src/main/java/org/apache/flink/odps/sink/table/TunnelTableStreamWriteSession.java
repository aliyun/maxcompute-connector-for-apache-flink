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

import java.io.IOException;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.TableStreamingWriteSession;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.table.write.impl.batch.TableBatchWriteSessionBase;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import org.apache.arrow.vector.VectorSchemaRoot;

public class TunnelTableStreamWriteSession extends TableBatchWriteSessionBase implements TableStreamingWriteSession {

    public TunnelTableStreamWriteSession(TableWriteSessionBuilder builder) throws IOException {
        super(builder);
        // Generate a simple session ID for standalone mode
        // In standalone mode, session ID is only used for local identification
        if (StringUtils.isNotBlank(builder.getSessionId())) {
            this.sessionId = builder.getSessionId();
        } else {
            // Generate a unique session ID based on table identifier and partition
            String partitionStr = builder.getTargetPartitionSpec() == null ? "" : 
                builder.getTargetPartitionSpec().toString(false, true);
            this.sessionId = builder.getIdentifier().getProject() + "_" + 
                           builder.getIdentifier().getSchema() + "_" + 
                           builder.getIdentifier().getTable() + "_" + 
                           partitionStr + "_" + System.currentTimeMillis();
        }
        this.sessionStatus = SessionStatus.NORMAL;
    }

    @Override
    protected void initSession() throws IOException {
        Preconditions.checkArgument(dynamicPartitionOptions.getInvalidStrategy().equals
                                        (DynamicPartitionOptions.InvalidStrategy.EXCEPTION),
                                    "Strategy not supported:" + dynamicPartitionOptions.getInvalidStrategy());
        // do nothing
    }

    @Override
    protected String reloadSession() throws IOException {
        // do nothing
        return "";
    }

    @Override
    public void commit(WriterCommitMessage[] messages) throws IOException {
        // do nothing
    }

    @Override
    public void cleanup() {
        // do nothing
    }

    @Override
    public BatchWriter<ArrayRecord> createRecordWriter(long writerId,
        WriterAttemptId attemptId,
        WriterOptions options) throws IOException {
        try {
            return new TunnelStreamingRecordWriter(identifier, requiredSchema,
                                                   (TunnelWriterOptions)options, targetPartitionSpec);
        } catch (TunnelException e) {
            throw new IOException(
                "TunnelException {Message: " + e.getMessage() + ", RequestId: " + e.getRequestId() + "}", e);
        }
    }

    @Override
    public BatchWriter<VectorSchemaRoot> createArrowWriter(long writerId,
        WriterAttemptId attemptId,
        WriterOptions options) throws IOException {
        throw new UnsupportedOperationException("Arrow is not supported");
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        if (dataFormat.getType().equals(DataFormat.Type.RECORD)) {
            return true;
        }
        return false;
    }
}
