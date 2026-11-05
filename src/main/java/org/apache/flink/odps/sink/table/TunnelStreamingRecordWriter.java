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
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.FlushOption;
import com.aliyun.odps.tunnel.TableTunnel.FlushResult;
import com.aliyun.odps.tunnel.TableTunnel.StreamRecordPack;
import com.aliyun.odps.tunnel.TableTunnel.StreamUploadSession.Builder;
import com.aliyun.odps.tunnel.TunnelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TunnelStreamingRecordWriter implements BatchWriter<ArrayRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(TunnelStreamingRecordWriter.class);

    protected StreamRecordPack writer;
    private boolean isClosed;

    private Metrics metrics;
    private RecordCount recordCount;

    private final TableIdentifier identifier;
    private final TunnelWriterOptions options;
    private final PartitionSpec partitionSpec;
    private TableTunnel.StreamUploadSession session;
    private long bufferSize;

    public TunnelStreamingRecordWriter(
        TableIdentifier identifier,
        DataSchema schema,
        TunnelWriterOptions writerOptions,
        PartitionSpec targetPartitionSpec)
        throws TunnelException, IOException {
        this.identifier = identifier;
        this.options = writerOptions;
        this.partitionSpec = targetPartitionSpec;
        this.bufferSize = writerOptions.getBufferSize();
        if (this.bufferSize <= 0) {
            this.bufferSize = 64 * 1024 * 1024;
        }
        this.isClosed = false;
        reloadSession();

        initWriter();
        initMetrics();
    }

    private void initMetrics() {
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        this.metrics.register(recordCount);
    }

    @Override
    public ArrayRecord newElement() {
        return new ArrayRecord(this.session.getSchema().getColumns().toArray(new Column[0]), false, null, true);
    }

    @Override
    public void write(ArrayRecord record) throws IOException {
        writer.append(record);
        if (writer.getDataSize() > bufferSize) {
            flush();
        }
        recordCount.inc(1);
    }

    @Override
    public void abort() throws IOException {
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        flush();
        close();
        return null;
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        session.close();
        isClosed = true;
    }

    public void flush() throws IOException {
        long start = System.currentTimeMillis();
        FlushResult flush = writer.flush(new FlushOption());
        long cost = System.currentTimeMillis() - start;
        LOG.info("Flush {} bytes cost {}ms, requestId {}", flush.getFlushSize(), cost, flush.getTraceId());
    }

    private void reloadSession() throws TunnelException {
        TableTunnel tunnel = TableUtils.getTableTunnel(options.getSettings());
        Builder sessionBuilder = tunnel.buildStreamUploadSession(
                identifier.getProject(),
                identifier.getTable())
            .setSchemaName(identifier.getSchema())
            .setSlotNum(this.options.getSlotNum());
        if (!partitionSpec.isEmpty()) {
            sessionBuilder.setPartitionSpec(partitionSpec);
        }
        this.session = sessionBuilder.build();
        LOG.info("Create streaming upload session for table {} partition {}, sessionId {}", identifier, partitionSpec,
                 session.getId());
    }

    private void initWriter() throws IOException {
        this.writer = session.newRecordPack();
    }
}
