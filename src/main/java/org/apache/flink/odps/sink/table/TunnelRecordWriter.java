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

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class TunnelRecordWriter implements BatchWriter<ArrayRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(TunnelRecordWriter.class);

    protected RecordWriter writer;
    private boolean isClosed;

    private Metrics metrics;
    private RecordCount recordCount;

    private final TableIdentifier identifier;
    private final List<Column> requiredDataColumns;
    private final String sinkId;
    private final long writerId;
    private final TunnelWriterOptions options;
    private final PartitionSpec partitionSpec;
    private TableTunnel.UploadSession session;

    public TunnelRecordWriter(String sinkId,
                              TableIdentifier identifier,
                              DataSchema schema,
                              long blockNumber,
                              TunnelWriterOptions writerOptions,
                              PartitionSpec targetPartitionSpec)
            throws IOException {
        this.identifier = identifier;
        this.requiredDataColumns = schema.getColumns();
        this.sinkId = sinkId;
        this.writerId = blockNumber;
        this.options = writerOptions;
        this.partitionSpec = targetPartitionSpec;
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
        return (ArrayRecord)session.newRecord(true);
    }

    @Override
    public void write(ArrayRecord record) throws IOException {
        writer.write(record);
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
        close();
        if (options.isBufferWriter()) {
            List<Long> blocks = new ArrayList<>();
            long nextBlockId = session.getAvailBlockId();

            for (long i = writerId; i < nextBlockId; ) {
                blocks.add(i);
                i = i + options.getNumberOfParallel();
            }

            return new TunnelMultiBlockCommitMessage(sinkId, blocks);
        } else {
            return new TunnelCommitMessage(sinkId, writerId);
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        writer.close();
        isClosed = true;
    }

    public void flush() throws IOException {
        if (options.isBufferWriter()) {
            ((TunnelBufferedWriter) writer).flush();
        }
    }

    private void reloadSession() throws IOException {
        TableTunnel tunnel = TableUtils.getTableTunnel(options.getSettings());
        int retry = 0;
        long sleep = 2000;
        while (true) {
            try {
                if (partitionSpec.isEmpty()) {
                    session = tunnel.getUploadSession(
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            null,
                            sinkId,
                            options.getNumberOfParallel(),
                            writerId);
                } else {
                    session = tunnel.getUploadSession(
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            partitionSpec,
                            sinkId,
                            options.getNumberOfParallel(),
                            writerId);
                }
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

    private void initWriter() throws IOException {
        try {
            if (options.isBufferWriter()) {
                this.writer = session.openBufferedWriter(true);
                ((TunnelBufferedWriter) writer).setBufferSize(options.getBufferSize());
            } else {
                this.writer = session.openRecordWriter(writerId, true);
            }
        } catch (TunnelException e) {
            throw new RuntimeException(e);
        }
    }
}
