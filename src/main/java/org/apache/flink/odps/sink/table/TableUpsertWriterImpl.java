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
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.metrics.Gauge;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl.DefaultUpsertSteamListener;
import com.aliyun.odps.tunnel.streams.UpsertStream;

import org.apache.flink.odps.util.FormatUtils;
import org.apache.flink.odps.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TableUpsertWriterImpl implements UpsertWriter<ArrayRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(TableUpsertWriterImpl.class);

    protected TableTunnel.UpsertSession session;
    protected UpsertStream stream;
    protected List<Column> schema;
    private boolean isClosed;
    private final long operationTimeoutMillis;
    private boolean partialWrite;
    private List<String> partialWriteColumns;
    private Metrics metrics;
    private BytesCount metricsBytesCount;
    private UpsertRecordCount metricsRecordCount;

    private long processCount;
    private AtomicLong recordCount;
    private AtomicLong bytesCount;

    public TableUpsertWriterImpl(
            TableTunnel.UpsertSession session, TableUpsertWriterOptions writerOptions)
            throws IOException {
        if (writerOptions.getMaxRetries() != 3) {
            LOG.warn(
                    "config 'upsert.writer.max-retries' is deprecated, the configuration may not"
                        + " take effect currently.");
        }
        this.session = session;
        this.operationTimeoutMillis =
                Math.multiplyExact(
                        ((UpsertSessionImpl) session).getReadTimeout(),
                        Math.max(1L, writerOptions.getMaxRetries() + 1L));
        this.schema = session.getSchema().getColumns();
        this.isClosed = false;
        this.partialWrite = writerOptions.supportPartialWrite();
        this.partialWriteColumns = writerOptions.getPartialWriteColumns();

        initMetrics();
        try {
            this.stream =
                    session.buildUpsertStream()
                            .setMaxBufferSize(writerOptions.getMaxBufferSize())
                            .setSlotBufferSize(writerOptions.getSlotBufferSize())
                            .setCompressOption(
                                    OdpsUtils.compressOptionOf(
                                            writerOptions.getCompressAlgorithm()))
                            .setListener(new UpsertStreamListener((UpsertSessionImpl) session))
                            .build();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    class UpsertStreamListener extends DefaultUpsertSteamListener {

        private final AtomicLong totalCost = new AtomicLong(0);
        private final AtomicLong totalBytes = new AtomicLong(0);

        public UpsertStreamListener(UpsertSessionImpl session) {
            super(session);
        }

        @Override
        public void onFlush(UpsertStream.FlushResult result) {
            totalCost.addAndGet(result.flushTime);
            totalBytes.addAndGet(result.flushSize);

            recordCount.addAndGet(result.recordCount);
            bytesCount.addAndGet(result.flushSize);
            LOG.info(
                    "Flush success, trace id: {}, time: {}, record: {}, size: {}, current speed:"
                        + " {}, average speed: {}",
                    result.traceId,
                    result.flushTime,
                    result.recordCount,
                    result.flushSize,
                    FormatUtils.getFormattedSpeed(result.flushTime, result.flushSize),
                    FormatUtils.getFormattedSpeed(totalCost.get(), totalBytes.get()));
        }

        @Override
        public boolean onFlushFail(Exception error, int retry) {
            LOG.error(
                    "Flush failed error {}, requestId {}",
                    error.getMessage(),
                    error instanceof TunnelException
                            ? ((TunnelException) error).getRequestId()
                            : "");
            return super.onFlushFail(error, retry);
        }
    }

    private void initMetrics() {
        this.metricsBytesCount = new BytesCount();
        this.metricsRecordCount = new UpsertRecordCount();
        this.metrics = new Metrics();
        this.metrics.register(metricsBytesCount);
        this.metrics.register(metricsRecordCount);

        this.recordCount = new AtomicLong(0L);
        this.bytesCount = new AtomicLong(0L);
        this.processCount = 0L;
    }

    @Override
    public ArrayRecord newElement() {
        return (ArrayRecord) session.newRecord(true);
    }

    @Override
    public void upsert(ArrayRecord record) throws IOException {
        if (partialWrite) {
            if (partialWriteColumns == null) {
                // update non-null field if user open partial-upsert but not indicate which column
                // to update
                List<String> upsertList = getUpsertList(record);
                BoundedSdkCall.run(
                        operationTimeoutMillis, () -> this.stream.upsert(record, upsertList));
            } else {
                BoundedSdkCall.run(
                        operationTimeoutMillis,
                        () -> this.stream.upsert(record, partialWriteColumns));
            }
        } else {
            BoundedSdkCall.run(operationTimeoutMillis, () -> this.stream.upsert(record));
        }
        this.processCount++;
    }

    private List<String> getUpsertList(ArrayRecord record) {
        List<String> upsertList = new ArrayList<>(record.getColumnCount());
        for (int i = 0; i < record.getColumnCount(); i++) {
            if (record.get(i) != null) {
                upsertList.add(schema.get(i).getName());
            }
        }
        return upsertList;
    }

    @Override
    public void delete(ArrayRecord record) throws IOException {
        BoundedSdkCall.run(operationTimeoutMillis, () -> this.stream.delete(record));
        this.processCount++;
    }

    @Override
    public void flush() throws IOException {
        BoundedSdkCall.run(operationTimeoutMillis, this.stream::flush);
    }

    @Override
    public void reset() throws IOException {
        this.stream.reset();
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        BoundedSdkCall.run(operationTimeoutMillis, this.stream::close);
        isClosed = true;
    }

    @Override
    public Metrics currentMetricsValues() {
        metricsBytesCount.setValue(bytesCount.get());
        metricsRecordCount.setValue(recordCount.get());
        return this.metrics;
    }

    static class UpsertRecordCount extends RecordCount implements Gauge<Long> {

        @Override
        public void setValue(Long value) {
            this.count = value;
        }

        @Override
        public Long getValue() {
            return this.count;
        }
    }
}
