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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.sink.datastream;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import javax.annotation.Nullable;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.storage.StorageWriter;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.vectorized.ArrowWriter;
import org.apache.flink.odps.vectorized.ArrowWriterFactory;
import org.apache.flink.table.types.logical.RowType;

/** Flink lifecycle adapter; batching, upload and cleanup belong to the shared Storage core. */
public class MaxComputeSinkWriter<T> implements SinkWriter<T>, ProcessingTimeService.ProcessingTimeCallback {
    private static final long FLUSH_RECHECK_INTERVAL_MS = 5000L;
    private final StorageWriter<T> writer;
    private final ProcessingTimeService timers;
    private final long intervalMs;
    private ScheduledFuture<?> timer;

    /** Preserves the constructor used by existing DataStream integrations. */
    public MaxComputeSinkWriter(
            Configuration config, OdpsConf odpsConf, TableIdentifier identifier,
            @Nullable String partition, ArrowWriterFactory<T> encoders, @Nullable RowType rowType,
            Sink.InitContext context) {
        this(config, odpsConf, identifier, partition, encoders, rowType,
                context.getSubtaskId(), context.getProcessingTimeService());
    }

    public MaxComputeSinkWriter(
            Configuration config, OdpsConf odpsConf, TableIdentifier identifier,
            @Nullable String partition, ArrowWriterFactory<T> encoders, @Nullable RowType rowType,
            int subtaskId, ProcessingTimeService timers) {
        this.timers = timers;
        this.intervalMs = config.get(OdpsOptions.SINK_FLUSH_MAX_INTERVAL).toMillis();
        try {
            this.writer = StorageWriter.open(OdpsUtils.getMaxStorageClient(odpsConf, config),
                    identifier, partition, subtaskId,
                    config.get(OdpsOptions.INSERT_ARROW_WRITE_BATCH_SIZE),
                    config.get(OdpsOptions.INSERT_WRITER_BUFFER_SIZE).getBytes(),
                    config.get(OdpsOptions.INSERT_WRITER_MAX_PENDING_BUFFERS),
                    (schema, batch) -> {
                        ArrowWriter<T> encoder = encoders.create(batch,
                                rowType == null ? OdpsUtils.getFlinkSchema(schema) : rowType);
                        return new StorageWriter.Encoder<T>() {
                            public void write(T row) { encoder.write(row); }
                            public VectorSchemaRoot finishBatch() { encoder.finish(); return encoder.getRecordBatch(); }
                            public void reset() { encoder.reset(); }
                            public void close() { encoder.close(); }
                        };
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MaxCompute Storage writer", e);
        }
        try {
            schedule(intervalMs);
        } catch (RuntimeException failure) {
            try { writer.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    @Override
    public void write(T element, Context context) { writer.write(element); }

    @Override
    public void flush(boolean endOfInput) throws IOException { writer.flush(); }

    @Override
    public void onProcessingTime(long timestamp) {
        schedule(writer.tryFlushAsync() ? intervalMs : FLUSH_RECHECK_INTERVAL_MS);
    }

    private void schedule(long delay) {
        if (timers != null && delay > 0) {
            timer = timers.registerTimer(timers.getCurrentProcessingTime() + delay, this);
        }
    }

    @Override
    public void close() throws Exception {
        if (timer != null) { timer.cancel(false); timer = null; }
        writer.close();
    }
}
