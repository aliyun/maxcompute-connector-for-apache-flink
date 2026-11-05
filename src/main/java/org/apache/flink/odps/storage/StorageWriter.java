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

package org.apache.flink.odps.storage;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.storage.MaxStorageClient;
import com.aliyun.odps.storage.write.TableArrowWriter;
import com.aliyun.odps.storage.write.TableWriteSession;
import com.aliyun.odps.storage.write.TableWriteSessionBuilder;
import com.aliyun.odps.storage.write.WriteMode;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Storage API v2 batching, asynchronous upload and resource ownership, independent of Flink.
 * Calls are serialized by the owning task. A flush waits for all pending uploads; it does not
 * provide a recoverable transaction or exactly-once delivery.
 */
public final class StorageWriter<T> implements AutoCloseable {
    public interface Encoder<T> extends AutoCloseable {
        void write(T row);
        VectorSchemaRoot finishBatch();
        void reset();
        void close();
    }

    @FunctionalInterface
    public interface EncoderFactory<T> {
        Encoder<T> create(TableSchema schema, VectorSchemaRoot batch);
    }

    // Internal seam shared by lifecycle tests and the actual SDK upload implementation.
    interface Upload extends AutoCloseable {
        void writeBatch(VectorSchemaRoot batch);
        void flush() throws IOException;
        boolean hasPendingFlush();
        long cachedSize();
        void flushAsync();
        void closeWriter() throws Exception;
        void close(boolean commit) throws Exception;
        default void close() throws Exception { close(false); }
    }

    private final Encoder<T> encoder;
    private final Upload upload;
    private final long batchSize;
    private long pendingRows;
    private boolean closed;

    StorageWriter(Encoder<T> encoder, Upload upload, long batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Arrow batch size must be positive");
        }
        this.encoder = encoder;
        this.upload = upload;
        this.batchSize = batchSize;
    }

    /** Takes ownership of client, including when session or encoder initialization fails. */
    public static <T> StorageWriter<T> open(
            MaxStorageClient client, TableIdentifier identifier, String partition,
            int subtask, long batchSize, long bufferBytes, int pendingBuffers,
            EncoderFactory<T> encoders) throws Exception {
        SdkUpload upload = new SdkUpload(client);
        VectorSchemaRoot batch = null;
        try {
            if (batchSize <= 0 || bufferBytes <= 0 || pendingBuffers <= 0) {
                throw new IllegalArgumentException("Writer batch and buffer limits must be positive");
            }
            upload.executor = Executors.newSingleThreadExecutor(
                    r -> new Thread(r, "maxcompute-sink-io-" + subtask));
            TableWriteSessionBuilder builder = client.createTableWriteSessionBuilder(identifier)
                    .withWriteMode(WriteMode.STREAMING_REALTIME);
            if (partition != null && !partition.isEmpty()) {
                builder.withPartition(new PartitionSpec(partition));
            }
            upload.session = builder.build();
            char[] suffix = new char[4];
            for (int i = 0; i < suffix.length; i++) {
                int n = ThreadLocalRandom.current().nextInt(52);
                suffix[i] = (char) (n < 26 ? 'A' + n : 'a' + n - 26);
            }
            String streamId = Long.toString(System.currentTimeMillis(), 36) + '-'
                    + Integer.toString(subtask, 36) + '-' + new String(suffix);
            upload.writer = (TableArrowWriter) upload.session.createWriterBuilder(streamId, 1L)
                    .withAutoFlushEnabled(true).withBufferSize(bufferBytes)
                    .withExecutorService(upload.executor).withMaxPendingBuffers(pendingBuffers).build();
            batch = upload.writer.createVectorSchemaRoot();
            Encoder<T> encoder = encoders.create(upload.writer.getWriteSchema(), batch);
            return new StorageWriter<>(encoder, upload, batchSize);
        } catch (Throwable failure) {
            failure = closeAfter(failure, upload::closeWriter);
            if (batch != null) {
                failure = closeAfter(failure, batch::close);
            }
            failure = closeAfter(failure, upload::close);
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
    }

    public void write(T row) {
        requireOpen();
        encoder.write(row);
        if (++pendingRows >= batchSize) {
            drain();
        }
    }

    public void flush() throws IOException {
        requireOpen();
        drain();
        upload.flush();
    }

    /** False asks the runtime to retry later without blocking its timer/mailbox thread. */
    public boolean tryFlushAsync() {
        requireOpen();
        if (upload.hasPendingFlush()) {
            return false;
        }
        drain();
        if (upload.cachedSize() > 0) {
            upload.flushAsync();
        }
        return true;
    }

    private void drain() {
        if (pendingRows != 0) {
            upload.writeBatch(encoder.finishBatch());
            encoder.reset();
            pendingRows = 0;
        }
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        Throwable failure = null;
        boolean flushOk = false;
        try {
            flush();
            flushOk = true;
        } catch (Throwable t) {
            failure = t;
        } finally {
            closed = true;
        }
        // The SDK may still own an in-flight batch after a failed flush. Close/join it
        // before releasing the encoder's Arrow vectors, as in the original sink lifecycle.
        failure = closeAfter(failure, upload::closeWriter);
        failure = closeAfter(failure, encoder::close);
        final boolean commit = flushOk;
        failure = closeAfter(failure, () -> upload.close(commit));
        if (failure != null) {
            rethrow(failure);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Storage writer is closed");
        }
    }

    private interface CloseAction { void run() throws Exception; }

    private static Throwable closeAfter(Throwable previous, CloseAction action) {
        try {
            action.run();
        } catch (Throwable failure) {
            if (previous == null) {
                return failure;
            }
            if (failure != previous) {
                previous.addSuppressed(failure);
            }
        }
        return previous;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw (Exception) failure;
    }

    private static final class SdkUpload implements Upload {
        private final MaxStorageClient client;
        private ExecutorService executor;
        private TableWriteSession session;
        private TableArrowWriter writer;

        private SdkUpload(MaxStorageClient client) { this.client = client; }
        public void writeBatch(VectorSchemaRoot batch) { writer.writeBatch(batch); }
        public void flush() throws IOException { writer.flush(); }
        public boolean hasPendingFlush() { return writer.hasPendingFlush(); }
        public long cachedSize() { return writer.getCachedSize(); }
        public void flushAsync() { writer.flushAsync(); }

        public void closeWriter() throws Exception {
            if (writer != null) {
                try { writer.close(); } finally { writer = null; }
            }
        }

        public void close(boolean commit) throws Exception {
            Throwable failure = closeAfter(null, this::closeWriter);
            if (session != null) {
                failure = closeAfter(failure, commit ? session::commit : session::close);
            }
            if (executor != null) {
                failure = closeAfter(failure, () -> {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) { executor.shutdownNow(); }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                });
            }
            failure = closeAfter(failure, client::close);
            if (failure != null) { rethrow(failure); }
        }
    }
}
