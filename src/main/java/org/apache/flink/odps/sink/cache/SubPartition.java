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

package org.apache.flink.odps.sink.cache;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.*;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

public class SubPartition {

    private static final Logger LOG = LoggerFactory.getLogger(SubPartition.class);

    /** This lock guards the creation of readers and disposal of the memory mapped file. */
    private final Object lock = new Object();

    /** The current buffer, may be filled further over time. */
    @Nullable
    private BufferConsumer currentBuffer;

    /** The bounded data store that we store the data in. */
    private final FileChannelBoundedData data;

    /** All created and not yet released readers. */
    @GuardedBy("lock")
    private final Set<ResultSubpartitionView> readers;

    /**
     * Flag to transfer file via FileRegion way in network stack if partition type is file without
     * SSL enabled.
     */
    private final boolean useDirectFileTransfer;

    /** Counter for the number of data buffers (not events!) written. */
    private int numDataBuffersWritten;

    /** The counter for the number of data buffers and events. */
    private int numBuffersAndEventsWritten;

    /** Flag indicating whether the writing has finished and this is now available for read. */
    private boolean isFinished;

    /** Flag indicating whether the subpartition has been released. */
    private boolean isReleased;

    public SubPartition(FileChannelBoundedData data, boolean useDirectFileTransfer) {
        this.data = data;
        this.useDirectFileTransfer = useDirectFileTransfer ;
        this.readers = new HashSet<>();
    }

    public SubPartition(FileChannelBoundedData data, boolean useDirectFileTransfer, boolean isFinished) {
        this.data = data;
        this.useDirectFileTransfer = useDirectFileTransfer ;
        this.readers = new HashSet<>();
        this.isFinished = isFinished;
    }

    // ------------------------------------------------------------------------

    /**
     * Checks if writing is finished. Readers cannot be created until writing is finished, and no
     * further writes can happen after that.
     */
    public boolean isFinished() {
        return isFinished;
    }

    public boolean isReleased() {
        return isReleased;
    }

    public boolean add(BufferConsumer bufferConsumer, int partialRecordLength) throws IOException {
        if (isFinished()) {
            bufferConsumer.close();
            return false;
        }

        flushCurrentBuffer();
        currentBuffer = bufferConsumer;
        return true;
    }

    public void flush() {
        // unfortunately, the signature of flush does not allow for any exceptions, so we
        // need to do this discouraged pattern of runtime exception wrapping
        try {
            flushCurrentBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    private void flushCurrentBuffer() throws IOException {
        if (currentBuffer != null) {
            writeAndCloseBufferConsumer(currentBuffer);
            currentBuffer = null;
        }
    }

    private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer) throws IOException {
        try {
            final Buffer buffer = bufferConsumer.build();
            try {
                data.writeBuffer(buffer);

                numBuffersAndEventsWritten++;
                if (buffer.isBuffer()) {
                    numDataBuffersWritten++;
                }
            } finally {
                buffer.recycleBuffer();
            }
        } finally {
            bufferConsumer.close();
        }
    }

    public void finish() throws IOException {
        checkState(!isReleased, "data partition already released");
        checkState(!isFinished, "data partition already finished");

        isFinished = true;
        flushCurrentBuffer();
        writeAndCloseBufferConsumer(
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false));
        data.finishWrite();
    }

    public void release() throws IOException {
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            isFinished = true; // for fail fast writes

            if (currentBuffer != null) {
                currentBuffer.close();
                currentBuffer = null;
            }
            checkReaderReferencesAndDispose();
        }
    }

    public ResultSubpartitionView createReadView(BufferAvailabilityListener availability)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "data partition already released");
            checkState(isFinished, "writing of blocking partition not yet finished");

            if (!Files.isReadable(data.getFilePath())) {
                throw new FlinkRuntimeException("Partition not found.");
            }

            final ResultSubpartitionView reader;
            reader =
                    new PartitionBufferReader(
                            this, data, numDataBuffersWritten, availability);
            readers.add(reader);
            return reader;
        }
    }

    void releaseReaderReference(ResultSubpartitionView reader) throws IOException {
        synchronized (lock) {
            if (readers.remove(reader) && isReleased) {
                checkReaderReferencesAndDispose();
            }
        }
    }

    @GuardedBy("lock")
    private void checkReaderReferencesAndDispose() throws IOException {
        assert Thread.holdsLock(lock);

        // To avoid lingering memory mapped files (large resource footprint), we don't
        // wait for GC to unmap the files, but use a Netty utility to directly unmap the file.
        // To avoid segmentation faults, we need to wait until all readers have been released.

        if (readers.isEmpty()) {
            data.close();
        }
    }

    @VisibleForTesting
    public BufferConsumer getCurrentBuffer() {
        return currentBuffer;
    }

    // ---------------------------- statistics --------------------------------

    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    protected long getTotalNumberOfBuffers() {
        return numBuffersAndEventsWritten;
    }

    protected long getTotalNumberOfBytes() {
        return data.getSize();
    }

    int getBuffersInBacklog() {
        return numDataBuffersWritten;
    }

    // ---------------------------- factories --------------------------------

    public static SubPartition createWithFileChannel(
            File tempFile,
            int readBufferSize,
            boolean sslEnabled)
            throws IOException {

        final FileChannelBoundedData bd =
                FileChannelBoundedData.create(tempFile.toPath(), readBufferSize);
        return new SubPartition(bd, !sslEnabled);
    }

    public static SubPartition createForRead(
            File tempFile,
            int readBufferSize,
            boolean sslEnabled) {
        final FileChannelBoundedData bd =
                FileChannelBoundedData.createForRead(tempFile.toPath(), readBufferSize);
        return new SubPartition(bd, !sslEnabled, true);
    }
}
