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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public final class FileChannelBoundedData {

    protected static final Logger LOG = LoggerFactory.getLogger(FileChannelBoundedData.class);

    private final Path filePath;

    @Nullable
    private FileChannel fileChannel;

    private ByteBuffer[] headerAndBufferArray;

    private long size;

    private final int memorySegmentSize;

    FileChannelBoundedData(Path filePath, FileChannel fileChannel, int memorySegmentSize) {

        this.filePath = checkNotNull(filePath);
        this.fileChannel = checkNotNull(fileChannel);
        this.memorySegmentSize = memorySegmentSize;
        this.headerAndBufferArray = BufferReaderWriterUtil.allocatedWriteBufferArray();
        // TODO: free header buffer
    }

    // Only for read
    FileChannelBoundedData(Path filePath, int memorySegmentSize) {

        this.filePath = checkNotNull(filePath);
        this.memorySegmentSize = memorySegmentSize;
    }

    public void writeBuffer(Buffer buffer) throws IOException {
        size +=
                BufferReaderWriterUtil.writeToByteChannel(
                        fileChannel, buffer, headerAndBufferArray);
    }

    public void finishWrite() throws IOException {
        fileChannel.close();
    }

    public Reader createReader(ResultSubpartitionView subpartitionView) throws IOException {
        if (fileChannel != null) {
            checkState(!fileChannel.isOpen());
        }

        final FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ);
        return new FileBufferReader(fc, memorySegmentSize, subpartitionView);
    }

    public long getSize() {
        return size;
    }

    public Path getFilePath() {
        return filePath;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(fileChannel);
        Files.deleteIfExists(filePath);

        LOG.info("Release data partition: " + filePath);
    }

    // ------------------------------------------------------------------------

    public static FileChannelBoundedData create(Path filePath, int memorySegmentSize)
            throws IOException {
        final FileChannel fileChannel =
                FileChannel.open(filePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

        return new FileChannelBoundedData(filePath, fileChannel, memorySegmentSize);
    }

    public static FileChannelBoundedData createForRead(Path filePath, int memorySegmentSize) {
        return new FileChannelBoundedData(filePath, memorySegmentSize);
    }

    // ------------------------------------------------------------------------

    static final class FileBufferReader implements Reader, BufferRecycler {

        private static final int NUM_BUFFERS = 2;

        private final FileChannel fileChannel;

        private ByteBuffer headerBuffer;

        private final ArrayDeque<MemorySegment> buffers;

        private final ResultSubpartitionView subpartitionView;

        /** The tag indicates whether we have read the end of this file. */
        private boolean isFinished;

        FileBufferReader(
                FileChannel fileChannel, int bufferSize, ResultSubpartitionView subpartitionView) {
            this.fileChannel = checkNotNull(fileChannel);
            this.headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();
            this.buffers = new ArrayDeque<>(NUM_BUFFERS);

            for (int i = 0; i < NUM_BUFFERS; i++) {
                buffers.addLast(
                        MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize, null));
            }

            this.subpartitionView = checkNotNull(subpartitionView);
        }

        @Nullable
        @Override
        public Buffer nextBuffer() throws IOException {
            final MemorySegment memory = buffers.pollFirst();
            if (memory == null) {
                throw new FlinkOdpsException("Not enough read buffer size for record!");
            }
            final Buffer next =
                    BufferReaderWriterUtil.readFromByteChannel(
                            fileChannel, headerBuffer, memory, this);
            if (next == null) {
                isFinished = true;
                recycle(memory);
            }

            return next;
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();

            MemorySegment segment;
            while ((segment = buffers.poll()) != null) {
                segment.free();
            }
            headerBuffer = null;
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            buffers.addLast(memorySegment);

            if (!isFinished) {
                subpartitionView.notifyDataAvailable();
            }
        }
    }

    /** A reader to the bounded data. */
    interface Reader extends Closeable {

        @Nullable
        Buffer nextBuffer() throws IOException;
    }
}