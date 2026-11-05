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
 */

package org.apache.flink.odps.sink.cache;

import static org.junit.Assert.*;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/** Spill encoding must preserve boundaries and fail truncated state without inventing rows. */
public class SpillBufferContractTest {
    @Test
    public void memoryBufferRespectsWrittenLimitWithUnusedCapacity() {
        Buffer original = buffer(new byte[] {0, 1, -1}, true);
        ByteBuffer memory = ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder());
        try {
            assertFalse(BufferReaderWriterUtil.writeBuffer(original, ByteBuffer.allocate(2)));
            assertTrue(BufferReaderWriterUtil.writeBuffer(original, memory));
            memory.flip();
            Buffer read = BufferReaderWriterUtil.sliceNextBuffer(memory);
            try {
                assertEquals(3, read.getSize());
                assertTrue(read.isCompressed());
                assertArrayEquals(new byte[] {0, 1, -1}, bytes(read));
            } finally {
                read.recycleBuffer();
            }
            assertNull(BufferReaderWriterUtil.sliceNextBuffer(memory));
        } finally {
            original.recycleBuffer();
        }
    }

    @Test
    public void fileRoundTripAndCorruptionBoundaries() throws Exception {
        Path path = Files.createTempFile("odps-spill-contract-", ".bin");
        Buffer original = buffer(new byte[] {2, 3, 4}, false);
        try (FileChannel channel =
                FileChannel.open(
                        path,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer[] buffers = BufferReaderWriterUtil.allocatedWriteBufferArray();
            assertEquals(
                    -1,
                    BufferReaderWriterUtil.writeToByteChannelIfBelowSize(
                            channel, original, buffers, 10));
            assertEquals(
                    11,
                    BufferReaderWriterUtil.writeToByteChannelIfBelowSize(
                            channel, original, buffers, 11));
            channel.position(0);
            Buffer read =
                    BufferReaderWriterUtil.readFromByteChannel(
                            channel,
                            BufferReaderWriterUtil.allocatedHeaderBuffer(),
                            MemorySegmentFactory.allocateUnpooledSegment(32),
                            FreeingBufferRecycler.INSTANCE);
            try {
                assertArrayEquals(new byte[] {2, 3, 4}, bytes(read));
            } finally {
                read.recycleBuffer();
            }
            assertNull(
                    BufferReaderWriterUtil.readFileRegionFromByteChannel(
                            channel, BufferReaderWriterUtil.allocatedHeaderBuffer()));
            channel.position(0);
            Buffer region =
                    BufferReaderWriterUtil.readFileRegionFromByteChannel(
                            channel, BufferReaderWriterUtil.allocatedHeaderBuffer());
            assertEquals(3, region.getSize());
            assertEquals(11, channel.position());
            region.recycleBuffer();
            ByteBuffer payload = ByteBuffer.allocate(3);
            BufferReaderWriterUtil.readByteBufferFully(channel, payload, 8);
            assertArrayEquals(new byte[] {2, 3, 4}, payload.array());
            channel.truncate(10);
            channel.position(0);
            assertThrows(
                    IOException.class,
                    () ->
                            BufferReaderWriterUtil.readFromByteChannel(
                                    channel,
                                    BufferReaderWriterUtil.allocatedHeaderBuffer(),
                                    MemorySegmentFactory.allocateUnpooledSegment(32),
                                    FreeingBufferRecycler.INSTANCE));
            channel.truncate(2);
            channel.position(0);
            assertThrows(
                    IOException.class,
                    () ->
                            BufferReaderWriterUtil.readFileRegionFromByteChannel(
                                    channel, BufferReaderWriterUtil.allocatedHeaderBuffer()));
        } finally {
            original.recycleBuffer();
            Files.deleteIfExists(path);
        }
    }

    private static Buffer buffer(byte[] bytes, boolean compressed) {
        return new NetworkBuffer(
                MemorySegmentFactory.wrap(bytes),
                FreeingBufferRecycler.INSTANCE,
                Buffer.DataType.DATA_BUFFER,
                compressed,
                bytes.length);
    }

    private static byte[] bytes(Buffer buffer) {
        byte[] bytes = new byte[buffer.getSize()];
        buffer.getNioBufferReadable().get(bytes);
        return bytes;
    }
}
