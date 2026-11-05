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

package org.apache.flink.odps.test.util;

import static org.junit.Assert.*;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.flink.core.memory.*;
import org.apache.flink.odps.sink.serializer.VectorSchemaRootSerializer;
import org.junit.Test;

import java.util.Arrays;

/** State and network copies must own their buffers after the producer is closed. */
public class ArrowStateOwnershipTest {
    @Test
    public void copiedAndRestoredBatchesRemainReadableAfterProducerClose() throws Exception {
        VectorSchemaRootSerializer serializer = new VectorSchemaRootSerializer();
        try (RootAllocator allocator = new RootAllocator(1024 * 1024)) {
            BigIntVector values = new BigIntVector("id", allocator);
            values.allocateNew(3);
            values.set(0, Long.MIN_VALUE);
            values.setNull(1);
            values.set(2, Long.MAX_VALUE);
            values.setValueCount(3);
            VectorSchemaRoot source = new VectorSchemaRoot(Arrays.asList(values));
            source.setRowCount(3);
            DataOutputSerializer bytes = new DataOutputSerializer(256);
            serializer.serialize(source, bytes);
            try (VectorSchemaRoot copy = serializer.copy(source)) {
                assertEquals(Long.MIN_VALUE, source.getVector(0).getObject(0));
                source.close();
                assertBatch(copy);
                try (VectorSchemaRoot restored =
                        serializer.deserialize(
                                new DataInputDeserializer(bytes.getCopyOfBuffer()))) {
                    assertBatch(restored);
                    DataOutputSerializer forwarded = new DataOutputSerializer(256);
                    serializer.copy(new DataInputDeserializer(bytes.getCopyOfBuffer()), forwarded);
                    assertArrayEquals(bytes.getCopyOfBuffer(), forwarded.getCopyOfBuffer());
                    try (VectorSchemaRoot reused =
                            serializer.deserialize(
                                    serializer.copy(restored),
                                    new DataInputDeserializer(forwarded.getCopyOfBuffer()))) {
                        assertBatch(reused);
                    }
                }
            }
        }
        assertNotNull(serializer.duplicate());
        assertTrue(
                serializer.snapshotConfiguration().restoreSerializer()
                        instanceof VectorSchemaRootSerializer);
        assertTrue(
                serializer
                        .snapshotConfiguration()
                        .resolveSchemaCompatibility(serializer)
                        .isCompatibleAsIs());
    }

    private static void assertBatch(VectorSchemaRoot root) {
        assertEquals(3, root.getRowCount());
        assertEquals(Long.MIN_VALUE, root.getVector(0).getObject(0));
        assertNull(root.getVector(0).getObject(1));
        assertEquals(Long.MAX_VALUE, root.getVector(0).getObject(2));
    }
}
