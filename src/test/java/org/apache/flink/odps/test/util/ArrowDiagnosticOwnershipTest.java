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
import org.apache.flink.odps.util.ArrowDebugUtils;
import org.junit.Test;

import java.nio.file.*;
import java.util.*;

public class ArrowDiagnosticOwnershipTest {
    @Test
    public void diagnosticCopiesAndFilesPreserveProducerAndOutliveReaders() throws Exception {
        Path file = Files.createTempFile("odps-arrow-diagnostic-", ".arrow");
        try (RootAllocator allocator = new RootAllocator(1024 * 1024)) {
            BigIntVector vector = new BigIntVector("id", allocator);
            vector.allocateNew(2);
            vector.set(0, 42);
            vector.setNull(1);
            vector.setValueCount(2);
            try (VectorSchemaRoot source =
                    new VectorSchemaRoot(Collections.singletonList(vector))) {
                source.setRowCount(2);
                String encoded = ArrowDebugUtils.saveVectorSchemaRootToBase64(source);
                try (VectorSchemaRoot clone =
                                ArrowDebugUtils.cloneVectorSchemaRoot(source, allocator);
                        VectorSchemaRoot decoded =
                                ArrowDebugUtils.loadVectorSchemaRootFromBase64(
                                        encoded, allocator)) {
                    assertEquals(42L, source.getVector(0).getObject(0));
                    assertEquals(42L, clone.getVector(0).getObject(0));
                    assertNull(decoded.getVector(0).getObject(1));
                }
                ArrowDebugUtils.writeVectorSchemaRootToFile(source, file.toString());
                try (VectorSchemaRoot read =
                        ArrowDebugUtils.readVectorSchemaRootFromFile(file.toString())) {
                    assertEquals(42L, read.getVector(0).getObject(0));
                    assertNull(read.getVector(0).getObject(1));
                }
            }
        } finally {
            Files.deleteIfExists(file);
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> ArrowDebugUtils.loadVectorSchemaRootFromBase64("", null));
        assertEquals("", ArrowDebugUtils.saveVectorSchemaRootToBase64(null));
    }
}
