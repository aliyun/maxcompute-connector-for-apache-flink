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

import com.aliyun.odps.*;
import com.aliyun.odps.type.*;

import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.partition.PartitionComputer;
import org.apache.flink.odps.table.OdpsTablePath;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

/** Partition values, qualified table identity and schema ordering are public data contracts. */
public class PartitionRuntimeValueContractTest {
    @Test
    public void dynamicEmptyPartitionsUseDefaultInSingleAndCompositeKeys() {
        RowType schema =
                RowType.of(
                        new LogicalType[] {new VarCharType(), new VarCharType()},
                        new String[] {"ds", "region"});
        for (String staticPartition : new String[] {"", "region='fixed'"}) {
            PartitionComputer computer =
                    PartitionComputer.instance(
                            schema,
                            schema,
                            Arrays.asList("ds", "region"),
                            staticPartition,
                            "missing");
            for (String value : new String[] {null, "", "中文😀", " "}) {
                GenericRowData row =
                        GenericRowData.of(
                                value == null ? null : StringData.fromString(value),
                                StringData.fromString(""));
                LinkedHashMap<String, String> values = computer.generatePartValues(row, null);
                assertEquals(
                        value == null || value.isEmpty() ? "missing" : value, values.get("ds"));
                assertEquals(staticPartition.isEmpty() ? "missing" : "fixed", values.get("region"));
                // The configured default is a connector routing decision; SDK still rejects empty
                // specs.
                assertEquals(
                        values,
                        OdpsUtils.getPartitionSpecKVMap(
                                new PartitionSpec(OdpsUtils.generatePartition(values))));
            }
        }
    }

    @Test
    public void partitionIdentityRetainsOrderUnicodeAndNumericBoundaries() {
        PartitionSpec spec = new PartitionSpec("region='中文',ds='2026-10-12'");
        LinkedHashMap<String, String> values = OdpsUtils.getPartitionSpecKVMap(spec);
        assertEquals(Arrays.asList("region", "ds"), new ArrayList<>(values.keySet()));
        assertEquals(
                values,
                OdpsUtils.getPartitionSpecKVMap(
                        new PartitionSpec(OdpsUtils.generatePartition(values))));
        assertEquals("中文", OdpsUtils.convertPartitionColumn("中文", TypeInfoFactory.STRING));
        assertEquals(
                (byte) -128, OdpsUtils.convertPartitionColumn("-128", TypeInfoFactory.TINYINT));
        assertEquals(
                (short) -32768,
                OdpsUtils.convertPartitionColumn("-32768", TypeInfoFactory.SMALLINT));
        assertEquals(
                Integer.MIN_VALUE,
                OdpsUtils.convertPartitionColumn("-2147483648", TypeInfoFactory.INT));
        assertEquals(
                Long.MAX_VALUE,
                OdpsUtils.convertPartitionColumn("9223372036854775807", TypeInfoFactory.BIGINT));
        assertEquals(
                "中文",
                OdpsUtils.convertPartitionColumn("中文", TypeInfoFactory.getVarcharTypeInfo(4))
                        .toString());
        assertEquals(
                "xy",
                OdpsUtils.convertPartitionColumn("xy", TypeInfoFactory.getCharTypeInfo(4))
                        .toString()
                        .trim());
        assertThrows(
                NumberFormatException.class,
                () -> OdpsUtils.convertPartitionColumn("128", TypeInfoFactory.TINYINT));
        assertThrows(
                FlinkOdpsException.class,
                () -> OdpsUtils.convertPartitionColumn("true", TypeInfoFactory.BOOLEAN));
        assertEquals(
                "00ff80",
                OdpsUtils.objToString(ByteBuffer.wrap(new byte[] {0, (byte) 255, (byte) 128})));
        assertNull(OdpsUtils.objToString(null));
    }

    @Test
    public void schemaKeepsPartitionColumnsAfterDataAndCodecNamesAreCaseInsensitive() {
        TableSchema schema = new TableSchema();
        schema.addColumn(new Column("id", TypeInfoFactory.BIGINT));
        schema.addColumn(new Column("arr", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)));
        schema.addPartitionColumn(new Column("ds", TypeInfoFactory.STRING));
        RowType row = OdpsUtils.getFlinkSchema(schema);
        assertEquals(Arrays.asList("id", "arr", "ds"), row.getFieldNames());
        assertEquals("BIGINT", row.getTypeAt(0).asSummaryString());
        assertEquals("ARRAY<INT>", row.getTypeAt(1).asSummaryString());
        assertEquals(
                com.aliyun.odps.tunnel.io.CompressOption.CompressAlgorithm.ODPS_RAW,
                OdpsUtils.compressOptionOf("RAW").algorithm);
        assertEquals(
                com.aliyun.odps.tunnel.io.CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME,
                OdpsUtils.compressOptionOf("Lz4").algorithm);
        assertEquals(
                com.aliyun.odps.tunnel.io.CompressOption.CompressAlgorithm.ODPS_SNAPPY,
                OdpsUtils.compressOptionOf("snappy").algorithm);
        assertEquals(
                com.aliyun.odps.tunnel.io.CompressOption.CompressAlgorithm.ODPS_ZLIB,
                OdpsUtils.compressOptionOf("unknown").algorithm);
    }

    @Test
    public void qualificationSeparatesProjectSchemaAndTableAndRejectsAmbiguity() {
        OdpsTablePath plain = OdpsTablePath.fromTablePath("project.table");
        assertEquals(new OdpsTablePath("project", "table"), plain);
        assertEquals(plain.hashCode(), new OdpsTablePath("project", "table").hashCode());
        assertEquals(
                new OdpsTablePath("project", "schema", "table"),
                OdpsTablePath.fromTablePath("project.schema.table"));
        assertEquals(
                new OdpsTablePath("default", "schema", "table"),
                OdpsTablePath.fromTablePath("default", "schema.table", true));
        assertEquals(
                new OdpsTablePath("default", "table"),
                OdpsTablePath.fromTablePath("default", "table", false));
        assertNotEquals(plain, new OdpsTablePath("other", "table"));
        assertNotEquals(plain, new OdpsTablePath("project", "schema", "table"));
        assertNotEquals(plain, "project.table");
        for (String bad : Arrays.asList("table", "p.s.t.extra", "p.."))
            assertThrows(IllegalArgumentException.class, () -> OdpsTablePath.fromTablePath(bad));
        assertThrows(
                IllegalArgumentException.class,
                () -> OdpsTablePath.fromTablePath("p", "p.s.t", false));
        assertThrows(
                IllegalArgumentException.class,
                () -> OdpsTablePath.fromTablePath("p", "table", true));
    }

    @Test(timeout = 5000)
    public void executorShutdownWaitsForWorkAndRetainsCancellation() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<Integer> work = worker.submit(() -> 42);
        OdpsUtils.shutdownExecutorService(worker);
        assertEquals(Integer.valueOf(42), work.get());
        assertTrue(worker.isTerminated());
        OdpsUtils.shutdownExecutorService(null);
        ExecutorService cancelled = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        cancelled.submit(
                () -> {
                    started.countDown();
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException expected) {
                        Thread.currentThread().interrupt();
                    }
                });
        assertTrue(started.await(1, TimeUnit.SECONDS));
        Thread.currentThread().interrupt();
        try {
            OdpsUtils.shutdownExecutorService(cancelled);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            cancelled.shutdownNow();
            assertTrue(cancelled.awaitTermination(1, TimeUnit.SECONDS));
        }
    }
}
