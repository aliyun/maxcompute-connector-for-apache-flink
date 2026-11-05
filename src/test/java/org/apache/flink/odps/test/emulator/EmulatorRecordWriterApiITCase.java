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

package org.apache.flink.odps.test.emulator;

import static org.junit.Assert.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.writer.*;
import org.apache.flink.odps.table.OdpsOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/** Public writer lifecycle contract: flushing, checkpoint commit and session recovery. */
@RunWith(Parameterized.class)
public class EmulatorRecordWriterApiITCase extends EmulatorFixture {
    private final boolean upsert;

    @Parameterized.Parameters(name = "upsert={0}")
    public static Collection<Object[]> cases() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    public EmulatorRecordWriterApiITCase(boolean upsert) {
        super("writerapi", "BIGINT", "BIGINT", "42", "42");
        this.upsert = upsert;
    }

    @Test(timeout = 90000)
    public void commitsAcrossResetAndRecoveredSessions() throws Exception {
        if (upsert) {
            sql("DROP TABLE " + table);
            sql(
                    "CREATE TABLE "
                            + table
                            + "(id BIGINT,v BIGINT,PRIMARY KEY(id))"
                            + " TBLPROPERTIES('transactional'='true','write.bucket.num'='1')");
        }
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(endpoint);
        Configuration config = new Configuration();
        config.set(OdpsOptions.RETRY_DELAY_MILLIS, 10L);
        TableIdentifier identifier = TableIdentifier.of("test_project", "default", table);
        OdpsRecordWriter writer =
                upsert
                        ? new UpsertSessionWriter(tunnel, identifier, "", config)
                        : new UploadSessionWriter(tunnel, identifier, "", config);
        try {
            String first = writer.getId();
            assertNotNull(first);
            writer.addElement(record(1, 10, writer.getId()));
            writer.flush();
            writer.reset();
            assertNotEquals(first, writer.getId());
            String resumed = writer.getId();
            writer.close();
            if (upsert) assertSdkClosed(writer);
            writer.recover(resumed);
            writer.flush(true);
            writer.addElement(record(2, 20, writer.getId()));
            writer.flush(false);
            if (!upsert) {
                ((UploadSessionWriter) writer).reInitializedResource();
                writer.addElement(record(3, 30, writer.getId()));
            }
            writer.finish(9);
            writer.close();
            if (upsert) assertSdkClosed(writer);
        } finally {
            writer.close();
        }
        TableTunnel.DownloadSession session = tunnel.createDownloadSession("test_project", table);
        assertEquals(upsert ? 2 : 3, session.getRecordCount());
        Map<Long, Long> actual = new HashMap<>();
        try (TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount())) {
            Record row;
            while ((row = reader.read()) != null) actual.put(row.getBigint(0), row.getBigint(1));
        }
        Map<Long, Long> expected = new HashMap<>();
        expected.put(1L, 10L);
        expected.put(2L, 20L);
        if (!upsert) expected.put(3L, 30L);
        assertEquals(expected, actual);
    }

    private static void assertSdkClosed(OdpsRecordWriter writer) throws Exception {
        // Observe the real SDK-owned executors; a closed stream alone is insufficient.
        java.lang.reflect.Field owned = UpsertSessionWriter.class.getDeclaredField("upsertSession");
        owned.setAccessible(true);
        Object session = owned.get(writer);
        for (String name : Arrays.asList("scheduler", "group")) {
            java.lang.reflect.Field field = session.getClass().getDeclaredField(name);
            field.setAccessible(true);
            java.util.concurrent.ExecutorService executor =
                    (java.util.concurrent.ExecutorService) field.get(session);
            assertTrue("SDK resource still running: " + name, executor.isTerminated());
        }
    }

    private Record record(long id, long value, String sessionId) throws Exception {
        com.aliyun.odps.Column[] columns =
                odps.tables()
                        .get("test_project", table)
                        .getSchema()
                        .getColumns()
                        .toArray(new com.aliyun.odps.Column[0]);
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(endpoint);
        Record row;
        if (upsert) {
            com.aliyun.odps.tunnel.impl.UpsertSessionImpl recordSession =
                    new com.aliyun.odps.tunnel.impl.UpsertSessionImpl.Builder()
                            .setConfig(tunnel.getConfig())
                            .setProjectName("test_project")
                            .setSchemaName("default")
                            .setTableName(table)
                            .setUpsertId(sessionId)
                            .build();
            try {
                row = recordSession.newRecord();
            } finally {
                recordSession.close();
            }
        } else {
            row = new com.aliyun.odps.data.ArrayRecord(columns);
        }
        row.setBigint(0, id);
        row.setBigint(1, value);
        return row;
    }
}
