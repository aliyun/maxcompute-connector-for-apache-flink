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
import com.aliyun.odps.tunnel.impl.UpsertSessionImpl;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.writer.UpsertSessionWriter;
import org.apache.flink.odps.table.OdpsOptions;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;

/** The public record writer must also bound the SDK's lost-connection wait. */
public class EmulatorPublicUpsertFaultITCase extends EmulatorWriteHarness {
    @Test(timeout = 90000)
    public void lostAckFailsInBoundedTimeAndRecoveredSessionConverges() throws Exception {
        sql("DROP TABLE " + table);
        sql(
                "CREATE TABLE "
                        + table
                        + "(id BIGINT,v BIGINT,PRIMARY KEY(id))"
                        + " TBLPROPERTIES('transactional'='true','write.bucket.num'='1')");
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(endpoint);
        Configuration config = new Configuration();
        config.set(OdpsOptions.READ_TIMEOUT, Duration.ofSeconds(3));
        config.set(OdpsOptions.RETRY_DELAY_MILLIS, 10L);
        TableIdentifier identifier = TableIdentifier.of("test_project", "default", table);
        UpsertSessionWriter writer = new UpsertSessionWriter(tunnel, identifier, "", config);
        String sessionId = writer.getId();
        UpsertSessionImpl recordSession =
                new UpsertSessionImpl.Builder()
                        .setConfig(tunnel.getConfig())
                        .setProjectName("test_project")
                        .setSchemaName("default")
                        .setTableName(table)
                        .setUpsertId(sessionId)
                        .build();
        Record record;
        try {
            record = recordSession.newRecord();
            record.setBigint(0, 1L);
            record.setBigint(1, 10L);
        } finally {
            recordSession.close();
        }
        String rule = table + "_lost";
        try {
            control(
                    "PUT",
                    "/__test/faults/" + rule,
                    "{\"match\":{\"plane\":\"tunnel\",\"object\":\"upsert\",\"action\":\"write\",\"table\":\""
                            + table
                            + "\"},\"effect\":{\"type\":\"lost_ack\"},\"times\":1}");
            writer.addElement(record);
            long started = System.nanoTime();
            IOException error = assertThrows(IOException.class, writer::flush);
            assertTrue(error.toString().contains("exceeded"));
            assertTrue(
                    java.util.concurrent.TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started)
                            < 30);
            assertEquals(1, control("GET", "/__test/faults/" + rule, null).get("hits").getAsInt());
            assertFalse(Thread.currentThread().isInterrupted());
            writer.recover(sessionId);
            writer.addElement(record);
            writer.finish(1);
            writer.close();
            TableTunnel.DownloadSession result =
                    tunnel.createDownloadSession("test_project", table);
            assertEquals(1L, result.getRecordCount());
            try (com.aliyun.odps.tunnel.io.TunnelRecordReader reader =
                    result.openRecordReader(0, 1)) {
                Record actual = reader.read();
                assertEquals(Long.valueOf(1), actual.getBigint(0));
                assertEquals(Long.valueOf(10), actual.getBigint(1));
                assertNull(reader.read());
            }
        } finally {
            writer.close();
            control("DELETE", "/__test/faults/" + rule, null);
        }
    }
}
