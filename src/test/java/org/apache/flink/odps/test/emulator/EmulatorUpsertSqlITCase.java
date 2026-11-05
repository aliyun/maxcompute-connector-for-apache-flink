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

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.TimeUnit;

/** PK updates and deletes reach the service through the Flink SQL planner. */
@RunWith(Parameterized.class)
public class EmulatorUpsertSqlITCase extends EmulatorFixture {
    private final String mode;
    private final boolean partial;

    @Parameterized.Parameters(name = "{0}/partial={1}")
    public static Collection<Object[]> cases() {
        List<Object[]> rows = new ArrayList<>();
        for (String mode : Arrays.asList("record", "cached", "modern"))
            for (boolean partial : Arrays.asList(false, true))
                rows.add(new Object[] {mode, partial});
        return rows;
    }

    public EmulatorUpsertSqlITCase(String mode, boolean partial) {
        super("upsert", "BIGINT", "BIGINT", "42", "42");
        this.mode = mode;
        this.partial = partial;
    }

    @Test(timeout = 180000)
    public void sqlUpsertReplacesOrPreservesColumnsAndDeletesKeys() throws Exception {
        sql("DROP TABLE " + table);
        sql(
                "CREATE TABLE "
                        + table
                        + "(id BIGINT,score BIGINT,note STRING,PRIMARY KEY(id)) "
                        + "TBLPROPERTIES('transactional'='true','write.bucket.num'='1','acid.partial.fields.update.enable'='true')");
        TableEnvironment batch = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        createSink(batch, false);
        awaitJob(batch.executeSql("INSERT INTO pk_rows VALUES (1,10,'keep'),(2,20,'remove')"));
        assertEquals(
                new TreeMap<Long, String>() {
                    {
                        put(1L, "10/keep");
                        put(2L, "20/remove");
                    }
                },
                readback());
        TableEnvironment updates = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        createSink(updates, partial);
        if (mode.equals("modern") && partial) {
            RuntimeException error =
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    updates.executeSql(
                                            "INSERT INTO pk_rows VALUES (1,30,'changed')"));
            Throwable cause = error;
            while (cause.getCause() != null) cause = cause.getCause();
            assertTrue(
                    cause.getMessage(),
                    cause.getMessage().contains("Partial-column writes require"));
            assertEquals(
                    new TreeMap<Long, String>() {
                        {
                            put(1L, "10/keep");
                            put(2L, "20/remove");
                        }
                    },
                    readback());
            return;
        }
        awaitJob(updates.executeSql("INSERT INTO pk_rows VALUES (1,30,'changed'),(3,40,'added')"));
        Map<Long, String> expected = new TreeMap<>();
        expected.put(1L, partial ? "30/keep" : "30/changed");
        expected.put(2L, "20/remove");
        expected.put(3L, "40/added");
        assertEquals(expected, readback());

        StreamExecutionEnvironment stream = StreamExecutionEnvironment.getExecutionEnvironment();
        stream.setParallelism(2);
        StreamTableEnvironment sqlEnv = StreamTableEnvironment.create(stream);
        createSink(sqlEnv, partial);
        List<Row> changes =
                Arrays.asList(
                        Row.ofKind(RowKind.DELETE, 2L, null, null),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1L, 31L, "updated"));
        org.apache.flink.api.java.typeutils.RowTypeInfo type =
                new org.apache.flink.api.java.typeutils.RowTypeInfo(
                        new org.apache.flink.api.common.typeinfo.TypeInformation<?>[] {
                            org.apache.flink.api.common.typeinfo.Types.LONG,
                            org.apache.flink.api.common.typeinfo.Types.LONG,
                            org.apache.flink.api.common.typeinfo.Types.STRING
                        },
                        new String[] {"id", "score", "note"});
        Table input =
                sqlEnv.fromChangelogStream(
                        stream.fromCollection(changes, type),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("score", DataTypes.BIGINT())
                                .column("note", DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        sqlEnv.createTemporaryView("changes", input);
        awaitJob(sqlEnv.executeSql("INSERT INTO pk_rows SELECT id,score,note FROM changes"));
        expected.remove(2L);
        expected.put(1L, partial ? "31/keep" : "31/updated");
        assertEquals(expected, readback());
    }

    private void createSink(TableEnvironment env, boolean partialWrite) {
        String options = "'sink.operation'='upsert','sink.retry.delay'='10'";
        if (mode.equals("cached")) options += ",'sink.file-cached.enable'='true'";
        if (mode.equals("modern")) options += ",'sink.coordinator.enable'='false'";
        if (partialWrite)
            options +=
                    ",'upsert.partial-column.enable'='true','upsert.partial-column.name'='score'";
        env.executeSql(
                "CREATE TABLE pk_rows(id BIGINT,score BIGINT,note STRING,PRIMARY KEY(id) NOT"
                        + " ENFORCED) WITH ('connector'='maxcompute','table.name'='test_project."
                        + table
                        + "','odps.project.name'='test_project',"
                        + "'odps.end.point'='"
                        + endpoint
                        + "','odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                        + options
                        + ")");
    }

    private Map<Long, String> readback() throws Exception {
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(endpoint);
        TableTunnel.DownloadSession session = tunnel.createDownloadSession("test_project", table);
        Map<Long, String> result = new TreeMap<>();
        if (session.getRecordCount() > 0)
            try (TunnelRecordReader reader =
                    session.openRecordReader(0, session.getRecordCount())) {
                com.aliyun.odps.data.Record record;
                while ((record = reader.read()) != null)
                    assertNull(
                            "duplicate PK",
                            result.put(
                                    record.getBigint(0),
                                    record.getBigint(1) + "/" + record.getString(2)));
            }
        return result;
    }

    private static void awaitJob(TableResult result) throws Exception {
        try {
            result.await(60, TimeUnit.SECONDS);
        } catch (Exception error) {
            try {
                result.getJobClient().get().cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception cleanup) {
                error.addSuppressed(cleanup);
            }
            throw error;
        }
    }
}
