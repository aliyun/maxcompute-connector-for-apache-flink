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

import org.apache.flink.table.api.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.TimeUnit;

/** Static/dynamic partition routing and null partition defaults through Flink SQL. */
@RunWith(Parameterized.class)
public class EmulatorPartitionSqlITCase extends EmulatorFixture {
    private final String writer, routing;

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> cases() {
        List<Object[]> rows = new ArrayList<>();
        for (String writer :
                Arrays.asList("record", "cached", "grouped-record", "grouped-cached", "modern"))
            for (String routing : Arrays.asList("static", "dynamic"))
                rows.add(new Object[] {writer, routing});
        return rows;
    }

    public EmulatorPartitionSqlITCase(String writer, String routing) {
        super("partitions", "BIGINT", "BIGINT", "42", "42");
        this.writer = writer;
        this.routing = routing;
    }

    @Test(timeout = 120000)
    public void sqlRoutesRowsToTheirDeclaredPartitions() throws Exception {
        sql("DROP TABLE " + table);
        sql("CREATE TABLE " + table + "(id BIGINT,v STRING) PARTITIONED BY(ds STRING)");
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().getConfiguration().setInteger("parallelism.default", 2);
        String options = "'sink.retry.delay'='10','sink.partition.default-value'='missing'";
        if (writer.contains("cached")) options += ",'sink.file-cached.enable'='true'";
        if (writer.startsWith("grouped")) options += ",'sink.group-partition.enable'='true'";
        if (writer.equals("modern")) options += ",'sink.coordinator.enable'='false'";
        env.executeSql(
                "CREATE TABLE partition_rows(id BIGINT,v STRING,ds STRING) PARTITIONED BY(ds) WITH"
                        + " ('connector'='maxcompute','table.name'='test_project."
                        + table
                        + "','odps.project.name'='test_project',"
                        + "'odps.end.point'='"
                        + endpoint
                        + "','odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                        + options
                        + ")");
        String insert =
                routing.equals("static")
                        ? "INSERT INTO partition_rows PARTITION(ds='a') VALUES"
                                + " (1,'one'),(2,CAST(NULL AS STRING)),(3,'three')"
                        : "INSERT INTO partition_rows VALUES (1,'one','a'),(2,CAST(NULL AS"
                                + " STRING),'b'),(3,'three',CAST(NULL AS STRING)),(4,'four','a')";
        if (writer.startsWith("grouped") && routing.equals("static")) {
            RuntimeException failure =
                    assertThrows(RuntimeException.class, () -> env.executeSql(insert));
            assertTrue(
                    failure.getMessage(),
                    failure.getMessage().contains("Dynamic partition should be true"));
            assertTrue(odps.tables().get("test_project", table).getPartitions().isEmpty());
            return;
        }
        TableResult result = env.executeSql(insert);
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
        Map<String, Map<Long, String>> expected = new TreeMap<>();
        if (routing.equals("static"))
            expected.put(
                    "a",
                    new TreeMap<Long, String>() {
                        {
                            put(1L, "one");
                            put(2L, null);
                            put(3L, "three");
                        }
                    });
        else {
            expected.put(
                    "a",
                    new TreeMap<Long, String>() {
                        {
                            put(1L, "one");
                            put(4L, "four");
                        }
                    });
            expected.put("b", Collections.singletonMap(2L, null));
            expected.put("missing", Collections.singletonMap(3L, "three"));
        }
        Map<String, Map<Long, String>> actual = new TreeMap<>();
        com.aliyun.odps.Table meta = odps.tables().get("test_project", table);
        for (com.aliyun.odps.Partition partition : meta.getPartitions()) {
            com.aliyun.odps.PartitionSpec spec = partition.getPartitionSpec();
            TableTunnel tunnel = new TableTunnel(odps);
            tunnel.setEndpoint(endpoint);
            TableTunnel.DownloadSession session =
                    tunnel.createDownloadSession("test_project", table, spec);
            Map<Long, String> rows = new TreeMap<>();
            if (session.getRecordCount() > 0)
                try (TunnelRecordReader reader =
                        session.openRecordReader(0, session.getRecordCount())) {
                    com.aliyun.odps.data.Record record;
                    while ((record = reader.read()) != null) {
                        long id = record.getBigint(0);
                        assertFalse("duplicate row", rows.containsKey(id));
                        rows.put(id, record.getString(1));
                    }
                }
            actual.put(spec.get("ds"), rows);
        }
        assertEquals(expected, actual);
    }
}
