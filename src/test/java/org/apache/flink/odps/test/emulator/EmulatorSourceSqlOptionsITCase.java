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

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/** SQL pruning, static partition, watermark and disk-buffer options share one journey. */
@RunWith(Parameterized.class)
public class EmulatorSourceSqlOptionsITCase extends EmulatorFixture {
    private final String routing;
    private final boolean streaming;

    @Parameterized.Parameters(name = "{0}/streaming={1}")
    public static Collection<Object[]> cases() {
        List<Object[]> cases = new ArrayList<>();
        for (String routing : Arrays.asList("pruned", "static", "all"))
            for (boolean streaming : new boolean[] {false, true})
                cases.add(new Object[] {routing, streaming});
        return cases;
    }

    public EmulatorSourceSqlOptionsITCase(String routing, boolean streaming) {
        super("sourceoptions", "BIGINT", "BIGINT", "42", "42");
        this.routing = routing;
        this.streaming = streaming;
    }

    @Test(timeout = 90000)
    public void readsOnlySelectedPartitionsWithProjectionAndLimit() throws Exception {
        sql("DROP TABLE " + table);
        sql("CREATE TABLE " + table + "(id BIGINT,v BIGINT) PARTITIONED BY(ds STRING)");
        sql("ALTER TABLE " + table + " ADD PARTITION(ds='a') PARTITION(ds='b')");
        sql("INSERT INTO " + table + " PARTITION(ds='a') VALUES (1,10),(2,20)");
        sql("INSERT INTO " + table + " PARTITION(ds='b') VALUES (3,30)");
        TableEnvironment env =
                TableEnvironment.create(
                        streaming
                                ? EnvironmentSettings.inStreamingMode()
                                : EnvironmentSettings.inBatchMode());
        String options =
                "'splitstrategy'='ROW_OFFSET','dynamicloadbalancing'='true','splitbatchreadsize'='1','scan.disk-spill.enabled'='true','scan.disk-spill.memory-capacity'='1'";
        if (routing.equals("static")) options += ",'scan.partition'='ds=a'";
        String watermark =
                streaming
                        ? ",ts AS TO_TIMESTAMP_LTZ(id,3),WATERMARK FOR ts AS ts - INTERVAL '1'"
                                + " SECOND"
                        : "";
        env.executeSql(
                "CREATE TABLE rows_to_read(id BIGINT,v BIGINT,ds STRING"
                        + watermark
                        + ") PARTITIONED BY(ds) WITH"
                        + " ('connector'='maxcompute','table.name'='test_project."
                        + table
                        + "','odps.project.name'='test_project','odps.end.point'='"
                        + endpoint
                        + "','odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                        + options
                        + ")");
        String query =
                "SELECT id,v FROM rows_to_read"
                        + (routing.equals("pruned") ? " WHERE ds='a'" : "")
                        + " LIMIT 2";
        List<Row> actual = new ArrayList<>();
        try (CloseableIterator<Row> rows = env.executeSql(query).collect()) {
            while (rows.hasNext()) actual.add(rows.next());
        }
        assertEquals(2, actual.size());
        for (Row row : actual) assertEquals(((Long) row.getField(0)) * 10L, row.getField(1));
        if (!routing.equals("all"))
            assertEquals(
                    new HashSet<>(Arrays.asList(Row.of(1L, 10L), Row.of(2L, 20L))),
                    new HashSet<>(actual));
    }
}
