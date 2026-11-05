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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.TimeUnit;

/** Wide typed rows exercise an actual Flink SQL read/write pipeline per parameter scenario. */
@RunWith(Parameterized.class)
public class EmulatorSqlRoundTripITCase extends EmulatorFixture {
    private final String[] parameters;
    private String output;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> scenarios() throws Exception {
        List<Object[]> result = new ArrayList<>();
        for (String[] row : SqlParameterMatrix.scenarios())
            result.add(new Object[] {String.join("/", row), row});
        java.nio.file.Path path = java.nio.file.Paths.get("target/sql-parameter-matrix.csv");
        java.nio.file.Files.createDirectories(path.getParent());
        List<String> rows = new ArrayList<>();
        rows.add("split,compression,batch_rows,parallelism,file_cache,query,data");
        for (Object[] scenario : result) rows.add(String.join(",", (String[]) scenario[1]));
        java.nio.file.Files.write(path, rows, java.nio.charset.StandardCharsets.UTF_8);
        return result;
    }

    public EmulatorSqlRoundTripITCase(String label, String[] parameters) {
        super("sqlmatrix", "BIGINT", "BIGINT", "42", "42");
        this.parameters = parameters;
    }

    @Test(timeout = 180000)
    public void flinkSqlReadsTransformsAndWritesAllTypes() throws Exception {
        List<Object[]> dataTypes = new ArrayList<>(testTypes());
        StringBuilder odpsSchema = new StringBuilder("id BIGINT");
        StringBuilder flinkSchema = new StringBuilder("id BIGINT");
        StringBuilder values = new StringBuilder("1");
        StringBuilder nulls = new StringBuilder("2");
        for (Object[] type : dataTypes) {
            String column = "c_" + type[0];
            odpsSchema.append(", ").append(column).append(' ').append(type[1]);
            flinkSchema.append(", ").append(column).append(' ').append(type[2]);
            values.append(", CAST(")
                    .append(flinkLiteral(type))
                    .append(" AS ")
                    .append(type[2])
                    .append(')');
            nulls.append(", CAST(NULL AS ").append(type[2]).append(')');
        }
        sql("DROP TABLE " + table);
        sql("CREATE TABLE " + table + " (" + odpsSchema + ")");
        output = table + "_out";
        sql("CREATE TABLE " + output + " (" + odpsSchema + ")");
        try {
            TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
            env.getConfig()
                    .getConfiguration()
                    .setInteger("parallelism.default", Integer.parseInt(parameters[3]));
            createSqlTable(env, "input_rows", table, flinkSchema.toString());
            createSqlTable(env, "output_rows", output, flinkSchema.toString());
            boolean empty = parameters[6].equals("empty");
            boolean filtered = parameters[5].equals("filter");
            if (!empty) {
                String third = "3" + values.substring(1);
                if (nativeFixtures()) {
                    StringBuilder nativeValues = new StringBuilder("1");
                    for (Object[] type : dataTypes) nativeValues.append(',').append(type[3]);
                    String nativeNulls =
                            "2" + String.join("", Collections.nCopies(dataTypes.size(), ",NULL"));
                    sql(
                            "INSERT INTO "
                                    + table
                                    + " VALUES ("
                                    + nativeValues
                                    + "),("
                                    + nativeNulls
                                    + "),(3"
                                    + nativeValues.substring(1)
                                    + ")");
                } else
                    awaitJob(
                            env.executeSql(
                                    "INSERT INTO input_rows VALUES ("
                                            + values
                                            + "),("
                                            + nulls
                                            + "),("
                                            + third
                                            + ")"));
            }
            awaitJob(
                    env.executeSql(
                            "INSERT INTO output_rows SELECT * FROM input_rows"
                                    + (filtered ? " WHERE id <> 2" : "")));
            int count = empty ? 0 : filtered ? 2 : 3;
            verifyTunnelReadback(dataTypes, count, filtered);
            try (CloseableIterator<Row> rows =
                    env.executeSql("SELECT COUNT(*), SUM(id), COUNT(c_string) FROM output_rows")
                            .collect()) {
                assertTrue(rows.hasNext());
                Row aggregate = rows.next();
                assertEquals((long) count, aggregate.getField(0));
                assertEquals(empty ? null : filtered ? 4L : 6L, aggregate.getField(1));
                assertEquals(empty ? 0L : 2L, aggregate.getField(2));
                assertFalse(rows.hasNext());
            }
        } finally {
            sql("DROP TABLE IF EXISTS " + output);
            assertFalse(odps.tables().exists("test_project", output));
        }
    }

    private static void awaitJob(TableResult result) throws Exception {
        try {
            result.await(60, TimeUnit.SECONDS);
        } catch (Exception failure) {
            try {
                result.getJobClient()
                        .orElseThrow(() -> new AssertionError("Missing SQL job client"))
                        .cancel()
                        .get(30, TimeUnit.SECONDS);
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private void createSqlTable(TableEnvironment env, String name, String physical, String schema) {
        env.executeSql(
                "CREATE TABLE "
                        + name
                        + " ("
                        + schema
                        + ") WITH ("
                        + "'connector'='maxcompute','table.name'='test_project."
                        + physical
                        + "',"
                        + "'odps.project.name'='test_project','odps.end.point'='"
                        + endpoint
                        + "',"
                        + "'odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                        + "'splitstrategy'='"
                        + parameters[0]
                        + "',"
                        + (parameters[1].equals("NO_COMPRESSION")
                                ? ""
                                : "'compressCodec'='" + parameters[1] + "',")
                        + "'splitbatchreadsize'='"
                        + parameters[2]
                        + "',"
                        + writerOptions()
                        + ")");
    }

    protected Collection<Object[]> testTypes() {
        return types();
    }

    protected boolean nativeFixtures() {
        return false;
    }

    protected String writerOptions() {
        return "'sink.file-cached.enable'='" + parameters[4] + "'";
    }

    private void verifyTunnelReadback(List<Object[]> dataTypes, int expectedCount, boolean filtered)
            throws Exception {
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(endpoint);
        TableTunnel.DownloadSession session = tunnel.createDownloadSession("test_project", output);
        assertEquals(expectedCount, session.getRecordCount());
        if (expectedCount == 0) return;
        Set<Long> ids = new HashSet<>();
        try (TunnelRecordReader reader = session.openRecordReader(0, expectedCount)) {
            com.aliyun.odps.data.Record record;
            while ((record = reader.read()) != null) {
                long id = record.getBigint(0);
                assertTrue("duplicate row", ids.add(id));
                for (int i = 0; i < dataTypes.size(); i++) {
                    Object[] type = dataTypes.get(i);
                    Object value =
                            type[1].equals("STRING") ? record.getString(i + 1) : record.get(i + 1);
                    if (id == 2) assertNull(type[0].toString(), value);
                    else
                        assertEquals(
                                type[0].toString(),
                                type[4],
                                normalizeValue(value, type[2].toString()));
                }
            }
        }
        assertEquals(
                filtered
                        ? new HashSet<>(Arrays.asList(1L, 3L))
                        : new HashSet<>(Arrays.asList(1L, 2L, 3L)),
                ids);
    }
}
