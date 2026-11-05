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

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;

import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.junit.*;

import java.net.URI;
import java.util.*;

/** Actual connector -> public SDK -> loopback emulator contracts; no cloud identity. */
public abstract class EmulatorFixture {
    public static Collection<Object[]> types() {
        return Arrays.asList(
                new Object[][] {
                    {"bigint", "BIGINT", "BIGINT", "42", "42"},
                    {"integer", "INT", "INT", "7", "7"},
                    {"smallint", "SMALLINT", "SMALLINT", "3", "3"},
                    {"tinyint", "TINYINT", "TINYINT", "2", "2"},
                    {"string", "STRING", "STRING", "'hello'", "hello"},
                    {"unicode", "STRING", "STRING", "'中文'", "中文"},
                    {"boolean", "BOOLEAN", "BOOLEAN", "true", "true"},
                    {"double", "DOUBLE", "DOUBLE", "1.5", "1.5"},
                    {"float", "FLOAT", "FLOAT", "1.25", "1.25"},
                    {"decimal", "DECIMAL(10,2)", "DECIMAL(10,2)", "12.34", "12.34"},
                    {"date", "DATE", "DATE", "DATE '2026-10-12'", "2026-10-12"},
                    {
                        "datetime",
                        "DATETIME",
                        "TIMESTAMP(3)",
                        "CAST('2026-10-12 08:30:00.123' AS DATETIME)",
                        "2026-10-12T08:30:00.123"
                    },
                    {
                        "timestamp",
                        "TIMESTAMP",
                        "TIMESTAMP(6)",
                        "TIMESTAMP '2026-10-12 08:30:00.123456'",
                        "2026-10-12T08:30:00.123456"
                    },
                    {"array", "ARRAY<BIGINT>", "ARRAY<BIGINT>", "array(1,2,NULL)", "[1, 2, null]"},
                    {
                        "map",
                        "MAP<STRING,BIGINT>",
                        "MAP<STRING,BIGINT>",
                        "map(array('a'),array(9))",
                        "{a=9}"
                    },
                    {
                        "struct",
                        "STRUCT<x:BIGINT,y:STRING>",
                        "ROW<x BIGINT,y STRING>",
                        "named_struct('x',8,'y','Value')",
                        "+I[8, Value]"
                    },
                    {"binary", "BINARY", "VARBINARY(4)", "unhex('00ff')", "[0, -1]"}
                });
    }

    protected final String name, odpsType, flinkType, literal, expected;
    protected Odps odps;
    protected OdpsCatalog catalog;
    protected String table, endpoint;

    protected EmulatorFixture(String n, String o, String f, String l, String e) {
        name = n;
        odpsType = o;
        flinkType = f;
        literal = l;
        expected = e;
    }

    @Before
    public void setup() throws Exception {
        endpoint = System.getProperty("emulator.endpoint", "");
        URI uri = URI.create(endpoint);
        assertEquals("Only an isolated loopback emulator is allowed", "http", uri.getScheme());
        assertEquals("127.0.0.1", uri.getHost());
        odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
        odps.setEndpoint(endpoint);
        odps.setTunnelEndpoint(endpoint);
        odps.setDefaultProject("test_project");
        odps.setCurrentSchema("default");
        table = "flink_em_" + name + "_" + UUID.randomUUID().toString().replace("-", "");
        sql("CREATE TABLE " + table + " (id BIGINT, v " + odpsType + ")");
        catalog =
                new OdpsCatalog(
                        "mc",
                        "test_project",
                        new OdpsConf("test-ak", "test-sk", endpoint, "test_project", endpoint));
        catalog.open();
    }

    protected void sql(String q) throws Exception {
        SQLTask.run(odps, q).waitForSuccess();
    }

    @After
    public void cleanup() throws Exception {
        if (odps != null && table != null) {
            sql("DROP TABLE IF EXISTS " + table);
            assertFalse("Emulator fixture leaked", odps.tables().exists("test_project", table));
        }
        if (catalog != null) catalog.close();
    }

    protected String sinkLiteral() {
        return flinkLiteral(new Object[] {name, odpsType, flinkType, literal, expected});
    }

    protected static String flinkLiteral(Object[] type) {
        String name = (String) type[0];
        String literal = (String) type[3];
        if (name.equals("datetime")) return "'2026-10-12 08:30:00.123'";
        if (name.equals("array"))
            return "ARRAY[CAST(1 AS BIGINT),CAST(2 AS BIGINT),CAST(NULL AS BIGINT)]";
        if (name.equals("map")) return "MAP['a',CAST(9 AS BIGINT)]";
        if (name.equals("struct")) return "ROW(CAST(8 AS BIGINT),'Value')";
        if (name.equals("binary")) return "X'00FF'";
        return literal;
    }

    protected String normalize(Object value) {
        return normalizeValue(value, flinkType);
    }

    protected static String normalizeValue(Object value, String flinkType) {
        if (value instanceof List)
            return ((List<?>) value)
                    .stream()
                            .map(item -> normalizeValue(item, flinkType))
                            .collect(java.util.stream.Collectors.toList())
                            .toString();
        if (value instanceof com.aliyun.odps.data.Binary)
            return Arrays.toString(((com.aliyun.odps.data.Binary) value).data());
        if (value instanceof java.time.ZonedDateTime)
            return ((java.time.ZonedDateTime) value).toInstant().toString().replace("Z", "");
        if (value instanceof byte[]) return Arrays.toString((byte[]) value);
        if (value instanceof Object[]) return Arrays.deepToString((Object[]) value);
        if (value instanceof Map) return new TreeMap<>((Map<?, ?>) value).toString();
        if (value instanceof java.sql.Date) return ((java.sql.Date) value).toLocalDate().toString();
        if (value instanceof com.aliyun.odps.data.Struct)
            return Row.of(((com.aliyun.odps.data.Struct) value).getFieldValues().toArray())
                    .toString();
        if (value instanceof java.time.Instant) return value.toString().replace("Z", "");
        if (value instanceof java.util.Date)
            return ((java.util.Date) value).toInstant().toString().replace("Z", "");
        String text = String.valueOf(value);
        return flinkType.startsWith("TIMESTAMP") ? text.replace(' ', 'T') : text;
    }

    protected String extraOptions() {
        return "";
    }

    protected TableEnvironment environment() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().getConfiguration().setInteger("parallelism.default", 1);
        env.executeSql(
                "CREATE TABLE mc_rows (id BIGINT, v "
                        + flinkType
                        + ") WITH ("
                        + "'connector'='maxcompute','table.name'='test_project."
                        + table
                        + "',"
                        + "'odps.project.name'='test_project','odps.end.point'='"
                        + endpoint
                        + "',"
                        + "'odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk'"
                        + extraOptions()
                        + ")");
        return env;
    }
}
