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

import org.junit.runners.Parameterized;

import java.util.*;

/**
 * Native typed fixtures avoid Flink literal-codegen limits; the tested pipeline is INSERT SELECT.
 */
public class EmulatorArraySqlITCase extends EmulatorSqlRoundTripITCase {
    private final String writer;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> scenarios() {
        return Arrays.asList(new Object[][] {{"arrow"}, {"modern"}, {"cached"}});
    }

    public EmulatorArraySqlITCase(String writer) {
        super(writer, new String[] {"ROW_OFFSET", "ZSTD", "1", "2", "false", "all", "rows"});
        this.writer = writer;
    }

    @Override
    protected String writerOptions() {
        return "'insert.arrow-writer.batch-size'='1','sink.retry.delay'='10',"
                + (writer.equals("modern")
                        ? "'sink.coordinator.enable'='false'"
                        : writer.equals("cached")
                                ? "'sink.file-cached.enable'='true'"
                                : "'insert.arrow-writer.enable'='true'");
    }

    @Override
    protected boolean nativeFixtures() {
        return true;
    }

    @Override
    protected Collection<Object[]> testTypes() {
        List<Object[]> types = new ArrayList<>(super.testTypes());
        types.add(
                new Object[] {
                    "array_tiny",
                    "ARRAY<TINYINT>",
                    "ARRAY<TINYINT>",
                    "array(-127,NULL)",
                    "[-127, null]"
                });
        types.add(
                new Object[] {
                    "array_small",
                    "ARRAY<SMALLINT>",
                    "ARRAY<SMALLINT>",
                    "array(-32768,NULL)",
                    "[-32768, null]"
                });
        types.add(
                new Object[] {
                    "array_int",
                    "ARRAY<INT>",
                    "ARRAY<INT>",
                    "array(-2147483648,NULL)",
                    "[-2147483648, null]"
                });
        types.add(
                new Object[] {
                    "array_boolean",
                    "ARRAY<BOOLEAN>",
                    "ARRAY<BOOLEAN>",
                    "array(TRUE,NULL)",
                    "[true, null]"
                });
        types.add(
                new Object[] {
                    "array_float",
                    "ARRAY<FLOAT>",
                    "ARRAY<FLOAT>",
                    "array(1.25,NULL)",
                    "[1.25, null]"
                });
        types.add(
                new Object[] {
                    "array_double",
                    "ARRAY<DOUBLE>",
                    "ARRAY<DOUBLE>",
                    "array(-1.5,NULL)",
                    "[-1.5, null]"
                });
        types.add(
                new Object[] {
                    "array_unicode",
                    "ARRAY<STRING>",
                    "ARRAY<STRING>",
                    "array('中文',NULL)",
                    "[中文, null]"
                });
        types.add(
                new Object[] {
                    "array_char",
                    "ARRAY<CHAR(4)>",
                    "ARRAY<CHAR(4)>",
                    "array('xy',NULL)",
                    "[xy, null]"
                });
        types.add(
                new Object[] {
                    "array_varchar",
                    "ARRAY<VARCHAR(3)>",
                    "ARRAY<VARCHAR(3)>",
                    "array('中文测',NULL)",
                    "[中文测, null]"
                });
        types.add(
                new Object[] {
                    "array_decimal",
                    "ARRAY<DECIMAL(38,8)>",
                    "ARRAY<DECIMAL(38,8)>",
                    "array(CAST('999999999999999999999999999999.12345678' AS DECIMAL(38,8)),NULL)",
                    "[999999999999999999999999999999.12345678, null]"
                });
        types.add(
                new Object[] {
                    "array_date",
                    "ARRAY<DATE>",
                    "ARRAY<DATE>",
                    "array(DATE '1960-01-02',NULL)",
                    "[1960-01-02, null]"
                });
        types.add(
                new Object[] {
                    "array_timestamp",
                    "ARRAY<TIMESTAMP>",
                    "ARRAY<TIMESTAMP(6)>",
                    "array(TIMESTAMP '1960-01-02 03:04:05.123456',NULL)",
                    "[1960-01-02T03:04:05.123456, null]"
                });
        types.add(
                new Object[] {
                    "array_binary",
                    "ARRAY<BINARY>",
                    "ARRAY<VARBINARY(4)>",
                    "array(unhex('00ff'),NULL)",
                    "[[0, -1], null]"
                });
        types.add(
                new Object[] {
                    "array_nested",
                    "ARRAY<ARRAY<BIGINT>>",
                    "ARRAY<ARRAY<BIGINT>>",
                    "array(array(1,NULL),NULL)",
                    "[[1, null], null]"
                });
        types.add(
                new Object[] {
                    "array_map",
                    "ARRAY<MAP<STRING,BIGINT>>",
                    "ARRAY<MAP<STRING,BIGINT>>",
                    "array(map(array('k'),array(9)),NULL)",
                    "[{k=9}, null]"
                });
        types.add(
                new Object[] {
                    "array_row",
                    "ARRAY<STRUCT<x:BIGINT,y:STRING>>",
                    "ARRAY<ROW<x BIGINT,y STRING>>",
                    "array(named_struct('x',8,'y','Value'),NULL)",
                    "[+I[8, Value], null]"
                });
        return types;
    }
}
