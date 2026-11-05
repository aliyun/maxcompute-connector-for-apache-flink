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

/** Numeric limits, Unicode truncation, pre-epoch time and nested nulls through SQL. */
public class EmulatorSqlBoundaryITCase extends EmulatorSqlRoundTripITCase {
    private final String writer;

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> scenarios() {
        List<Object[]> cases = new ArrayList<>();
        for (String writer : Arrays.asList("arrow", "modern"))
            for (String split : Arrays.asList("ROW_OFFSET", "BYTE_SIZE"))
                cases.add(new Object[] {writer, split});
        return cases;
    }

    public EmulatorSqlBoundaryITCase(String writer, String split) {
        super(writer, new String[] {split, "ZSTD", "1", "2", "false", "all", "rows"});
        this.writer = writer;
    }

    @Override
    protected String writerOptions() {
        return "'insert.arrow-writer.batch-size'='1','sink.retry.delay'='10',"
                + (writer.equals("modern")
                        ? "'sink.coordinator.enable'='false'"
                        : "'insert.arrow-writer.enable'='true'");
    }

    @Override
    protected Collection<Object[]> testTypes() {
        List<Object[]> types = new ArrayList<>(super.testTypes());
        types.addAll(
                Arrays.asList(
                        new Object[][] {
                            {
                                "bigint_min",
                                "BIGINT",
                                "BIGINT",
                                "-9223372036854775807",
                                "-9223372036854775807"
                            },
                            {
                                "bigint_max",
                                "BIGINT",
                                "BIGINT",
                                "9223372036854775807",
                                "9223372036854775807"
                            },
                            {"char_pad", "CHAR(4)", "CHAR(4)", "'xy'", "xy"},
                            {"varchar_unicode", "VARCHAR(3)", "VARCHAR(3)", "'中文测试'", "中文测"},
                            {
                                "date_before_epoch",
                                "DATE",
                                "DATE",
                                "DATE '1960-01-02'",
                                "1960-01-02"
                            },
                            {
                                "timestamp_before_epoch",
                                "TIMESTAMP",
                                "TIMESTAMP(6)",
                                "TIMESTAMP '1960-01-02 03:04:05.123456'",
                                "1960-01-02T03:04:05.123456"
                            },
                            {
                                "decimal_max",
                                "DECIMAL(38,8)",
                                "DECIMAL(38,8)",
                                "'999999999999999999999999999999.12345678'",
                                "999999999999999999999999999999.12345678"
                            },
                            {
                                "map_null",
                                "MAP<STRING,BIGINT>",
                                "MAP<STRING,BIGINT>",
                                "MAP['k',CAST(NULL AS BIGINT)]",
                                "{k=null}"
                            },
                            {
                                "nested_struct",
                                "STRUCT<a:ARRAY<BIGINT>,b:STRING>",
                                "ROW<a ARRAY<BIGINT>,b STRING>",
                                "ROW(ARRAY[CAST(1 AS BIGINT),CAST(NULL AS BIGINT)],CAST(NULL AS"
                                        + " STRING))",
                                "+I[[1, null], null]"
                            }
                        }));
        return types;
    }
}
