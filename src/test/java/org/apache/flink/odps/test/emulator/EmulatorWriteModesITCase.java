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

/** The same wide SQL journey validates each supported writer implementation. */
public class EmulatorWriteModesITCase extends EmulatorSqlRoundTripITCase {
    private final String mode;

    @Parameterized.Parameters(name = "{0}/{1}")
    public static Collection<Object[]> scenarios() {
        List<Object[]> rows = new ArrayList<>();
        for (String mode :
                Arrays.asList(
                        "record",
                        "cached",
                        "arrow",
                        "standalone-record",
                        "standalone-arrow",
                        "stream-record",
                        "stream-arrow",
                        "modern")) {
            for (int parallelism : Arrays.asList(1, 2)) rows.add(new Object[] {mode, parallelism});
        }
        return rows;
    }

    public EmulatorWriteModesITCase(String mode, int parallelism) {
        super(
                mode,
                new String[] {
                    "ROW_OFFSET",
                    "NO_COMPRESSION",
                    parallelism == 1 ? "1" : "3",
                    Integer.toString(parallelism),
                    "false",
                    "all",
                    "rows"
                });
        this.mode = mode;
    }

    @Override
    protected String writerOptions() {
        String common = "'insert.arrow-writer.batch-size'='1','sink.retry.delay'='10'";
        switch (mode) {
            case "cached":
                return common + ",'sink.file-cached.enable'='true'";
            case "arrow":
                return common + ",'insert.arrow-writer.enable'='true'";
            case "standalone-record":
                return common + ",'sink.standalone.enable'='true'";
            case "standalone-arrow":
                return common
                        + ",'sink.standalone.enable'='true','insert.arrow-writer.enable'='true'";
            case "stream-record":
                return common + ",'insert.streaming-tunnel.enable'='true'";
            case "stream-arrow":
                return common
                        + ",'insert.streaming-tunnel.enable'='true','insert.arrow-writer.enable'='true'";
            case "modern":
                return common + ",'sink.coordinator.enable'='false'";
            default:
                return common;
        }
    }
}
