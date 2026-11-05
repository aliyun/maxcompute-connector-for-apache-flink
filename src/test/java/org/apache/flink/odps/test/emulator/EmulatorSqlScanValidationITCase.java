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
import org.junit.Test;

/** Validate SQL startup parameters before any read session can be created. */
public class EmulatorSqlScanValidationITCase extends EmulatorFixture {
    public EmulatorSqlScanValidationITCase() {
        super("scanvalidation", "BIGINT", "BIGINT", "42", "42");
    }

    @Test
    public void invalidStreamingAndIncrementalParametersFailDuringPlanning() throws Exception {
        sql("DROP TABLE " + table);
        sql(
                "CREATE TABLE "
                        + table
                        + "(id BIGINT,v BIGINT,PRIMARY KEY(id))"
                        + " TBLPROPERTIES('transactional'='true','write.bucket.num'='1')");
        String[][] cases = {
            {"true", "'scan.mode'='from-snapshot'", "scan.snapshot-version"},
            {"true", "'scan.mode'='from-timestamp'", "scan.timestamp"},
            {"true", "'scan.mode'='from-snapshot-full'", "FROM_SNAPSHOT_FULL"},
            {"true", "'scan.mode'='incremental'", "streaming mode"},
            {"true", "'scan.mode'='latest','continuousDiscoveryInterval'='1 s'", "30 seconds"},
            {"false", "'scan.mode'='incremental'", "incremental-between"},
            {
                "false",
                "'scan.mode'='incremental','scan.incremental-between'='1'",
                "incremental-between"
            },
            {
                "false",
                "'scan.mode'='incremental','scan.incremental-between-timestamp'='2026-10-12'",
                "incremental-between"
            }
        };
        for (String[] scenario : cases) {
            TableEnvironment env = environment(Boolean.parseBoolean(scenario[0]), scenario[1]);
            Exception failure =
                    assertThrows(Exception.class, () -> env.explainSql("SELECT * FROM scan_rows"));
            assertTrue(
                    messages(failure),
                    messages(failure).toLowerCase().contains(scenario[2].toLowerCase()));
        }
    }

    private TableEnvironment environment(boolean streaming, String options) {
        TableEnvironment env =
                TableEnvironment.create(
                        streaming
                                ? EnvironmentSettings.inStreamingMode()
                                : EnvironmentSettings.inBatchMode());
        env.executeSql(
                "CREATE TABLE scan_rows(id BIGINT NOT NULL,v BIGINT,PRIMARY KEY(id) NOT ENFORCED)"
                        + " WITH ('connector'='maxcompute','table.name'='test_project."
                        + table
                        + "','odps.project.name'='test_project','odps.end.point'='"
                        + endpoint
                        + "','odps.tunnel.end.point'='"
                        + endpoint
                        + "','odps.access.id'='test-ak','odps.access.key'='test-sk',"
                        + options
                        + ")");
        return env;
    }

    private String messages(Throwable failure) {
        StringBuilder text = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause())
            text.append(cause.getMessage()).append(" ");
        return text.toString();
    }
}
