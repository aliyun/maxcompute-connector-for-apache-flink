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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsPipeline;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/** Public RowData pipeline API, which is not reachable through a SQL statement. */
@RunWith(Parameterized.class)
public class EmulatorDataStreamApiITCase extends EmulatorFixture {
    private final boolean upsert;

    @Parameterized.Parameters(name = "upsert={0}")
    public static Collection<Object[]> cases() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    public EmulatorDataStreamApiITCase(boolean upsert) {
        super("pipeline", "BIGINT", "BIGINT", "42", "42");
        this.upsert = upsert;
    }

    @Test(timeout = 90000)
    public void rowDataPipelineCommitsAndPreservesValues() throws Exception {
        if (upsert) {
            sql("DROP TABLE " + table);
            sql(
                    "CREATE TABLE "
                            + table
                            + "(id BIGINT,v BIGINT,PRIMARY KEY(id))"
                            + " TBLPROPERTIES('transactional'='true','write.bucket.num'='1')");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        RowType type =
                RowType.of(
                        new LogicalType[] {new BigIntType(), new BigIntType()},
                        new String[] {"id", "v"});
        List<RowData> input =
                Arrays.asList(
                        GenericRowData.of(1L, (Long.MIN_VALUE + 1)),
                        GenericRowData.of(2L, null),
                        GenericRowData.of(3L, Long.MAX_VALUE));
        Configuration conf = new Configuration();
        conf.set(OdpsOptions.RETRY_DELAY_MILLIS, 10L);
        OdpsPipeline.builder()
                .odpsConf(new OdpsConf("test-ak", "test-sk", endpoint, "test_project", endpoint))
                .tableName(table)
                .configuration(conf)
                .sink(env.fromCollection(input, InternalTypeInfo.of(type)), true);
        env.execute("public RowData pipeline contract");
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(endpoint);
        TableTunnel.DownloadSession session = tunnel.createDownloadSession("test_project", table);
        assertEquals(3, session.getRecordCount());
        Map<Long, Long> actual = new HashMap<>();
        try (TunnelRecordReader reader = session.openRecordReader(0, 3)) {
            com.aliyun.odps.data.Record row;
            while ((row = reader.read()) != null) actual.put(row.getBigint(0), row.getBigint(1));
        }
        Map<Long, Long> expected = new HashMap<>();
        expected.put(1L, (Long.MIN_VALUE + 1));
        expected.put(2L, null);
        expected.put(3L, Long.MAX_VALUE);
        assertEquals(expected, actual);
    }
}
