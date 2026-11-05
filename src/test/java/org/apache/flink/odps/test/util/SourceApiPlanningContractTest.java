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

package org.apache.flink.odps.test.util;

import static org.junit.Assert.*;

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.*;
import com.aliyun.odps.type.TypeInfoFactory;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.source.*;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.*;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.time.ZoneOffset;
import java.util.*;

/** Public source construction and latest-partition startup contracts; no CDC service claim. */
public class SourceApiPlanningContractTest {
    @Test
    public void validatesPartitionVisibilityAndStartupBeforeOpeningAnyServiceSession()
            throws Exception {
        OdpsConf conf = new OdpsConf("dummy", "dummy", "http://127.0.0.1:1", "test");
        conf.setProperty("scan.partition.latest", "true");
        assertThrows(
                IllegalArgumentException.class,
                () -> source(conf, true, Collections.emptyList(), OdpsStartupMode.LATEST));
        conf.setProperty("scan.partition.ready-mode", "partition-visible");
        assertThrows(
                IllegalArgumentException.class,
                () -> source(conf, false, Collections.emptyList(), OdpsStartupMode.LATEST));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        source(
                                conf,
                                true,
                                Collections.singletonList("ds='a'"),
                                OdpsStartupMode.LATEST));
        assertThrows(
                IllegalArgumentException.class,
                () -> source(conf, true, Collections.emptyList(), OdpsStartupMode.LATEST_FULL));
        for (OdpsStartupMode mode :
                Arrays.asList(OdpsStartupMode.LATEST, OdpsStartupMode.FROM_SNAPSHOT)) {
            ContinuousOdpsSource source = source(conf, true, Collections.emptyList(), mode);
            assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());
            assertNotNull(source.getEnumeratorCheckpointSerializer());
            try (SourceReader<?, ?> reader = source.createReader(context())) {
                assertEquals(
                        "latest-partition reader must persist its epoch marker",
                        1,
                        reader.snapshotState(1).size());
            }
        }
    }

    @Test
    public void nonPartitionedSourceConstructsReaderAndClosesWithoutStartingFetchers()
            throws Exception {
        OdpsConf conf = new OdpsConf("dummy", "dummy", "http://127.0.0.1:1", "test");
        ContinuousOdpsSource source =
                source(conf, false, Collections.emptyList(), OdpsStartupMode.LATEST);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());
        assertNotNull(source.getProducedType());
        try (SourceReader<?, ?> reader = source.createReader(context())) {
            assertTrue(reader.snapshotState(1).isEmpty());
        }
        source.newSplitReaderSupplier().get().close();
    }

    private static ContinuousOdpsSource source(
            OdpsConf conf, boolean partitioned, List<String> partitions, OdpsStartupMode mode) {
        return new ContinuousOdpsSource(
                conf,
                partitioned,
                TableIdentifier.of("test", "default", "table"),
                partitions,
                new OdpsColumn[] {new OdpsColumn("id", TypeInfoFactory.BIGINT)},
                partitioned
                        ? new OdpsColumn[] {new OdpsColumn("ds", TypeInfoFactory.STRING, true)}
                        : null,
                InternalTypeInfo.of(RowType.of(new BigIntType())),
                64,
                CompressionCodec.NO_COMPRESSION,
                OdpsSplitStrategy.BUCKET,
                0,
                2,
                1000,
                mode,
                IncrementalOptions.newBuilder()
                        .startVersion(2L)
                        .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC)
                        .build(),
                ZoneOffset.UTC);
    }

    private static SourceReaderContext context() {
        return (SourceReaderContext)
                Proxy.newProxyInstance(
                        SourceApiPlanningContractTest.class.getClassLoader(),
                        new Class[] {SourceReaderContext.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "getConfiguration":
                                    return new Configuration();
                                case "metricGroup":
                                    return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
                                case "getIndexOfSubtask":
                                    return 0;
                                case "getLocalHostName":
                                    return "localhost";
                                default:
                                    return null;
                            }
                        });
    }
}
