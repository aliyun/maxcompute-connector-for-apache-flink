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

package org.apache.flink.odps.source.partition;

import static org.junit.Assert.*;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.read.*;

import org.apache.flink.odps.source.*;
import org.apache.flink.odps.source.enumerator.*;
import org.apache.flink.odps.util.OdpsConf;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/** Pure enumerator state-machine tests; only metadata/session responses are fixtures. */
public class ContinuousEnumeratorWindowContractTest {
    @Test
    public void advancesVersionAndTransactionCursorAndAvoidsRepeatedScans() throws Exception {
        for (OdpsStartupMode mode :
                Arrays.asList(
                        OdpsStartupMode.LATEST,
                        OdpsStartupMode.LATEST_FULL,
                        OdpsStartupMode.FROM_SNAPSHOT,
                        OdpsStartupMode.FROM_TIMESTAMP)) {
            LatestPartitionEnumeratorTest.Harness h = new LatestPartitionEnumeratorTest.Harness();
            IncrementalOptions options =
                    mode == OdpsStartupMode.FROM_TIMESTAMP
                            ? IncrementalOptions.newBuilder()
                                    .startTimeStamp("2026-10-12 00:00:00")
                                    .build()
                            : IncrementalOptions.newBuilder().startVersion(2L).build();
            Legacy e = new Legacy(h, mode, options);
            e.tick();
            assertEquals(Long.valueOf(4), e.snapshotState(1).currentSnapshotId());
            assertEquals(mode == OdpsStartupMode.LATEST_FULL ? 1 : 0, e.batchScans);
            assertEquals(mode == OdpsStartupMode.LATEST_FULL ? 0 : 1, e.incrementalScans);
            e.tick();
            assertEquals(1, e.batchScans + e.incrementalScans);
            e.latest = 5L;
            e.tick();
            assertEquals(Long.valueOf(5), e.snapshotState(2).currentSnapshotId());
            for (int reader : h.readers.keySet()) e.handleSplitRequest(reader, "localhost");
            assertFalse(h.assigned.isEmpty());
            e.addSplitsBack(Collections.singletonList(LatestPartitionReaderTest.split(0)), 0);
            e.close();
        }
    }

    @Test
    public void defersFutureStartsAndRejectsInvalidModeTypePairs() throws Exception {
        LatestPartitionEnumeratorTest.Harness h = new LatestPartitionEnumeratorTest.Harness();
        Legacy future =
                new Legacy(
                        h,
                        OdpsStartupMode.FROM_SNAPSHOT,
                        IncrementalOptions.newBuilder().startVersion(9L).build());
        future.tick();
        assertNull(future.snapshotState(1).currentSnapshotId());
        assertEquals(0, future.incrementalScans);
        future.latest = null;
        future.tick();
        assertEquals(0, future.incrementalScans);
        Legacy invalid =
                new Legacy(
                        h,
                        OdpsStartupMode.FROM_TIMESTAMP,
                        IncrementalOptions.newBuilder().startVersion(2L).build());
        assertThrows(IOException.class, invalid::tick);
        Legacy unsupported =
                new Legacy(
                        h,
                        OdpsStartupMode.INCREMENTAL,
                        IncrementalOptions.newBuilder().startVersion(2L).build());
        assertThrows(IOException.class, unsupported::tick);
    }

    static class Legacy extends ContinuousOdpsEnumerator {
        Long latest = 4L;
        int batchScans, incrementalScans;

        Legacy(
                LatestPartitionEnumeratorTest.Harness h,
                OdpsStartupMode mode,
                IncrementalOptions options) {
            super(
                    h.context,
                    new OdpsConf("dummy", "dummy", "http://127.0.0.1:1", "test"),
                    TableIdentifier.of("test", "default", "table"),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    OdpsSplitStrategy.BUCKET,
                    0,
                    10,
                    30000,
                    mode,
                    options,
                    null);
        }

        @Override
        protected TableSchema readTableSchema() {
            return new TableSchema();
        }

        @Override
        protected Long latestSnapshotVersion() {
            return latest;
        }

        @Override
        protected TableReadSessionBuilder sessionBuilder(List<String> partitions) {
            return new TableReadSessionBuilder() {
                public TableBatchReadSession buildBatchReadSession() {
                    batchScans++;
                    return LatestPartitionEnumeratorTest.session("batch", latest, 2);
                }

                public TableIncrementalReadSession buildIncrementalReadSession() {
                    incrementalScans++;
                    return LatestPartitionEnumeratorTest.session("incremental" + latest, latest, 2);
                }
            };
        }

        void tick() throws IOException {
            processDiscoveredSplits(scanNextSnapshot(), null);
        }
    }
}
