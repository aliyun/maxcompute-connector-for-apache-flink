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
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.source.partition;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.*;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.odps.source.reader.OdpsRecordEmitter;
import org.apache.flink.odps.source.reader.OdpsSourceReader;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

/** Exercises the real Flink multiplex reader/fetcher, without a remote service or credentials. */
public class LatestPartitionFlinkReaderTest {
    static final class BufferedSplitReader implements SplitReader<RowData, OdpsSourceSplit> {
        final int value;
        String id;
        boolean sent;
        volatile boolean closed;
        BufferedSplitReader(int value) { this.value = value; }
        public RecordsWithSplitIds<RowData> fetch() {
            final String current = id;
            if (sent) {
                return org.apache.flink.odps.source.reader.OdpsRecords.finishedSplit(current);
            }
            sent = true;
            return new RecordsWithSplitIds<RowData>() {
                boolean selected;
                int remaining = 100;
                public String nextSplit() { if (selected) { return null; } selected=true; return current; }
                public RowData nextRecordFromSplit() { return remaining-- > 0 ? GenericRowData.of(value) : null; }
                public Set<String> finishedSplits() { return Collections.emptySet(); }
            };
        }
        public void handleSplitsChanges(SplitsChange<OdpsSourceSplit> change) { id=change.splits().get(0).splitId(); }
        public void wakeUp() {}
        public void close() { closed=true; }
    }

    @Test public void realFetcherDoesNotDrainOldBufferedBatchOnSwitch() throws Exception {
        List<PartitionEvent> events = new ArrayList<>(); List<BufferedSplitReader> fetchers = new ArrayList<>();
        SourceReaderContext context = (SourceReaderContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[]{SourceReaderContext.class}, (proxy, method, args) -> {
                    switch(method.getName()) {
                        case "getConfiguration": return new Configuration();
                        case "getIndexOfSubtask": return 0;
                        case "metricGroup": return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
                        case "sendSourceEventToCoordinator": events.add((PartitionEvent) args[0]); return null;
                        default: return null;
                    }
                });
        AtomicInteger generation = new AtomicInteger();
        LatestPartitionReader reader = new LatestPartitionReader(context, () -> {
            int value=generation.incrementAndGet();
            return new OdpsSourceReader(() -> {
                BufferedSplitReader fetcher=new BufferedSplitReader(value); fetchers.add(fetcher); return fetcher;
            },new OdpsRecordEmitter(),new Configuration(),context);
        });
        LatestPartitionReaderTest.Output output = new LatestPartitionReaderTest.Output();
        try {
            reader.start(); reader.handleSourceEvents(PartitionEvent.switchTo(1));
            reader.addSplits(Collections.singletonList(LatestPartitionReaderTest.split(1)));
            reader.isAvailable().get(5,TimeUnit.SECONDS); reader.pollNext(output);
            assertEquals(Collections.singletonList(1),output.rows);
            reader.handleSourceEvents(PartitionEvent.switchTo(2));
            assertTrue(fetchers.get(0).closed); assertTrue(output.outputs.isEmpty());
            reader.addSplits(Collections.singletonList(LatestPartitionReaderTest.split(2)));
            reader.isAvailable().get(5,TimeUnit.SECONDS); reader.pollNext(output);
            assertEquals(Arrays.asList(1,2),output.rows);
            assertEquals(2, events.get(events.size()-1).epoch);
        } finally { reader.close(); }
    }
}
