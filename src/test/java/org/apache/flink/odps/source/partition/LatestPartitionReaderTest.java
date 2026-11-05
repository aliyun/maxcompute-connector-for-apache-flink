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
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.source.split.OdpsSourceSplitSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.Test;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static org.junit.Assert.*;

public class LatestPartitionReaderTest {
    static class Delegate implements SourceReader<RowData, OdpsSourceSplit> {
        final List<OdpsSourceSplit> splits = new ArrayList<>();
        final CompletableFuture<Void> available = new CompletableFuture<>();
        boolean closed;
        int value;
        Delegate(int value) { this.value = value; }
        public void start() {}
        public InputStatus pollNext(ReaderOutput<RowData> output) {
            if (!splits.isEmpty()) { output.createOutputForSplit(splits.get(0).splitId()).collect(GenericRowData.of(value)); }
            return InputStatus.NOTHING_AVAILABLE;
        }
        public List<OdpsSourceSplit> snapshotState(long checkpointId) { return new ArrayList<>(splits); }
        public CompletableFuture<Void> isAvailable() { return available; }
        public void addSplits(List<OdpsSourceSplit> added) { splits.addAll(added); }
        public void notifyNoMoreSplits() {}
        public void close() { closed = true; }
    }
    static class Output implements ReaderOutput<RowData> {
        final List<Integer> rows = new ArrayList<>();
        final Set<String> outputs = new HashSet<>();
        public void collect(RowData row) { rows.add(row.getInt(0)); }
        public void collect(RowData row, long t) { collect(row); }
        public void emitWatermark(Watermark w) {}
        public void markIdle() {}
        public void markActive() {}
        public SourceOutput<RowData> createOutputForSplit(String id) { outputs.add(id); return this; }
        public void releaseOutputForSplit(String id) { outputs.remove(id); }
    }
    static SourceReaderContext context(List<PartitionEvent> events) {
        return (SourceReaderContext) Proxy.newProxyInstance(LatestPartitionReaderTest.class.getClassLoader(),
                new Class[]{SourceReaderContext.class}, (p,m,a) -> {
                    if (m.getName().equals("sendSourceEventToCoordinator")) { events.add((PartitionEvent) a[0]); }
                    return null;
                });
    }
    static OdpsSourceSplit split(long epoch) {
        return new OdpsSourceSplit(PartitionEvent.prefix(epoch) + "session/0", 0, null, null, 7);
    }

    @Test public void switchClosesBufferedReaderBeforeAckAndReleasesWatermarkOutput() throws Exception {
        List<PartitionEvent> events = new ArrayList<>(); List<Delegate> delegates = new ArrayList<>();
        LatestPartitionReader reader = new LatestPartitionReader(context(events), () -> {
            Delegate d = new Delegate(delegates.size() + 1); delegates.add(d); return d;
        });
        reader.start(); reader.handleSourceEvents(PartitionEvent.switchTo(1)); reader.addSplits(Collections.singletonList(split(1)));
        Output output = new Output(); reader.pollNext(output);
        CompletableFuture<Void> waiting = reader.isAvailable(); assertFalse(waiting.isDone());
        reader.handleSourceEvents(PartitionEvent.switchTo(2));
        assertTrue(delegates.get(0).closed); assertTrue(output.outputs.isEmpty()); assertTrue(waiting.isDone());
        assertEquals(2, events.get(events.size()-1).epoch);
        reader.addSplits(Arrays.asList(split(1), split(2))); reader.pollNext(output);
        assertEquals(Arrays.asList(1, 2), output.rows); // old prefetched value is never emitted again
        assertEquals(1, delegates.get(1).splits.size());
        reader.close(); assertTrue(delegates.get(1).closed);
    }

    @Test public void matchingRestoreRetainsOffsetsButDoesNotEmitBeforeHandshake() throws Exception {
        List<PartitionEvent> events = new ArrayList<>(); Delegate delegate = new Delegate(1);
        LatestPartitionReader reader = new LatestPartitionReader(context(events), () -> delegate);
        OdpsSourceSplitSerializer serializer = new OdpsSourceSplitSerializer();
        OdpsSourceSplit marker = serializer.deserialize(serializer.getVersion(), serializer.serialize(PartitionEvent.marker(4)));
        reader.addSplits(Arrays.asList(marker, split(4))); reader.start();
        Output output = new Output(); assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(output)); assertTrue(output.rows.isEmpty());
        reader.handleSourceEvents(PartitionEvent.switchTo(4));
        assertEquals(4, events.get(0).epoch);
        assertEquals(7, delegate.splits.get(0).getRecordsToSkip());
        assertEquals(2, reader.snapshotState(2).size());
        reader.close();
    }

    @Test public void restoreDuringSwitchDiscardsOldGenerationAndStaleEvents() throws Exception {
        List<PartitionEvent> events = new ArrayList<>(); Delegate delegate = new Delegate(2);
        LatestPartitionReader reader = new LatestPartitionReader(context(events), () -> delegate);
        reader.addSplits(Arrays.asList(PartitionEvent.marker(1), split(1)));
        reader.handleSourceEvents(PartitionEvent.switchTo(2));
        assertTrue(delegate.splits.isEmpty()); assertEquals(2, events.get(0).epoch);
        reader.handleSourceEvents(PartitionEvent.switchTo(1)); assertEquals(1, events.size());
        assertEquals(2, PartitionEvent.markerEpoch(reader.snapshotState(1).get(0)));
        reader.close();
    }

    @Test public void closeFailureNeverAcknowledgesNewGeneration() {
        List<PartitionEvent> events = new ArrayList<>();
        Delegate delegate = new Delegate(1) { public void close() { throw new IllegalStateException("close failed"); } };
        LatestPartitionReader reader = new LatestPartitionReader(context(events), () -> delegate);
        reader.handleSourceEvents(PartitionEvent.switchTo(1));
        try { reader.handleSourceEvents(PartitionEvent.switchTo(2)); fail(); }
        catch (IllegalStateException expected) { assertEquals(1, events.size()); }
    }
    @Test public void rejectsReaderStateAheadOfCoordinatorInsteadOfHanging() {
        LatestPartitionReader reader = new LatestPartitionReader(context(new ArrayList<>()), () -> new Delegate(1));
        reader.addSplits(Collections.singletonList(PartitionEvent.marker(2)));
        try { reader.handleSourceEvents(PartitionEvent.switchTo(1)); fail(); }
        catch (IllegalStateException expected) { }
    }

}
