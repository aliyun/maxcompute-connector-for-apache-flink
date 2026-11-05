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

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.read.*;
import com.aliyun.odps.table.read.split.*;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.odps.source.OdpsSplitStrategy;
import org.apache.flink.odps.source.OdpsStartupMode;
import org.apache.flink.odps.source.enumerator.*;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.util.OdpsConf;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import static org.junit.Assert.*;

/** Only service responses are fake; the production enumerator, split creation and assigner run. */
public class LatestPartitionEnumeratorTest {
    static final class Harness {
        final Map<Integer, ReaderInfo> readers = new HashMap<>();
        final Map<Integer, List<OdpsSourceSplit>> assigned = new HashMap<>();
        final List<PartitionEvent> events = new ArrayList<>();
        final Queue<Runnable> jobs = new ArrayDeque<>();
        final List<String> windows = new ArrayList<>();
        SortedSet<String> partitions = new TreeSet<>(Collections.singleton("pt=1"));
        long version = 3;
        int splitCount = 2;
        boolean badCutoff;
        int parallelism;
        Harness() { this(2); }
        Harness(int parallelism) {
            this.parallelism = parallelism;
            for (int i = 0; i < parallelism; i++) { readers.put(i, new ReaderInfo(i, "localhost")); }
        }
        @SuppressWarnings("unchecked")
        final SplitEnumeratorContext<OdpsSourceSplit> context = (SplitEnumeratorContext<OdpsSourceSplit>)
                Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{SplitEnumeratorContext.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "registeredReaders": return readers;
                                case "currentParallelism": return parallelism;
                                case "sendEventToSourceReader": events.add((PartitionEvent) args[1]); return null;
                                case "assignSplits":
                                    ((SplitsAssignment<OdpsSourceSplit>) args[0]).assignment().forEach((reader, splits) ->
                                            assigned.computeIfAbsent(reader, k -> new ArrayList<>()).addAll(splits));
                                    return null;
                                case "callAsync":
                                    if (args.length == 2) {
                                        jobs.add(() -> {
                                            Object result = null; Throwable failure = null;
                                            try { result = ((Callable<?>) args[0]).call(); }
                                            catch (Throwable t) { failure = t; }
                                            ((BiConsumer<Object, Throwable>) args[1]).accept(result, failure);
                                        });
                                    }
                                    return null;
                                default: return null;
                            }
                        });
        Enumerator create(ContinuousOdpsSplitEnumState state) { return create(state, true); }
        Enumerator create(ContinuousOdpsSplitEnumState state, boolean latest) { return new Enumerator(state, latest); }
        final class Enumerator extends ContinuousOdpsEnumerator {
            Enumerator(ContinuousOdpsSplitEnumState state, boolean latest) {
                super(Harness.this.context, conf(latest), TableIdentifier.of("test", "default", "table"),
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                        OdpsSplitStrategy.BUCKET, 0, 2, 2000, OdpsStartupMode.FROM_SNAPSHOT,
                        IncrementalOptions.newBuilder().startVersion(2L)
                                .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC).build(), state);
            }
            @Override protected TableSchema readTableSchema() { return new TableSchema(); }
            @Override protected long latestReadableVersion() { return version; }
            @Override protected SortedSet<String> latestPartitions() { return new TreeSet<>(partitions); }
            @Override protected TableReadSessionBuilder sessionBuilder(List<String> selected) {
                return new TableReadSessionBuilder() {
                    @Override public TableIncrementalReadSession buildIncrementalReadSession() {
                        long start = getIncrementalOptions().getStartVersion().get().getVersion().get();
                        long end = getIncrementalOptions().getEndVersion().get().getVersion().get();
                        windows.add(selected.get(0) + ":" + start + "-" + end);
                        return session("session" + windows.size(), badCutoff ? end - 1 : end, splitCount);
                    }
                };
            }
            void tick() { requestLatestScan(); }
            void legacyResult() { processDiscoveredSplits(Optional.of(new SessionWithNextSnapshotId(session("legacy", 3, 2), 3L)), null); }
        }
        void run() { jobs.remove().run(); }
        void ack(Enumerator e, int reader, long epoch) {
            e.handleSourceEvent(reader, new PartitionEvent(PartitionEvent.Kind.READY, epoch));
        }
        void ackAll(Enumerator e, long epoch) { for (int i : readers.keySet()) { ack(e, i, epoch); } }
        void requestAll(Enumerator e) { for (int i : readers.keySet()) { e.handleSplitRequest(i, "localhost"); } }
        Enumerator active() {
            Enumerator e = create(null);
            for (int i : readers.keySet()) { e.addReader(i); }
            ackAll(e, 0); run(); // discover and switch
            ackAll(e, 1); run(); // plan the first bounded window
            return e;
        }
        void discover(Enumerator e, long cutoff, String... parts) {
            version = cutoff; partitions = new TreeSet<>(Arrays.asList(parts)); e.tick(); run();
        }
    }

    static OdpsConf conf(boolean latest) {
        OdpsConf conf = new OdpsConf("unused", "unused", "unused", "test");
        conf.setProperty("scan.partition.latest", String.valueOf(latest));
        return conf;
    }
    static TableIncrementalReadSession session(String id, long end, int count) {
        InputSplit[] splits = new InputSplit[count];
        for (int i = 0; i < count; i++) {
            final int bucket = i % 2;
            splits[i] = new InputSplitWithBucket() {
                public int getBucketId() { return bucket; }
                public String getSessionId() { return id; }
            };
        }
        InvocationHandler handler = (InvocationHandler & Serializable) (proxy, method, args) -> {
            switch (method.getName()) {
                case "getId": return id;
                case "getSnapshot": return new TableSnapshotSpec() {
                    public Optional<Long> getVersion() { return Optional.of(end); }
                };
                case "getInputSplitAssigner": return new InputSplitAssigner() {
                    public InputSplit[] getAllSplits() { return splits; }
                };
                case "getLatestTxnId": return Optional.empty();
                default: return null;
            }
        };
        return (TableIncrementalReadSession) Proxy.newProxyInstance(LatestPartitionEnumeratorTest.class.getClassLoader(),
                new Class[]{TableIncrementalReadSession.class}, handler);
    }

    @Test public void coalescesSlowPollsAndDiscardsResultAfterClose() throws Exception {
        Harness h = new Harness(); Harness.Enumerator e = h.create(null);
        e.tick(); e.tick(); e.tick(); assertEquals(1, h.jobs.size());
        e.close(); h.run(); assertFalse(e.snapshotState(1).partitionProgress().initialized);
    }
    @Test public void waitsForAllReadersBeforeSwitchAndAssignment() {
        Harness h = new Harness(); h.partitions.add("pt=0"); Harness.Enumerator e = h.create(null);
        e.addReader(0); e.addReader(1); h.ack(e, 0, 0); e.tick(); h.run();
        assertNull(e.snapshotState(1).partitionProgress().active);
        h.ack(e, 1, 0); h.run();
        assertEquals("pt=1", e.snapshotState(2).partitionProgress().active);
        h.ack(e, 0, 1); h.requestAll(e); assertTrue(h.assigned.isEmpty());
        h.ack(e, 1, 1); h.run();
        assertEquals(1, h.assigned.get(0).size()); assertEquals(1, h.assigned.get(1).size());
    }
    @Test public void reusesPullAssignmentAndPreservesBucketOrderAcrossWindows() {
        Harness h = new Harness(); h.splitCount = 4; Harness.Enumerator e = h.active();
        assertTrue(h.assigned.isEmpty()); h.requestAll(e);
        assertEquals(2, e.snapshotState(1).splits().size());
        h.discover(e, 4, "pt=1"); // plans ahead using the existing bounded pending queue
        assertEquals(6, e.snapshotState(2).splits().size());
        h.requestAll(e);
        assertTrue(h.assigned.get(0).get(1).splitId().contains("session1/2"));
        h.requestAll(e);
        assertTrue(h.assigned.get(0).get(2).splitId().contains("session2/0"));
    }
    @Test public void switchesWithoutDrainingAndFiltersReturnedOldSplits() {
        Harness h = new Harness(); Harness.Enumerator e = h.active(); h.requestAll(e);
        OdpsSourceSplit old = h.assigned.get(0).get(0);
        h.discover(e, 4, "pt=1", "pt=2");
        assertTrue(e.snapshotState(2).splits().isEmpty());
        e.addSplitsBack(Collections.singletonList(old), 0); h.ack(e, 0, 1);
        assertTrue(h.jobs.isEmpty());
        h.ackAll(e, 2); h.run();
        assertEquals("pt=2:2-4", h.windows.get(1));
    }
    @Test public void idleInitialPartitionSwitchesWithoutPlanningOldData() {
        Harness h = new Harness(); h.version = 2; Harness.Enumerator e = h.active();
        assertTrue(h.windows.isEmpty());
        h.discover(e, 3, "pt=1", "pt=2"); h.ackAll(e, 2); h.run();
        assertEquals(Collections.singletonList("pt=2:2-3"), h.windows);
    }

    @Test public void plansEveryDiscoveredPartitionInOrderBeforeSwitchingAgain() {
        Harness h = new Harness(); Harness.Enumerator e = h.active();
        h.discover(e, 5, "pt=1", "pt=3", "pt=2"); h.ackAll(e, 2); h.run();
        assertEquals("pt=2:2-5", h.windows.get(1));
        e.tick(); h.run(); h.ackAll(e, 3); h.run();
        assertEquals("pt=3:2-5", h.windows.get(2));
    }
    @Test public void checkpointDuringScanRetainsOldCursorAndReplaysTheWindow() {
        Harness h = new Harness(); Harness.Enumerator e = h.active(); h.requestAll(e);
        h.version = 4; e.tick(); ContinuousOdpsSplitEnumState state = e.snapshotState(1);
        h.run(); assertEquals(3, state.partitionProgress().cursor);
        Harness restored = new Harness(); restored.version = 4;
        Harness.Enumerator r = restored.create(state); restored.ackAll(r, 1); restored.run();
        assertEquals(Collections.singletonList("pt=1:3-4"), restored.windows);
    }
    @Test public void checkpointRestoresPendingSplitsThroughTheSameAssigner() throws Exception {
        Harness h = new Harness(); Harness.Enumerator e = h.active();
        ContinuousOdpsSplitEnumStateSerializer serializer = new ContinuousOdpsSplitEnumStateSerializer();
        ContinuousOdpsSplitEnumState state = serializer.deserialize(serializer.getVersion(), serializer.serialize(e.snapshotState(1)));
        Harness restored = new Harness(); Harness.Enumerator r = restored.create(state);
        restored.requestAll(r); assertTrue(restored.assigned.isEmpty());
        restored.ackAll(r, 1); restored.run();
        assertEquals(1, restored.assigned.get(0).size()); assertTrue(restored.windows.isEmpty());
    }
    @Test public void emptyWindowAdvancesButNewPartitionStillUsesStartupFloor() {
        Harness h = new Harness(); h.splitCount = 0; Harness.Enumerator e = h.active();
        assertEquals(3, e.snapshotState(1).partitionProgress().cursor);
        h.discover(e, 3, "pt=1", "pt=2"); h.splitCount = 2; h.ackAll(e, 2); h.run();
        assertEquals("pt=2:2-3", h.windows.get(1));
    }
    @Test public void cutoffMismatchDoesNotCommitProgress() {
        Harness h = new Harness(); Harness.Enumerator e = h.active(); h.badCutoff = true;
        try { h.discover(e, 4, "pt=1"); fail(); } catch (IllegalStateException expected) { }
        assertEquals(3, e.snapshotState(1).partitionProgress().cursor);
    }
    @Test public void disappearanceAndVersionRegressionFailExplicitly() {
        Harness h = new Harness(); Harness.Enumerator e = h.active();
        try { h.discover(e, 2, "pt=1"); fail(); } catch (IllegalStateException expected) { }
        try { h.discover(e, 3); fail(); } catch (IllegalStateException expected) { }
    }
    @Test public void disabledModeKeepsOriginalSplitIdsAndLegacyCheckpoint() throws Exception {
        Harness h = new Harness(); Harness.Enumerator e = h.create(null, false);
        e.addReader(0); assertTrue(h.events.isEmpty()); e.legacyResult();
        ContinuousOdpsSplitEnumState state = e.snapshotState(1);
        assertNull(state.partitionProgress()); assertEquals(Long.valueOf(3), state.currentSnapshotId());
        assertEquals("legacy 0", state.splits().iterator().next().splitId());
        Harness.Enumerator restored = h.create(state, false);
        restored.handleSplitRequest(0, "localhost"); assertEquals("legacy 0", h.assigned.get(0).get(0).splitId());
    }
}
