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

import org.apache.flink.odps.source.enumerator.*;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.junit.Test;
import java.io.*;
import java.util.*;
import static org.junit.Assert.*;

public class LatestPartitionStateTest {
    @Test public void readsV0AndV1Checkpoints() throws Exception {
        ContinuousOdpsSplitEnumStateSerializer serializer = new ContinuousOdpsSplitEnumStateSerializer();
        for (int version = 0; version <= 1; version++) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(0); out.writeLong(5);
            if (version == 1) { out.writeBoolean(true); out.writeUTF("txn"); }
            ContinuousOdpsSplitEnumState s = serializer.deserialize(version, bytes.toByteArray());
            assertEquals(Long.valueOf(5), s.currentSnapshotId()); assertNull(s.partitionProgress());
            assertEquals(version == 1 ? "txn" : null, s.currentTxnId());
        }
    }
    @Test public void roundTripsPendingPartitionsAndSerializedSessions() throws Exception {
        PartitionProgress p = new PartitionProgress(); p.discover(new TreeSet<>(Arrays.asList("pt=0", "pt=1")), 2);
        p.advancePartition(); p.cursor = 3; p.discover(new TreeSet<>(Arrays.asList("pt=1", "pt=2", "pt=3")), 3);
        OdpsSourceSplit split = LatestPartitionReaderTest.split(1);
        ContinuousOdpsSplitEnumState state = new ContinuousOdpsSplitEnumState(Collections.singletonList(split),3L,null).withPartitionProgress(p);
        ContinuousOdpsSplitEnumStateSerializer serializer = new ContinuousOdpsSplitEnumStateSerializer();
        ContinuousOdpsSplitEnumState restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(state));
        assertEquals(p.pending, restored.partitionProgress().pending); assertEquals("pt=1",restored.partitionProgress().initialPartition);
        assertEquals(2, restored.partitionProgress().initialStart); assertEquals(3,restored.partitionProgress().cursor);
        assertEquals(split.splitId(), restored.splits().iterator().next().splitId()); assertEquals(7,restored.splits().iterator().next().getRecordsToSkip());
        restored.partitionProgress().advancePartition(); assertEquals(2,restored.partitionProgress().cursor);
        assertEquals(2, state.partitionProgress().pending.size());
    }
    @Test public void refusesChangingSourceModeOnRestore() {
        LatestPartitionEnumeratorTest.Harness h = new LatestPartitionEnumeratorTest.Harness();
        try { h.create(new ContinuousOdpsSplitEnumState(Collections.emptyList(), 3L,null)); fail(); }
        catch (IllegalArgumentException expected) { }
        try { h.create(h.active().snapshotState(1), false); fail(); }
        catch (IllegalArgumentException expected) { }
    }
}
