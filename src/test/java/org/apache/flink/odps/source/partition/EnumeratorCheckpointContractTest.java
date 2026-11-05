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

import org.apache.flink.odps.source.enumerator.*;
import org.apache.flink.odps.source.split.*;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class EnumeratorCheckpointContractTest {
    @Test
    public void restoresAssignmentsAndOffsetsAndRejectsCorruption() throws Exception {
        BatchOdpsSplitEnumStateSerializer codec = new BatchOdpsSplitEnumStateSerializer();
        List<LinkedList<OdpsSourceSplit>> readers =
                Arrays.asList(
                        new LinkedList<>(),
                        new LinkedList<>(
                                Arrays.asList(
                                        LatestPartitionReaderTest.split(1),
                                        LatestPartitionReaderTest.split(2))));
        byte[] bytes = codec.serialize(new BatchOdpsSplitEnumState(readers));
        BatchOdpsSplitEnumState restored = codec.deserialize(codec.getVersion(), bytes);
        assertTrue(restored.getAssignedSplits().get(0).isEmpty());
        assertEquals(2, restored.getAssignedSplits().get(1).size());
        assertEquals(7, restored.getAssignedSplits().get(1).getFirst().getRecordsToSkip());
        assertEquals(
                readers.get(1).getLast().splitId(),
                restored.getAssignedSplits().get(1).getLast().splitId());
        assertThrows(IOException.class, () -> codec.deserialize(99, bytes));
        assertThrows(
                IOException.class,
                () ->
                        codec.deserialize(
                                codec.getVersion(), Arrays.copyOf(bytes, bytes.length - 1)));
        assertThrows(
                IOException.class,
                () -> new OdpsSourceSplitSerializer().deserialize(99, new byte[0]));
    }
}
