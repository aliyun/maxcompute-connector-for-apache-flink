/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for the regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

package org.apache.flink.odps.test.shuffle;

import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignment;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for {@link PartitionBucketAssignment}.
 */
public class PartitionBucketAssignmentTest {

    @Test
    public void testBasicAssignment() {
        Map<PartitionBucketKey, Integer> map = new HashMap<>();
        map.put(new PartitionBucketKey("ds=2024", 0), 0);
        map.put(new PartitionBucketKey("ds=2024", 1), 1);
        map.put(new PartitionBucketKey("ds=2025", 0), 2);

        PartitionBucketAssignment assignment = new PartitionBucketAssignment(map, 1L);

        assertEquals(0, assignment.getSubtask(new PartitionBucketKey("ds=2024", 0)));
        assertEquals(1, assignment.getSubtask(new PartitionBucketKey("ds=2024", 1)));
        assertEquals(2, assignment.getSubtask(new PartitionBucketKey("ds=2025", 0)));
    }

    @Test
    public void testUnknownKeyReturnsNegative() {
        PartitionBucketAssignment assignment = new PartitionBucketAssignment();
        // Empty assignment → unknown key should return -1
        int result = assignment.getSubtask(new PartitionBucketKey("ds=2024", 0));
        assertEquals(-1, result);
    }

    @Test
    public void testGetAssignment() {
        Map<PartitionBucketKey, Integer> map = new HashMap<>();
        map.put(new PartitionBucketKey("ds=2024", 0), 0);
        map.put(new PartitionBucketKey("ds=2024", 1), 0);
        map.put(new PartitionBucketKey("ds=2025", 0), 1);

        PartitionBucketAssignment assignment = new PartitionBucketAssignment(map, 1L);

        Map<PartitionBucketKey, Integer> returned = assignment.getAssignment();
        assertEquals(map, returned);
    }

    @Test
    public void testIsEmpty() {
        PartitionBucketAssignment empty = new PartitionBucketAssignment();
        assertTrue(empty.isEmpty());

        Map<PartitionBucketKey, Integer> map = new HashMap<>();
        map.put(new PartitionBucketKey("ds=2024", 0), 0);
        PartitionBucketAssignment nonEmpty = new PartitionBucketAssignment(map, 1L);
        assertFalse(nonEmpty.isEmpty());
    }

    @Test
    public void testGetVersion() {
        PartitionBucketAssignment a1 = new PartitionBucketAssignment(new HashMap<>(), 1L);
        assertEquals(1L, a1.getVersion());

        PartitionBucketAssignment a2 = new PartitionBucketAssignment(new HashMap<>(), 5L);
        assertEquals(5L, a2.getVersion());
    }

    @Test
    public void testDefaultAssignment() {
        PartitionBucketAssignment empty = new PartitionBucketAssignment();
        assertEquals(0L, empty.getVersion());
        assertTrue(empty.isEmpty());
        assertTrue(empty.getAssignment().isEmpty());
    }
}
