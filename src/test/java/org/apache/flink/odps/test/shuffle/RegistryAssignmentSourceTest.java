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

package org.apache.flink.odps.test.shuffle;

import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignment;
import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignmentRegistry;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.apache.flink.odps.sink.shuffle.RegistryAssignmentSource;
import org.apache.flink.odps.sink.shuffle.StaticHashAssignmentSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RegistryAssignmentSource}.
 */
public class RegistryAssignmentSourceTest {

    private String operatorId;

    @Before
    public void setUp() {
        operatorId = "test-registry-source-" + UUID.randomUUID();
        PartitionBucketAssignmentRegistry.getInstance().register(operatorId);
    }

    @After
    public void tearDown() {
        PartitionBucketAssignmentRegistry.getInstance().removeAssignment(operatorId);
    }

    @Test
    public void route_hit_returnsRegistryValue() {
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(new PartitionBucketKey("ds=20240101", 3), 7);
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(
                operatorId, new PartitionBucketAssignment(assignment, 1L));

        RegistryAssignmentSource source = new RegistryAssignmentSource(operatorId);
        assertEquals(7, source.route(new PartitionBucketKey("ds=20240101", 3), 32));
    }

    @Test
    public void route_miss_emptyRegistry_fallsBackToStaticHash() {
        // No assignment update → assignment is empty
        PartitionBucketKey key = new PartitionBucketKey("ds=20240101", 3);
        int expected = new StaticHashAssignmentSource().route(key, 32);
        assertEquals(expected, new RegistryAssignmentSource(operatorId).route(key, 32));
    }

    @Test
    public void route_miss_partiallyPopulatedRegistry_fallsBackToStaticHash() {
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(new PartitionBucketKey("ds=20240101", 3), 7);
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(
                operatorId, new PartitionBucketAssignment(assignment, 1L));

        // A key NOT in the assignment falls back to hash even though other keys are mapped.
        PartitionBucketKey absent = new PartitionBucketKey("ds=20240102", 3);
        int expected = new StaticHashAssignmentSource().route(absent, 32);
        assertEquals(expected, new RegistryAssignmentSource(operatorId).route(absent, 32));
    }

    @Test
    public void route_outOfRangeAssignment_fallsBackToStaticHash() {
        // Sink parallelism shrunk on rescale: the old assignment maps to an index >= numPartitions.
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        PartitionBucketKey key = new PartitionBucketKey("ds=20240101", 3);
        assignment.put(key, 50);  // out of range for numPartitions=32
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(
                operatorId, new PartitionBucketAssignment(assignment, 1L));

        int expected = new StaticHashAssignmentSource().route(key, 32);
        assertEquals(expected, new RegistryAssignmentSource(operatorId).route(key, 32));
    }
}
