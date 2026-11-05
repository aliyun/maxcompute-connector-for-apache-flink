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

import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.apache.flink.odps.sink.shuffle.StaticHashAssignmentSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StaticHashAssignmentSource}.
 */
public class StaticHashAssignmentSourceTest {

    @Test
    public void route_isDeterministic_acrossCallsAndInstances() {
        PartitionBucketKey key = new PartitionBucketKey("ds=20240101/region=cn", 7);
        int n = 32;
        int r1 = new StaticHashAssignmentSource().route(key, n);
        int r2 = new StaticHashAssignmentSource().route(key, n);
        StaticHashAssignmentSource shared = new StaticHashAssignmentSource();
        for (int i = 0; i < 100; i++) {
            assertEquals(r1, shared.route(key, n));
        }
        assertEquals(r1, r2);
    }

    @Test
    public void route_alwaysInRange_forRandomKeys() {
        StaticHashAssignmentSource source = new StaticHashAssignmentSource();
        Random rnd = new Random(42);
        for (int n : new int[] {4, 16, 64}) {
            for (int i = 0; i < 1000; i++) {
                PartitionBucketKey k = new PartitionBucketKey("p" + rnd.nextInt(100), rnd.nextInt(64));
                int s = source.route(k, n);
                assertTrue("subtask " + s + " out of [0," + n + ")", s >= 0 && s < n);
            }
        }
    }

    @Test
    public void route_spread_isReasonablyUniform() {
        StaticHashAssignmentSource source = new StaticHashAssignmentSource();
        for (int n : new int[] {4, 16, 64}) {
            int[] counts = new int[n];
            for (int p = 0; p < 50; p++) {
                for (int b = 0; b < 64; b++) {
                    counts[source.route(new PartitionBucketKey("ds=" + p, b), n)]++;
                }
            }
            int total = 50 * 64;
            int floor = (total / n) / 2;     // every subtask gets at least 50% of mean
            int min = Arrays.stream(counts).min().getAsInt();
            assertTrue("min=" + min + " floor=" + floor + " counts=" + Arrays.toString(counts),
                    min >= floor);
        }
    }

    @Test
    public void route_isPartitionAware_notBucketOnly() {
        // Same bucketId across many partitionSpecs must hit more than one subtask —
        // otherwise the source is bucket-only (the bug we're killing).
        StaticHashAssignmentSource source = new StaticHashAssignmentSource();
        Set<Integer> subtasks = new HashSet<>();
        for (int p = 0; p < 50; p++) {
            subtasks.add(source.route(new PartitionBucketKey("ds=" + p, 0), 16));
        }
        assertTrue("partition-aware should produce >1 subtask, got: " + subtasks,
                subtasks.size() > 1);
    }

    @Test
    public void route_negativeHashCode_inRange() {
        StaticHashAssignmentSource source = new StaticHashAssignmentSource();
        // Find any key whose hashCode is negative and verify routing stays in range
        // (Math.floorMod, not Math.abs(%), handles Integer.MIN_VALUE).
        PartitionBucketKey neg = null;
        for (int i = 0; i < 100_000 && neg == null; i++) {
            PartitionBucketKey k = new PartitionBucketKey("p" + i, i);
            if (k.hashCode() < 0) {
                neg = k;
            }
        }
        assertTrue("expected to find a negative-hash key in the search range", neg != null);
        int s = source.route(neg, 16);
        assertTrue("subtask " + s + " out of range", s >= 0 && s < 16);
    }

    /**
     * Regression guard: pin three known routings. If these break, the routing has
     * shifted across JDK versions or {@link PartitionBucketKey#hashCode()} has
     * changed. Update the constants only after confirming the change is intentional
     * and acceptable as a routing-stability incident (in-flight writers to
     * (partition, bucket) groups will relocate on next restart).
     */
    @Test
    public void route_pinnedValues_crossRestartGuard() {
        StaticHashAssignmentSource source = new StaticHashAssignmentSource();
        assertEquals(EXPECTED_DS_CN, source.route(
                new PartitionBucketKey("ds=20240101/region=cn", 3), 32));
        assertEquals(EXPECTED_ROOT, source.route(
                new PartitionBucketKey("", 0), 8));
        assertEquals(EXPECTED_DS_512, source.route(
                new PartitionBucketKey("ds=20240512", 7), 16));
    }

    // Filled in once from the first run of route_pinnedValues_crossRestartGuard.
    private static final int EXPECTED_DS_CN = 15;
    private static final int EXPECTED_ROOT  = 1;
    private static final int EXPECTED_DS_512 = 4;
}
