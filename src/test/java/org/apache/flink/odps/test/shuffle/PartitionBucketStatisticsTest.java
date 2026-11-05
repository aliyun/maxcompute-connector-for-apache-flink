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

import org.apache.flink.odps.sink.shuffle.PartitionBucketStatistics;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for {@link PartitionBucketStatistics}.
 */
public class PartitionBucketStatisticsTest {

    @Test
    public void testIncrementAndGet() {
        PartitionBucketStatistics stats = new PartitionBucketStatistics(0);
        PartitionBucketKey key = new PartitionBucketKey("ds=2024", 3);

        // No data yet → key not present
        assertFalse(stats.getBucketCounts().containsKey(key));

        stats.increment(key, 100);
        assertEquals(Long.valueOf(100), stats.getBucketCounts().get(key));

        stats.increment(key, 50);
        assertEquals(Long.valueOf(150), stats.getBucketCounts().get(key));
    }

    @Test
    public void testGetSubtaskIndex() {
        PartitionBucketStatistics stats0 = new PartitionBucketStatistics(0);
        PartitionBucketStatistics stats3 = new PartitionBucketStatistics(3);
        assertEquals(0, stats0.getSubtaskIndex());
        assertEquals(3, stats3.getSubtaskIndex());
    }

    @Test
    public void testGetBucketCountsUnmodifiable() {
        PartitionBucketStatistics stats = new PartitionBucketStatistics(0);
        stats.increment(new PartitionBucketKey("ds=2024", 0), 100);
        Map<PartitionBucketKey, Long> counts = stats.getBucketCounts();
        try {
            counts.put(new PartitionBucketKey("ds=9999", 0), 999L);
            fail("Should throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testEmpty() {
        PartitionBucketStatistics stats = new PartitionBucketStatistics(0);
        assertTrue(stats.getBucketCounts().isEmpty());
    }

    @Test
    public void testMultipleKeys() {
        PartitionBucketStatistics stats = new PartitionBucketStatistics(1);
        stats.increment(new PartitionBucketKey("ds=2024", 0), 100);
        stats.increment(new PartitionBucketKey("ds=2024", 1), 200);
        stats.increment(new PartitionBucketKey("ds=2025", 0), 300);

        Map<PartitionBucketKey, Long> counts = stats.getBucketCounts();
        assertEquals(3, counts.size());
        assertEquals(Long.valueOf(100), counts.get(new PartitionBucketKey("ds=2024", 0)));
        assertEquals(Long.valueOf(200), counts.get(new PartitionBucketKey("ds=2024", 1)));
        assertEquals(Long.valueOf(300), counts.get(new PartitionBucketKey("ds=2025", 0)));
    }
}
