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

import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests for {@link PartitionBucketKey}.
 */
public class PartitionBucketKeyTest {

    @Test
    public void testEqualsAndHashCode() {
        PartitionBucketKey key1 = new PartitionBucketKey("ds=20240101", 3);
        PartitionBucketKey key2 = new PartitionBucketKey("ds=20240101", 3);
        PartitionBucketKey key3 = new PartitionBucketKey("ds=20240101", 4);
        PartitionBucketKey key4 = new PartitionBucketKey("ds=20240102", 3);
        PartitionBucketKey key5 = new PartitionBucketKey(null, 3);
        PartitionBucketKey key6 = new PartitionBucketKey(null, 3);

        // Same partition + bucket => equal
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());

        // Different bucket => not equal
        assertNotEquals(key1, key3);

        // Different partition => not equal
        assertNotEquals(key1, key4);

        // null partition + same bucket => equal
        assertEquals(key5, key6);
        assertEquals(key5.hashCode(), key6.hashCode());

        // null partition vs non-null => not equal
        assertNotEquals(key1, key5);
    }

    @Test
    public void testAsHashMapKey() {
        Map<PartitionBucketKey, Long> map = new HashMap<>();
        PartitionBucketKey key1 = new PartitionBucketKey("ds=20240101", 0);
        PartitionBucketKey key1Copy = new PartitionBucketKey("ds=20240101", 0);

        map.put(key1, 100L);
        // Can retrieve using an equal but different instance
        assertEquals(Long.valueOf(100L), map.get(key1Copy));

        // Increment
        map.merge(key1Copy, 50L, Long::sum);
        assertEquals(Long.valueOf(150L), map.get(key1));
    }

    @Test
    public void testAsHashSetKey() {
        Set<PartitionBucketKey> set = new HashSet<>();
        for (int b = 0; b < 16; b++) {
            set.add(new PartitionBucketKey("ds=20240101", b));
        }
        assertEquals(16, set.size());

        // Adding same key again doesn't increase size
        set.add(new PartitionBucketKey("ds=20240101", 0));
        assertEquals(16, set.size());

        // Adding different partition increases size
        set.add(new PartitionBucketKey("ds=20240102", 0));
        assertEquals(17, set.size());
    }

    @Test
    public void testToString() {
        PartitionBucketKey key = new PartitionBucketKey("ds=20240101", 7);
        String str = key.toString();
        assertTrue(str.contains("ds=20240101"));
        assertTrue(str.contains("7"));
    }

    @Test
    public void testNullPartitionSpec() {
        PartitionBucketKey key = new PartitionBucketKey(null, 5);
        assertNotNull(key);
        assertNotNull(key.toString());
        // null partition is normalized to "" in constructor
        assertEquals(key, new PartitionBucketKey("", 5));
        // But still different from a non-empty partition
        assertNotEquals(key, new PartitionBucketKey("ds=2024", 5));
    }
}
