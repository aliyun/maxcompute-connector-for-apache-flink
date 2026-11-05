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

import org.apache.flink.odps.sink.shuffle.AssignmentSource;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.apache.flink.odps.sink.shuffle.PartitionBucketPartitioner;
import org.apache.flink.odps.sink.shuffle.StaticHashAssignmentSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PartitionBucketPartitioner}. The partitioner is a thin delegator;
 * the routing-logic tests live in {@link StaticHashAssignmentSourceTest} and
 * {@link RegistryAssignmentSourceTest}.
 */
public class PartitionBucketPartitionerTest {

    @Test
    public void partition_delegatesToInjectedSource() {
        AssignmentSource fixed = new AssignmentSource() {
            private static final long serialVersionUID = 1L;
            @Override public int route(PartitionBucketKey key, int numPartitions) {
                return 5;
            }
        };
        PartitionBucketPartitioner partitioner = new PartitionBucketPartitioner(fixed);
        assertEquals(5, partitioner.partition(new PartitionBucketKey("p", 0), 32));
    }

    @Test
    public void partition_withStaticSource_matchesSourceDirectly() {
        StaticHashAssignmentSource source = new StaticHashAssignmentSource();
        PartitionBucketPartitioner partitioner = new PartitionBucketPartitioner(source);
        PartitionBucketKey key = new PartitionBucketKey("ds=20240101", 7);
        assertEquals(source.route(key, 16), partitioner.partition(key, 16));
    }
}
