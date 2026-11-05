/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work by the regarding copyright ownership.
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

package org.apache.flink.odps.sink.shuffle;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Traffic statistics collected per (partitionSpec, bucketId) pair.
 * <p>
 * This is the local statistics snapshot from a single subtask, sent to the coordinator
 * at checkpoint boundaries.
 */
public class PartitionBucketStatistics implements Serializable {

    private static final long serialVersionUID = 1L;

    /** subtask index that produced this statistics */
    private final int subtaskIndex;

    /** (partitionSpec, bucketId) -> record count */
    private final Map<PartitionBucketKey, Long> bucketCounts;

    public PartitionBucketStatistics(int subtaskIndex) {
        this.subtaskIndex = subtaskIndex;
        this.bucketCounts = new HashMap<>();
    }

    public void increment(PartitionBucketKey key, long delta) {
        bucketCounts.merge(key, delta, Long::sum);
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public Map<PartitionBucketKey, Long> getBucketCounts() {
        return Collections.unmodifiableMap(bucketCounts);
    }

    @Override
    public String toString() {
        return "PartitionBucketStatistics{" +
                "subtaskIndex=" + subtaskIndex +
                ", bucketCounts=" + bucketCounts +
                '}';
    }
}
