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
import java.util.Objects;

/**
 * Represents a composite key of (partitionSpec, bucketId) for MaxCompute primary-key partitioned tables.
 * <p>
 * In MaxCompute's primary-key partitioned tables, data must be routed by (partitionSpec + bucketId)
 * to guarantee that records belonging to the same (partitionSpec, bucketId) group always go to the
 * same Writer/subtask. This is critical for write correctness.
 * <p>
 * This class is used as the partition key in {@link PartitionBucketPartitioner}.
 */
public class PartitionBucketKey implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Partition spec string, e.g., "ds=20240101/region=cn" */
    private final String partitionSpec;

    /** Bucket index within the partition, computed by primary key hash % bucketNum */
    private final int bucketId;

    public PartitionBucketKey(String partitionSpec, int bucketId) {
        this.partitionSpec = partitionSpec != null ? partitionSpec : "";
        this.bucketId = bucketId;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public int getBucketId() {
        return bucketId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionBucketKey that = (PartitionBucketKey) o;
        return bucketId == that.bucketId && Objects.equals(partitionSpec, that.partitionSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionSpec, bucketId);
    }

    @Override
    public String toString() {
        return "PartitionBucketKey{" +
                "partitionSpec='" + partitionSpec + '\'' +
                ", bucketId=" + bucketId +
                '}';
    }
}
