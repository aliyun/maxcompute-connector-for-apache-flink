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

import java.io.Serializable;
import java.util.SortedSet;
import java.util.TreeSet;

/** Coordinator-owned progress. A new partition must never inherit the previous partition's cursor. */
public final class PartitionProgress implements Serializable {
    private static final long serialVersionUID = 1L;
    public boolean initialized;
    public long initialStart;
    public long cursor;
    public long epoch;
    public String active;
    public String initialPartition;
    public final SortedSet<String> pending = new TreeSet<>();

    public PartitionProgress copy() {
        PartitionProgress copy = new PartitionProgress();
        copy.initialized = initialized;
        copy.initialStart = initialStart;
        copy.cursor = cursor;
        copy.epoch = epoch;
        copy.active = active;
        copy.initialPartition = initialPartition;
        copy.pending.addAll(pending);
        return copy;
    }

    public void discover(SortedSet<String> partitions, long start) {
        if (!initialized) {
            initialized = true;
            initialStart = start;
            cursor = start;
            // Start at the latest published partition, then catch up subsequent ones in order.
            if (!partitions.isEmpty()) {
                initialPartition = partitions.last();
                pending.add(initialPartition);
            }
        } else {
            for (String partition : partitions) {
                if ((initialPartition == null || partition.compareTo(initialPartition) >= 0)
                        && (active == null || partition.compareTo(active) > 0)) {
                    pending.add(partition);
                }
            }
        }
    }

    public void advancePartition() {
        active = pending.first();
        pending.remove(active);
        // Retain the startup floor even if metadata discovery is delayed. Old partition progress
        // is not a safe starting point for a partition we have never read.
        cursor = initialStart;
        epoch++;
    }
}
