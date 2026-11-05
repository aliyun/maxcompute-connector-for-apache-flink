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

package org.apache.flink.odps.source.enumerator;

import org.apache.flink.odps.source.partition.PartitionProgress;
import org.apache.flink.odps.source.split.OdpsSourceSplit;

import javax.annotation.Nullable;
import java.util.Collection;

public class ContinuousOdpsSplitEnumState {

    private final Collection<OdpsSourceSplit> splits;

    private PartitionProgress partitionProgress;

    public PartitionProgress partitionProgress() {
        return partitionProgress;
    }

    public ContinuousOdpsSplitEnumState withPartitionProgress(
            PartitionProgress progress) {
        this.partitionProgress = progress.copy();
        return this;
    }

    private final @Nullable Long currentSnapshotId;

    private final @Nullable String currentTxnId;

    public ContinuousOdpsSplitEnumState(Collection<OdpsSourceSplit> splits,
                                        @Nullable Long currentSnapshotId,
                                        @Nullable String currentTxnId) {
        this.splits = splits;
        this.currentSnapshotId = currentSnapshotId;
        this.currentTxnId = currentTxnId;
    }

    public Collection<OdpsSourceSplit> splits() {
        return splits;
    }

    public @Nullable Long currentSnapshotId() {
        return currentSnapshotId;
    }

    public @Nullable String currentTxnId() {
        return currentTxnId;
    }
}