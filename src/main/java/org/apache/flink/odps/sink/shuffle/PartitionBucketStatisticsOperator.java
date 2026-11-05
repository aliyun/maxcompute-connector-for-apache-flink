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

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Statistics operator that collects (partitionSpec, bucketId) traffic data per subtask.
 *
 * <p>Topology:
 * <pre>
 *   Source → [StatisticsOperator] → partitionCustom → Sink
 *                ↕ OperatorEvent
 *           [Coordinator]
 * </pre>
 *
 * <p>It counts records per (partitionSpec, bucketId) group, sends the aggregated snapshot
 * to the coordinator at checkpoint boundaries, and applies the coordinator's broadcast
 * assignments at the matching barrier.
 *
 * <p><b>Barrier-aligned assignment switch:</b> assignments arrive via OperatorEvent at
 * arbitrary wall-clock times (post-{@code notifyCheckpointComplete} on each subtask). To
 * avoid each subtask switching at a different moment — which would split a (partitionSpec,
 * bucketId) group across two sink subtasks during the transition window — incoming
 * assignments tagged with {@code validFromCheckpointId > 0} are queued, and applied to the
 * registry only at {@link #snapshotState} for matching {@code N >= validFromCheckpointId}.
 * Bootstrap assignments (from {@code executionAttemptReady}, tagged
 * {@link PartitionBucketAssignment#APPLY_IMMEDIATELY}) bypass the queue and apply on
 * receive — a freshly-restarted attempt has nothing to align with.
 *
 * <p>This guarantees per-subtask consistency. Cross-subtask alignment is approximate:
 * different subtasks process barrier {@code N} at different wall-clock times. The window of
 * inconsistency is bounded by the snapshot-processing delta across subtasks, which is far
 * tighter than the broadcast-delivery delta.
 */
public class PartitionBucketStatisticsOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketStatisticsOperator.class);
    private static final long serialVersionUID = 2L;

    private final PartitionBucketKeyExtractor keyExtractor;
    private final OperatorEventGateway eventGateway;
    private final String operatorUid;

    /** Local statistics: (partitionSpec, bucketId) -> record count in current interval */
    private final Map<PartitionBucketKey, Long> localStats = new HashMap<>();

    /**
     * The most recent assignment received from the coordinator that has not yet been
     * applied (because its {@code validFromCheckpointId} is in the future). Replaced on each
     * incoming event with a higher-version assignment.
     *
     * <p>Only ever read/written from the operator's mailbox thread.
     */
    private PartitionBucketAssignment pendingAssignment = null;

    /** The id of the last snapshot this operator has processed. */
    private long lastSnapshotCheckpointId = 0L;

    public PartitionBucketStatisticsOperator(
            PartitionBucketKeyExtractor keyExtractor,
            OperatorEventGateway eventGateway,
            String operatorUid) {
        this.keyExtractor = keyExtractor;
        this.eventGateway = eventGateway;
        this.operatorUid = operatorUid;
    }

    @Override
    public void open() throws Exception {
        super.open();
        PartitionBucketAssignmentRegistry.getInstance().register(operatorUid);
        LOG.info("PartitionBucketStatisticsOperator opened on subtask {}, operatorUid={}",
                getRuntimeContext().getIndexOfThisSubtask(), operatorUid);
    }

    @Override
    public void close() throws Exception {
        try {
            PartitionBucketAssignmentRegistry.getInstance().release(operatorUid);
        } finally {
            super.close();
        }
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData record = element.getValue();

        PartitionBucketKey key = keyExtractor.getKey(record);
        if (key != null) {
            localStats.merge(key, 1L, Long::sum);
        }

        // Forward downstream. The partitioner (running in this operator's emit path) reads
        // the current assignment from the registry — which is updated only at snapshotState.
        output.collect(element);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        long checkpointId = context.getCheckpointId();
        lastSnapshotCheckpointId = checkpointId;

        // Apply any queued assignment whose validFromCheckpointId is now reached. This
        // happens BEFORE forwarding stats so the assignment in effect for snapshot N's
        // post-barrier records reflects the coordinator's intent for that boundary.
        if (pendingAssignment != null
                && pendingAssignment.getValidFromCheckpointId() <= checkpointId) {
            applyAssignment(pendingAssignment, "barrier@" + checkpointId);
            pendingAssignment = null;
        }

        if (!localStats.isEmpty()) {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            PartitionBucketStatistics stats = new PartitionBucketStatistics(subtaskIndex);
            for (Map.Entry<PartitionBucketKey, Long> entry : localStats.entrySet()) {
                stats.increment(entry.getKey(), entry.getValue());
            }

            LOG.debug("Sending statistics to coordinator: {} bucket groups, subtask {}",
                    localStats.size(), subtaskIndex);
            eventGateway.sendEventToCoordinator(new PartitionBucketStatisticsEvent(stats));
            localStats.clear();
        }
    }

    /**
     * Handle assignment event from coordinator (called from the operator event handler).
     *
     * <p>Bootstrap assignments (validFromCheckpointId == APPLY_IMMEDIATELY) apply now.
     * Regular assignments are queued and applied at the matching snapshotState. If a newer
     * assignment arrives while one is already queued, the newer one replaces it.
     */
    public void handleOperatorEvent(PartitionBucketAssignmentEvent event) {
        PartitionBucketAssignment assignment = event.getAssignment();
        long vfci = assignment.getValidFromCheckpointId();

        if (vfci == PartitionBucketAssignment.APPLY_IMMEDIATELY
                || vfci <= lastSnapshotCheckpointId) {
            // Either explicit bootstrap, or a late delivery that we should already have
            // applied — apply now. The registry's version check still gates against stale
            // updates, so a re-applied bootstrap is a no-op.
            applyAssignment(assignment, vfci == PartitionBucketAssignment.APPLY_IMMEDIATELY
                    ? "bootstrap" : "late@vfci=" + vfci);
            return;
        }

        // Queue for snapshotState to apply at the matching barrier. If the queue already has
        // a pending one, keep the higher-version (= newer) one — older queued assignments
        // become stale once a newer one is announced.
        if (pendingAssignment == null || assignment.getVersion() > pendingAssignment.getVersion()) {
            pendingAssignment = assignment;
            LOG.info("Queued assignment v{} (validFromCheckpointId={}) for barrier-aligned switch",
                    assignment.getVersion(), vfci);
        }
    }

    private void applyAssignment(PartitionBucketAssignment assignment, String reason) {
        LOG.info("Applying assignment v{} (validFromCheckpointId={}, reason={}): {} groups",
                assignment.getVersion(), assignment.getValidFromCheckpointId(), reason,
                assignment.getAssignment().size());
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(operatorUid, assignment);
    }
}
