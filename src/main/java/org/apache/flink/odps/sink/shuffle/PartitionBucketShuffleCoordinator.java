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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator for the dynamic (partitionSpec, bucketId) shuffle.
 *
 * <p>Receives local statistics from each subtask, aggregates them, and at every checkpoint
 * computes a new balanced assignment via {@link AssignmentComputer}. The new assignment is
 * tagged with {@code validFromCheckpointId = N+1} (where {@code N} is the just-checkpointed
 * id) and broadcast to all subtasks in {@link #notifyCheckpointComplete}. The statistics
 * operator queues incoming assignments and applies them only at its own snapshot boundary
 * matching {@code validFromCheckpointId}, so the cross-subtask switch is barrier-aligned.
 *
 * <p><b>Note on aggregation lag:</b> per Flink's {@code OperatorCoordinator} contract,
 * {@code checkpointCoordinator(N)} runs <em>before</em> the operator's
 * {@code snapshotState(N)} (and therefore before the stats event for snapshot {@code N}
 * arrives). The assignment computed at checkpoint {@code N} is therefore based on stats
 * from snapshot {@code N-1} — a fixed one-checkpoint lag.
 *
 * <p><b>State persistence:</b> the current assignment (with version + validFromCheckpointId)
 * is serialized into the coordinator checkpoint state so that on restore the version
 * counter and assignment continue monotonically. Without this, after a subtask reset the
 * registry's old version would reject newly-computed assignments until the version catches
 * up — leading to long stale-routing windows.
 */
public class PartitionBucketShuffleCoordinator implements OperatorCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketShuffleCoordinator.class);

    private final OperatorID operatorID;

    /**
     * Number of stats operator subtasks. Used to size {@link #gateways} and bound the
     * bootstrap/broadcast loops. Must equal the parallelism set on the stats operator
     * vertex — that is, the operator this coordinator is attached to.
     */
    private final int statsParallelism;

    /**
     * Number of sink subtasks. Used as the assignment value range
     * ({@link AssignmentComputer#compute(Map, int)}). The downstream {@code partitionCustom}
     * routes records to {@code 0..sinkParallelism-1}, so any subtask index assigned beyond
     * that would silently fall back to static routing in the partitioner. Decoupled from
     * {@link #statsParallelism} because users can configure a different parallelism for the
     * sink (e.g. via {@code table.exec.sink.parallelism}) than the upstream stream.
     */
    private final int sinkParallelism;

    /** Aggregated global statistics: (partitionSpec, bucketId) -> total record count */
    private final Map<PartitionBucketKey, Long> aggregatedStats = new HashMap<>();

    /** Current assignment (the latest computed one, broadcast to all subtasks). */
    private PartitionBucketAssignment currentAssignment = new PartitionBucketAssignment();

    /**
     * Version of the most recent assignment that was actually broadcast to subtasks. Used to
     * suppress redundant broadcasts when {@link #checkpointCoordinator} computes an assignment
     * map identical to the current one — under stable traffic the map rarely changes, but each
     * broadcast is another chance to land in a checkpoint window that later aborts (which
     * drops the queued event and triggers Flink's "event lost" task failover). Skipping
     * unchanged broadcasts cuts that surface area dramatically.
     */
    private long lastBroadcastVersion = 0L;

    /** Gateway for each stats-operator subtask, used to send assignment events */
    private final SubtaskGateway[] gateways;

    /** Number of subtasks that have reported statistics in the current checkpoint interval */
    private int reportedSubtasks = 0;

    public PartitionBucketShuffleCoordinator(OperatorID operatorID, int statsParallelism, int sinkParallelism) {
        this.operatorID = operatorID;
        this.statsParallelism = statsParallelism;
        this.sinkParallelism = sinkParallelism;
        this.gateways = new SubtaskGateway[statsParallelism];
    }

    @Override
    public void start() throws Exception {
        LOG.info("PartitionBucketShuffleCoordinator started: statsParallelism={}, sinkParallelism={}",
                statsParallelism, sinkParallelism);
    }

    @Override
    public void close() throws Exception {
        // nothing to close
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if (event instanceof PartitionBucketStatisticsEvent) {
            PartitionBucketStatisticsEvent statsEvent = (PartitionBucketStatisticsEvent) event;
            PartitionBucketStatistics stats = statsEvent.getStatistics();

            for (Map.Entry<PartitionBucketKey, Long> entry : stats.getBucketCounts().entrySet()) {
                aggregatedStats.merge(entry.getKey(), entry.getValue(), Long::sum);
            }

            reportedSubtasks++;
            LOG.debug("Received statistics from subtask {} (attempt {}), total reported: {}/{}",
                    subtask, attemptNumber, reportedSubtasks, statsParallelism);
        } else {
            LOG.warn("Unknown event type from subtask {}: {}", subtask, event.getClass().getName());
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        LOG.info("Checkpoint {} triggered: {} reported subtasks, {} bucket groups in window",
                checkpointId, reportedSubtasks, aggregatedStats.size());

        // Compute new assignment from accumulated stats. Tag it with validFromCheckpointId =
        // checkpointId + 1: subtasks will switch to this assignment at their next snapshot
        // boundary, ensuring cross-subtask alignment around a single barrier.
        //
        // Suppress the version bump if the new map equals the current one. Stable traffic
        // produces near-identical assignments every checkpoint, so without this check we'd
        // bump the version (and broadcast in notifyCheckpointComplete) at every interval —
        // each unnecessary broadcast is another roll of the dice on event-delivery loss.
        if (!aggregatedStats.isEmpty()) {
            Map<PartitionBucketKey, Integer> newAssignmentMap =
                    AssignmentComputer.compute(aggregatedStats, sinkParallelism);

            if (newAssignmentMap.equals(currentAssignment.getAssignment())) {
                LOG.info("Checkpoint {}: computed assignment is identical to current v{}; skipping update",
                        checkpointId, currentAssignment.getVersion());
            } else {
                long newVersion = currentAssignment.getVersion() + 1;
                currentAssignment = new PartitionBucketAssignment(
                        newAssignmentMap, newVersion, checkpointId + 1);
                LOG.info("New assignment v{} computed (validFromCheckpointId={}): {} groups",
                        newVersion, checkpointId + 1, newAssignmentMap.size());
                LOG.info(AssignmentBreakdownFormatter.format(
                        newVersion, aggregatedStats, newAssignmentMap, sinkParallelism));
            }
        } else {
            LOG.info("No statistics collected at checkpoint {}, keeping current assignment {}",
                    checkpointId, currentAssignment);
        }

        // Reset window for next interval.
        aggregatedStats.clear();
        reportedSubtasks = 0;

        // Persist the assignment so version monotonicity survives restart. Without this, a
        // restored coordinator restarts from version 0 while subtask-side registries (in
        // surviving JVMs) still hold the high pre-restart version → updateAssignment rejects
        // every new broadcast until coordinator catches up, freezing routing.
        resultFuture.complete(serialize(currentAssignment));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (currentAssignment.getVersion() == lastBroadcastVersion) {
            // No new assignment since the last broadcast. Skipping avoids two failure modes:
            //  (1) sending an event into a checkpoint window that later aborts → Flink drops
            //      the queued event and reports it as lost → kills the target subtask;
            //  (2) sending into a gateway whose attempt happens to die between sendEvent and
            //      RPC arrival → same "event lost" failover.
            // Subtasks restarted in the meantime are bootstrapped via executionAttemptReady,
            // so this skip does not delay any new attempt's catchup.
            LOG.debug("Checkpoint {} complete; assignment v{} unchanged since last broadcast, skipping",
                    checkpointId, currentAssignment.getVersion());
            return;
        }

        LOG.info("Checkpoint {} complete, broadcasting assignment {}", checkpointId, currentAssignment);
        final long broadcastVersion = currentAssignment.getVersion();
        PartitionBucketAssignmentEvent event = new PartitionBucketAssignmentEvent(currentAssignment);
        for (int i = 0; i < statsParallelism; i++) {
            SubtaskGateway gateway = gateways[i];
            if (gateway != null) {
                final int subtaskIndex = i;
                gateway.sendEvent(event).whenComplete((ack, t) -> {
                    if (t != null) {
                        // Flink will trigger task failover on its own (built-in
                        // event-loss handling). Log here for observability so we can
                        // correlate the failover with the broadcast that triggered it.
                        LOG.warn("Failed to deliver assignment v{} to stats subtask {}: {}",
                                broadcastVersion, subtaskIndex, t.toString());
                    }
                });
            } else {
                LOG.warn("No gateway for subtask {}, assignment will be delivered on attempt-ready", i);
            }
        }
        lastBroadcastVersion = broadcastVersion;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.debug("Checkpoint {} aborted", checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) throws Exception {
        LOG.info("Reset to checkpoint {}", checkpointId);
        aggregatedStats.clear();
        reportedSubtasks = 0;
        if (checkpointData != null && checkpointData.length > 0) {
            currentAssignment = deserialize(checkpointData);
            LOG.info("Restored assignment from checkpoint state: {}", currentAssignment);
        } else {
            currentAssignment = new PartitionBucketAssignment();
        }
        // After restore, every restarted attempt receives the current assignment via
        // executionAttemptReady's bootstrap path. Mark the broadcast as already done so the
        // next notifyCheckpointComplete doesn't re-broadcast the same version (which would be
        // redundant and adds another delivery to potentially fail).
        lastBroadcastVersion = currentAssignment.getVersion();
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        LOG.debug("Subtask {} reset to checkpoint {}", subtask, checkpointId);
        if (subtask < gateways.length) {
            // Defensive: drop the stale gateway. executionAttemptReady will re-register.
            gateways[subtask] = null;
        }
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        LOG.debug("Subtask {} execution attempt {} ready", subtask, attemptNumber);
        if (subtask < gateways.length) {
            gateways[subtask] = gateway;
        }

        // Bootstrap the new attempt with the current assignment, tagged "apply immediately".
        // The validFromCheckpointId gating exists to align cross-subtask switching at the
        // same barrier; for a fresh attempt joining mid-job there's nothing to align with,
        // so it should adopt the latest assignment as fast as possible.
        if (!currentAssignment.isEmpty()) {
            gateway.sendEvent(new PartitionBucketAssignmentEvent(currentAssignment.asBootstrap()));
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable throwable) {
        LOG.warn("Subtask {} execution attempt {} failed", subtask, attemptNumber, throwable);
        if (subtask < gateways.length) {
            gateways[subtask] = null;
        }
    }

    /** Visible for testing. */
    public PartitionBucketAssignment getCurrentAssignment() {
        return currentAssignment;
    }

    private static byte[] serialize(PartitionBucketAssignment assignment) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(assignment);
            oos.flush();
            return baos.toByteArray();
        }
    }

    private static PartitionBucketAssignment deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (PartitionBucketAssignment) ois.readObject();
        }
    }
}
