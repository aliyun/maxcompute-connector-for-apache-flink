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

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfoFactory;
import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignment;
import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignmentEvent;
import org.apache.flink.odps.sink.shuffle.PartitionBucketAssignmentRegistry;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKeyExtractor;
import org.apache.flink.odps.sink.shuffle.PartitionBucketPartitioner;
import org.apache.flink.odps.sink.shuffle.PartitionBucketShuffleCoordinator;
import org.apache.flink.odps.sink.shuffle.PartitionBucketStatistics;
import org.apache.flink.odps.sink.shuffle.PartitionBucketStatisticsOperator;
import org.apache.flink.odps.sink.shuffle.PartitionBucketStatisticsEvent;
import org.apache.flink.odps.sink.shuffle.RegistryAssignmentSource;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Fault-recovery acceptance matrix for the dynamic shuffle sink.
 *
 * <p>Covers 用例 D (故障恢复) of {@code docs/dynamic-shuffle-sink-test-plan.md} and the
 * rescale combinations the plan left implicit, as a deterministic in-JVM matrix with no
 * MaxCompute dependency, so it is repeatable in CI. Matrix rows FM-01 .. FM-12 are described,
 * with their mapping to the plan and to the design invariants, in
 * {@code docs/dynamic-shuffle-sink-fault-recovery-matrix.md}.
 *
 * <p>Layering: this class drives the Flink {@code OperatorCoordinator} / one-input-operator
 * contract directly — that contract is the code under test — while
 * {@code DynamicShuffleSinkFailoverMiniClusterTest} exercises the same invariants through
 * Flink's own failure handling, so nothing here is asserted purely on the assumption that the
 * framework calls these methods the way the design documents it.
 *
 * <p>Run: {@code mvn -Dmaven.test.skip=false test
 * -Dtest=DynamicShuffleFaultRecoveryMatrixTest}. On JDK 17 the {@code jdk17-plus} profile adds
 * the {@code --add-opens} flags that Flink's closure cleaner needs.
 */
public class DynamicShuffleFaultRecoveryMatrixTest {

    private static final int BUCKET_NUM = 16;

    private final List<String> usedUids = new ArrayList<>();

    @After
    public void cleanupRegistry() {
        for (String uid : usedUids) {
            PartitionBucketAssignmentRegistry.getInstance().removeAssignment(uid);
        }
        usedUids.clear();
    }

    // ==========================================================================
    // FM-01 coordinator checkpoint state round-trip
    // design §5.4 step 2; plan 用例 D "version 跨 restart 单调"
    // ==========================================================================
    @Test
    public void fm01_coordinatorStateRoundTripKeepsVersionBarrierAndRouting() throws Exception {
        PartitionBucketShuffleCoordinator before = newCoordinator(2, 2);
        before.start();

        reportStats(before, 0, key("ds=20240101", 0), 100L);
        reportStats(before, 1, key("ds=20240101", 1), 900L);
        reportStats(before, 0, key("ds=20240102", 3), 50L);

        byte[] state = checkpoint(before, 5L);
        PartitionBucketAssignment preCrash = before.getCurrentAssignment();
        Assert.assertEquals("assignment must carry a version", 1L, preCrash.getVersion());
        Assert.assertEquals(
                "assignment must be tagged validFrom the checkpoint after the one it was computed at",
                6L, preCrash.getValidFromCheckpointId());

        PartitionBucketShuffleCoordinator after = newCoordinator(2, 2);
        after.start();
        after.resetToCheckpoint(5L, state);
        PartitionBucketAssignment restored = after.getCurrentAssignment();

        Assert.assertEquals(preCrash.getVersion(), restored.getVersion());
        Assert.assertEquals(preCrash.getValidFromCheckpointId(), restored.getValidFromCheckpointId());
        Assert.assertEquals(
                "deserialized keys must stay equal and identically hashed, otherwise routing lookups miss",
                preCrash.getAssignment(), restored.getAssignment());

        String uid = newUid("fm01");
        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            op.deliver(restored.asBootstrap());
            Assert.assertEquals("restored version must be adoptable", 1L,
                    registryAssignment(uid).getVersion());
            RegistryAssignmentSource source = new RegistryAssignmentSource(uid);
            for (Map.Entry<PartitionBucketKey, Integer> entry : preCrash.getAssignment().entrySet()) {
                Assert.assertEquals(
                        "routing for " + entry.getKey() + " must survive the coordinator restart",
                        entry.getValue().intValue(), source.route(entry.getKey(), 2));
            }
        }
        after.close();
        before.close();
    }

    // ==========================================================================
    // FM-02 version monotonic across coordinator restart with surviving registries
    // design §5.4 "为什么必须持久化 currentAssignment"
    // ==========================================================================
    @Test
    public void fm02_versionStaysMonotonicAcrossCoordinatorRestart() throws Exception {
        String uid = newUid("fm02");
        PartitionBucketShuffleCoordinator first = newCoordinator(2, 2);
        first.start();
        List<OperatorCoordinator.SubtaskGateway> gateways = registerGateways(first, 2);
        try (OperatorAttempt op0 = newOperator(uid, keyExtractor(), 0);
             OperatorAttempt op1 = newOperator(uid, keyExtractor(), 1)) {

            byte[] lastState = null;
            for (long ckpt = 1L; ckpt <= 3L; ckpt++) {
                reportStats(first, 0, key("ds=stable", 0), 100L);
                reportStats(first, 1, key("ds=stable", 1), 100L);
                // one new hot key per round keeps the computed map changing, so the version bumps
                reportStats(first, 0, key("ds=round" + ckpt, (int) ckpt), 10_000L * ckpt);
                lastState = checkpoint(first, ckpt);
                first.notifyCheckpointComplete(ckpt);
                applyAll(gateways, Arrays.asList(op0, op1));
                Assert.assertEquals(ckpt, first.getCurrentAssignment().getVersion());
                Assert.assertEquals(ckpt, registryAssignment(uid).getVersion());
            }

            // JobManager dies here: a brand new coordinator instance, restored from the last state.
            PartitionBucketShuffleCoordinator second = newCoordinator(2, 2);
            second.start();
            List<OperatorCoordinator.SubtaskGateway> newGateways = registerGateways(second, 2);
            second.resetToCheckpoint(3L, lastState);
            Assert.assertEquals(
                    "restored coordinator must resume at the pre-crash version",
                    3L, second.getCurrentAssignment().getVersion());

            reportStats(second, 0, key("ds=stable", 0), 100L);
            reportStats(second, 1, key("ds=stable", 1), 100L);
            reportStats(second, 0, key("ds=aftercrash", 7), 20_000L);
            checkpoint(second, 4L);
            second.notifyCheckpointComplete(4L);
            applyAll(newGateways, Arrays.asList(op0, op1));

            Assert.assertEquals(4L, second.getCurrentAssignment().getVersion());
            Assert.assertEquals(
                    "surviving subtask registries must accept the post-restart assignment "
                            + "(this is the freeze the design avoids by persisting currentAssignment)",
                    4L, registryAssignment(uid).getVersion());
            second.close();
        }
        first.close();
    }

    // ==========================================================================
    // FM-03 restoring without state freezes routing; only a task restart heals it
    // design §11-8 (serialization compat) and §5.4 rationale, as a negative control
    // ==========================================================================
    @Test
    public void fm03_statelessCoordinatorRestoreFreezesRoutingUntilTaskRestartHealsIt()
            throws Exception {
        String uid = newUid("fm03");
        PartitionBucketShuffleCoordinator coordinator = newCoordinator(2, 2);
        coordinator.start();
        List<OperatorCoordinator.SubtaskGateway> gateways = registerGateways(coordinator, 2);

        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            // The subtasks are alive and already hold a high version, as they would after a
            // long run of a previous connector build.
            Map<PartitionBucketKey, Integer> staleMap = new HashMap<>();
            staleMap.put(key("ds=stable", 0), 1);
            staleMap.put(key("ds=stable", 1), 1);
            applyAssignmentDirectly(uid, new PartitionBucketAssignment(staleMap, 50L));
            Assert.assertEquals(50L, registryAssignment(uid).getVersion());

            // The coordinator comes back WITHOUT state: resetToCheckpoint(checkpointId, null).
            coordinator.resetToCheckpoint(1L, null);
            Assert.assertEquals(0L, coordinator.getCurrentAssignment().getVersion());

            // Stable traffic: the computed map is identical from the second round on, so the
            // version advances exactly once and the broadcast dedup suppresses everything after.
            for (long ckpt = 1L; ckpt <= 3L; ckpt++) {
                reportStats(coordinator, 0, key("ds=stable", 0), 100L);
                reportStats(coordinator, 1, key("ds=stable", 1), 100L);
                checkpoint(coordinator, ckpt);
                coordinator.notifyCheckpointComplete(ckpt);
                applyAll(gateways, Collections.singletonList(op));
            }
            Assert.assertEquals(
                    "stable traffic must produce at most one version bump",
                    1L, coordinator.getCurrentAssignment().getVersion());
            Assert.assertEquals(
                    "... and that version is rejected by a registry sitting at v50, so routing stays "
                            + "frozen on the stale assignment",
                    50L, registryAssignment(uid).getVersion());

            // Even a traffic shift every round only climbs one version per change: with a
            // stateless restore the coordinator has to catch up to 50 before anything applies.
            for (long ckpt = 4L; ckpt <= 8L; ckpt++) {
                reportStats(coordinator, 0, key("ds=stable", 0), 100L);
                reportStats(coordinator, 1, key("ds=stable", 1), 100L);
                reportStats(coordinator, 0, key("ds=shift" + ckpt, (int) ckpt), 9_000L * ckpt);
                checkpoint(coordinator, ckpt);
                coordinator.notifyCheckpointComplete(ckpt);
                applyAll(gateways, Collections.singletonList(op));
            }
            long stuckVersion = coordinator.getCurrentAssignment().getVersion();
            Assert.assertTrue("coordinator did keep counting", stuckVersion > 1L);
            Assert.assertEquals(
                    "still below 50: nothing the coordinator computes can reach the subtasks "
                            + "(R-2 in the matrix doc)",
                    50L, registryAssignment(uid).getVersion());

            // The only heal path while the coordinator is stateless is a task restart, which
            // releases the registry entry and re-registers it at v0.
            op.closeAndReopen();
            Assert.assertTrue(
                    "a restarted attempt re-registers and starts from an empty assignment",
                    registryAssignment(uid).isEmpty());

            RecordingGateway replacement = new RecordingGateway(0, 1);
            coordinator.executionAttemptReady(0, 1, replacement);
            coordinator.notifyCheckpointComplete(9L);
            applyAll(Collections.singletonList((OperatorCoordinator.SubtaskGateway) replacement),
                    Collections.singletonList(op));
            Assert.assertEquals(
                    "the restarted attempt adopts the current assignment through bootstrap",
                    stuckVersion, registryAssignment(uid).getVersion());
        }
        coordinator.close();
    }

    // ==========================================================================
    // FM-04 aborted checkpoint must not broadcast; the pending version is delivered
    //        by the next completed checkpoint (no subtask stays stale)
    // design §5.1 / notifyCheckpointComplete skip logic; plan 用例 D "checkpoint 前失败"
    // ==========================================================================
    @Test
    public void fm04_abortedCheckpointSkipsBroadcastAndNextCompletionDeliversIt() throws Exception {
        String uid = newUid("fm04");
        PartitionBucketShuffleCoordinator coordinator = newCoordinator(1, 1);
        coordinator.start();
        List<OperatorCoordinator.SubtaskGateway> gateways = registerGateways(coordinator, 1);
        RecordingGateway gw = (RecordingGateway) gateways.get(0);

        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            op.send(row(1L, "ds=20240101"));
            op.barrier(5L);

            reportStats(coordinator, 0, key("ds=20240101", 1 % BUCKET_NUM), 1L);
            reportStats(coordinator, 0, key("ds=20240101", 2 % BUCKET_NUM), 9L);
            checkpoint(coordinator, 6L);
            gw.reset();

            coordinator.notifyCheckpointAborted(6L);
            Assert.assertEquals(
                    "an aborted checkpoint must not broadcast an assignment: subtasks would apply a "
                            + "version that no completed checkpoint contains",
                    0, gw.deliveredCount());

            // Next checkpoint: identical traffic => map unchanged, version stays v1.
            checkpoint(coordinator, 7L);
            coordinator.notifyCheckpointComplete(7L);
            Assert.assertEquals(
                    "the not-yet-broadcast assignment must go out with the next completed checkpoint",
                    1, gw.deliveredCount());
            PartitionBucketAssignment broadcast = latestReceived(gw);
            Assert.assertEquals(1L, broadcast.getVersion());

            op.barrier(7L);
            op.deliver(broadcast);
            Assert.assertEquals(
                    "a delivery that lands after its barrier must still be applied (late path)",
                    1L, registryAssignment(uid).getVersion());
        }
        coordinator.close();
    }

    // ==========================================================================
    // FM-05 subtask attempt gone at broadcast time => bootstrap on attempt-ready
    // design §5.3 / §5.5; plan 用例 D "StatsOp 重启 bootstrap 路径"
    // ==========================================================================
    @Test
    public void fm05_deadGatewayAtBroadcastIsDeliveredOnAttemptReady() throws Exception {
        PartitionBucketShuffleCoordinator coordinator = newCoordinator(2, 2);
        coordinator.start();
        List<OperatorCoordinator.SubtaskGateway> gateways = registerGateways(coordinator, 2);
        RecordingGateway gw0 = (RecordingGateway) gateways.get(0);
        RecordingGateway gw1 = (RecordingGateway) gateways.get(1);

        reportStats(coordinator, 0, key("ds=a", 0), 10L);
        reportStats(coordinator, 1, key("ds=a", 1), 900L);
        checkpoint(coordinator, 3L);

        // subtask 1's attempt died before the broadcast went out
        coordinator.executionAttemptFailed(1, 0, new RuntimeException("injected TaskManager loss"));
        coordinator.subtaskReset(1, 3L);
        coordinator.notifyCheckpointComplete(3L);

        Assert.assertEquals("live subtask still gets the assignment", 1, gw0.deliveredCount());
        Assert.assertEquals("dead attempt must not be addressed", 0, gw1.deliveredCount());

        RecordingGateway replacement = new RecordingGateway(1, 1);
        coordinator.executionAttemptReady(1, 1, replacement);
        Assert.assertEquals(
                "the new attempt must be bootstrapped with the current assignment",
                1, replacement.deliveredCount());
        PartitionBucketAssignment bootstrapped = latestReceived(replacement);
        Assert.assertEquals(1L, bootstrapped.getVersion());
        Assert.assertEquals(
                "a fresh attempt has nothing to align with, so it must apply immediately",
                PartitionBucketAssignment.APPLY_IMMEDIATELY, bootstrapped.getValidFromCheckpointId());
        coordinator.close();
    }

    // ==========================================================================
    // FM-06 lost event delivery heals through Flink's failover + bootstrap; the
    //        broadcast-skip means it is NOT re-sent on the next checkpoint.
    // design §5.3, notifyCheckpointComplete dedup; residual risk R-1 in the matrix doc
    // ==========================================================================
    @Test
    public void fm06_lostDeliveryRecoversThroughFailoverBootstrap() throws Exception {
        String uid = newUid("fm06");
        PartitionBucketShuffleCoordinator coordinator = newCoordinator(2, 2);
        coordinator.start();
        List<OperatorCoordinator.SubtaskGateway> gateways = registerGateways(coordinator, 2);
        RecordingGateway gw0 = (RecordingGateway) gateways.get(0);
        RecordingGateway gw1 = (RecordingGateway) gateways.get(1);
        gw1.failDelivery(true);

        try (OperatorAttempt op0 = newOperator(uid, keyExtractor(), 0);
             OperatorAttempt op1 = newOperator(uid, keyExtractor(), 1)) {

            reportStats(coordinator, 0, key("ds=a", 0), 10L);
            reportStats(coordinator, 1, key("ds=a", 1), 900L);
            checkpoint(coordinator, 1L);
            coordinator.notifyCheckpointComplete(1L); // must not throw on the failed delivery

            applyAll(gateways, Arrays.asList(op0, op1));
            gw1.failDelivery(false);
            Assert.assertEquals(1, gw0.deliveredCount());
            Assert.assertEquals("delivery failed, subtask 1 never got it", 0, gw1.deliveredCount());

            // Same traffic => version unchanged => the dedup suppresses a re-broadcast.
            checkpoint(coordinator, 2L);
            coordinator.notifyCheckpointComplete(2L);
            Assert.assertEquals(
                    "documented current behaviour: a lost event is NOT re-broadcast while the "
                            + "assignment version is unchanged; recovery relies on Flink failing the "
                            + "task on event loss (R-1)",
                    0, gw1.deliveredCount());

            // ... which is exactly what the next assertion simulates.
            coordinator.executionAttemptFailed(1, 0, new RuntimeException("injected event-loss failover"));
            coordinator.subtaskReset(1, 2L);
            op1.closeAndReopen();
            RecordingGateway replacement = new RecordingGateway(1, 1);
            coordinator.executionAttemptReady(1, 1, replacement);
            applyAll(Collections.singletonList((OperatorCoordinator.SubtaskGateway) replacement),
                    Collections.singletonList(op1));

            Assert.assertEquals(
                    "after the failover both attempts must converge on the same version",
                    coordinator.getCurrentAssignment().getVersion(),
                    registryAssignment(uid).getVersion());
            Set<Integer> used = new HashSet<>();
            for (Map.Entry<PartitionBucketKey, Integer> e : registryAssignment(uid).getAssignment().entrySet()) {
                used.add(new RegistryAssignmentSource(uid).route(e.getKey(), 2));
            }
            Assert.assertEquals("routing must be single-writer for every key", 2, used.size());
        }
        coordinator.close();
    }

    // ==========================================================================
    // FM-07 rescale down: restored assignment references sink subtasks that no
    //        longer exist; routing must stay deterministic and converge.
    // plan 用例 D "rescale"; design §10.1 (fallback) / §9
    // ==========================================================================
    @Test
    public void fm07_rescaleDownKeepsSingleWriterAndRecomputesIntoRange() throws Exception {
        String uid = newUid("fm07");
        PartitionBucketShuffleCoordinator wide = newCoordinator(4, 4);
        wide.start();
        List<OperatorCoordinator.SubtaskGateway> wideGateways = registerGateways(wide, 4);
        for (int i = 0; i < 4; i++) {
            reportStats(wide, i, key("ds=a", i), 1_000L * (i + 1));
        }
        byte[] state = checkpoint(wide, 1L);
        wide.notifyCheckpointComplete(1L);
        PartitionBucketAssignment fourWay = latestReceived((RecordingGateway) wideGateways.get(0));
        Set<Integer> preRescaleTargets = new HashSet<>(fourWay.getAssignment().values());
        Assert.assertTrue("expected the assignment to spread over >1 sink subtask",
                preRescaleTargets.size() > 1);

        // Rescale the sink to 2 subtasks: a fresh coordinator instance with the new parallelism,
        // restored from the previous state.
        PartitionBucketShuffleCoordinator narrow = newCoordinator(4, 2);
        narrow.start();
        narrow.resetToCheckpoint(1L, state);
        Assert.assertEquals(fourWay.getVersion(), narrow.getCurrentAssignment().getVersion());

        List<OperatorCoordinator.SubtaskGateway> restoredGateways = registerGateways(narrow, 4);
        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            PartitionBucketAssignment bootstrapped =
                    latestReceived((RecordingGateway) restoredGateways.get(0));
            op.deliver(bootstrapped);
            Assert.assertEquals(
                    "the restored (pre-rescale) assignment must be adoptable after rescaling",
                    bootstrapped.getVersion(), registryAssignment(uid).getVersion());

            // Out-of-range entries fall back to the static hash; that must be a pure function of
            // the key: independent partitioner instances (one per upstream subtask) may never
            // disagree, or a (partition, bucket) group would get two writers.
            List<Map<PartitionBucketKey, Integer>> tables = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                tables.add(routingTable(uid, 2));
            }
            for (Map<PartitionBucketKey, Integer> table : tables) {
                Assert.assertEquals("all upstream subtasks must agree on routing", tables.get(0), table);
            }
            for (Integer target : tables.get(0).values()) {
                Assert.assertTrue("routing must stay inside the new sink parallelism",
                        target >= 0 && target < 2);
            }

            // The next recompute is done against the new parallelism, so it converges exactly.
            for (int i = 0; i < 4; i++) {
                reportStats(narrow, i, key("ds=a", i), 1_000L * (i + 1));
            }
            checkpoint(narrow, 2L);
            narrow.notifyCheckpointComplete(2L);
            RecordingGateway gw = (RecordingGateway) restoredGateways.get(0);
            PartitionBucketAssignment recomputed = latestReceived(gw);
            Assert.assertTrue(recomputed.getVersion() > bootstrapped.getVersion());
            for (Integer target : recomputed.getAssignment().values()) {
                Assert.assertTrue(
                        "a recomputed assignment must be inside the new sink bound by construction",
                        target >= 0 && target < 2);
            }
            applyAtBarrier(op, recomputed);
            PartitionBucketAssignment converged = registryAssignment(uid);
            Assert.assertEquals(recomputed.getVersion(), converged.getVersion());
            RegistryAssignmentSource source = new RegistryAssignmentSource(uid);
            for (Map.Entry<PartitionBucketKey, Integer> e : converged.getAssignment().entrySet()) {
                Assert.assertEquals(
                        "once in range, routing must follow the coordinator's map exactly",
                        e.getValue().intValue(), source.route(e.getKey(), 2));
            }
        }
        narrow.close();
        wide.close();
    }

    // ==========================================================================
    // FM-08 rescale up: restored low-parallelism assignment stays valid (under-utilised,
    //        not incorrect) until the next recompute spreads the load.
    // ==========================================================================
    @Test
    public void fm08_rescaleUpKeepsSingleWriterUntilRecomputeSpreadsLoad() throws Exception {
        String uid = newUid("fm08");
        PartitionBucketShuffleCoordinator small = newCoordinator(2, 2);
        small.start();
        List<OperatorCoordinator.SubtaskGateway> smallGateways = registerGateways(small, 2);
        reportStats(small, 0, key("ds=a", 0), 1_000L);
        reportStats(small, 1, key("ds=a", 1), 1_000L);
        byte[] state = checkpoint(small, 1L);

        PartitionBucketShuffleCoordinator grown = newCoordinator(2, 4);
        grown.start();
        grown.resetToCheckpoint(1L, state);
        List<OperatorCoordinator.SubtaskGateway> grownGateways = registerGateways(grown, 2);

        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            op.deliver(latestReceived((RecordingGateway) grownGateways.get(0)));
            Map<PartitionBucketKey, Integer> table = routingTable(uid, 4);
            for (Integer target : table.values()) {
                Assert.assertTrue("a pre-rescale assignment never routes past the new bound",
                        target >= 0 && target < 4);
            }
            List<Map<PartitionBucketKey, Integer>> independent = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                independent.add(routingTable(uid, 4));
            }
            for (Map<PartitionBucketKey, Integer> other : independent) {
                Assert.assertEquals(
                        "independent partitioner instances may never disagree on a key's sink",
                        table, other);
            }

            reportStats(grown, 0, key("ds=a", 0), 1_000L);
            reportStats(grown, 1, key("ds=a", 1), 1_000L);
            reportStats(grown, 0, key("ds=a", 2), 1_000L);
            reportStats(grown, 1, key("ds=a", 3), 1_000L);
            checkpoint(grown, 2L);
            grown.notifyCheckpointComplete(2L);
            applyAtBarrier(op, latestReceived((RecordingGateway) grownGateways.get(0)));
            Set<Integer> targets = new HashSet<>(registryAssignment(uid).getAssignment().values());
            Assert.assertTrue("after rescaling up the recompute must use more than 2 sink subtasks",
                    targets.size() > 2);
        }
        grown.close();
        small.close();
    }

    // ==========================================================================
    // FM-09 stale and out-of-order events after failover are rejected
    // design §10.5
    // ==========================================================================
    @Test
    public void fm09_staleAndOutOfOrderAssignmentsAreRejected() throws Exception {
        String uid = newUid("fm09");
        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            op.send(row(1L, "ds=20240101"));
            op.barrier(10L);

            Map<PartitionBucketKey, Integer> newer = new HashMap<>();
            newer.put(key("ds=20240101", 0), 1);
            Map<PartitionBucketKey, Integer> older = new HashMap<>();
            older.put(key("ds=20240101", 0), 0);

            op.deliver(new PartitionBucketAssignment(newer, 5L, 20L));
            op.deliver(new PartitionBucketAssignment(older, 3L, 12L));
            op.barrier(20L);
            Assert.assertEquals(
                    "a stale assignment queued behind a newer one must not win",
                    5L, registryAssignment(uid).getVersion());

            // Late redelivery of the old version (e.g. after an attempt restart + replay).
            op.deliver(new PartitionBucketAssignment(older, 3L, 12L));
            Assert.assertEquals(
                    "a version at or below the applied one must be dropped by the registry",
                    5L, registryAssignment(uid).getVersion());
            Assert.assertEquals(
                    "routing must stay on the newer assignment",
                    Integer.valueOf(1),
                    Integer.valueOf(new RegistryAssignmentSource(uid).route(key("ds=20240101", 0), 2)));
        }
    }

    // ==========================================================================
    // FM-10 registry lifecycle: a restarting attempt releases and re-registers, and the
    //        entry is gone once the last attempt closes (no leak in a shared TM JVM).
    // plan 用例 F "registry 累积"; design §3.4 refcount
    // ==========================================================================
    @Test
    public void fm10_registryRefcountReleasedAcrossAttemptsAndNoStaleEntryKept() throws Exception {
        String uid = newUid("fm10");
        PartitionBucketKeyExtractor extractor = keyExtractor();

        Map<PartitionBucketKey, Integer> map = new HashMap<>();
        map.put(key("ds=a", 0), 1);
        PartitionBucketAssignment v7 = new PartitionBucketAssignment(map, 7L,
                PartitionBucketAssignment.APPLY_IMMEDIATELY);

        OperatorAttempt first = newOperator(uid, extractor);
        first.deliver(v7);
        Assert.assertEquals(7L, registryAssignment(uid).getVersion());
        first.close();

        // Entry was released, so an update for the same uid must be dropped (registry is a no-op
        // for unknown uids) and the assignment must read as empty.
        applyAssignmentDirectly(uid, new PartitionBucketAssignment(map, 8L));
        Assert.assertTrue(
                "after the last attempt closed, the registry entry must be gone (no leak)",
                registryAssignment(uid).isEmpty());

        try (OperatorAttempt second = newOperator(uid, extractor)) {
            second.deliver(v7);
            Assert.assertEquals(
                    "a re-registered attempt must be able to adopt the current assignment again",
                    7L, registryAssignment(uid).getVersion());
        }
        Assert.assertTrue(
                "the second attempt's close must release its registration too",
                registryAssignment(uid).isEmpty());
    }

    // ==========================================================================
    // FM-11 vfci barrier gating: an assignment becomes effective exactly at its barrier
    // design §5.1 / §5.2; the baseline the failure rows are compared against
    // ==========================================================================
    @Test
    public void fm11_assignmentBecomesEffectiveExactlyAtItsBarrier() throws Exception {
        String uid = newUid("fm11");
        try (OperatorAttempt op = newOperator(uid, keyExtractor())) {
            op.send(row(1L, "ds=20240101"));
            op.barrier(10L);
            Assert.assertEquals(
                    "the barrier must report statistics for the window it closes",
                    1, op.reportedStats().size());

            Map<PartitionBucketKey, Integer> map = new HashMap<>();
            map.put(key("ds=20240101", 1 % BUCKET_NUM), 1);
            PartitionBucketAssignment v1 = new PartitionBucketAssignment(map, 1L, 11L);

            op.deliver(v1);
            Assert.assertTrue(
                    "vfci=11 must not be applied before barrier 11 arrives",
                    registryAssignment(uid).isEmpty());

            op.send(row(2L, "ds=20240101"));
            op.barrier(10L); // a duplicate/older barrier must not switch either
            Assert.assertTrue(registryAssignment(uid).isEmpty());

            op.barrier(11L);
            Assert.assertEquals(
                    "barrier 11 is where the switch happens",
                    1L, registryAssignment(uid).getVersion());
        }
    }

    // ==========================================================================
    // FM-12 repeated failovers: 3 rounds of {compute, broadcast, subtask loss,
    //        coordinator restart}; version stays monotonic and routing converges.
    // plan 用例 D "进阶：再 kill 一次 JobManager"
    // ==========================================================================
    @Test
    public void fm12_repeatedFailoverCyclesStayMonotonicAndConverge() throws Exception {
        String uid = newUid("fm12");
        PartitionBucketKeyExtractor extractor = keyExtractor();
        OperatorAttempt op0 = newOperator(uid, extractor, 0);
        OperatorAttempt op1 = newOperator(uid, extractor, 1);
        try {
            PartitionBucketShuffleCoordinator coordinator = newCoordinator(2, 2);
            coordinator.start();
            List<OperatorCoordinator.SubtaskGateway> gateways = registerGateways(coordinator, 2);
            long lastVersion = 0L;

            for (long round = 1L; round <= 3L; round++) {
                reportStats(coordinator, 0, key("ds=a", 0), 100L);
                reportStats(coordinator, 1, key("ds=a", 1), 100L);
                reportStats(coordinator, 0, key("ds=round" + round, (int) round), 9_000L * round);
                byte[] state = checkpoint(coordinator, round);
                coordinator.notifyCheckpointComplete(round);
                applyAll(gateways, Arrays.asList(op0, op1));

                // (a) one sink attempt dies and comes back mid-round
                coordinator.executionAttemptFailed(1, (int) round,
                        new RuntimeException("injected TaskManager loss"));
                coordinator.subtaskReset(1, round);
                op1.closeAndReopen();
                RecordingGateway replacement = new RecordingGateway(1, (int) round + 1);
                coordinator.executionAttemptReady(1, (int) round + 1, replacement);
                applyAtBarrier(op1, latestReceived(replacement));

                // (b) the JobManager dies too: restore a fresh coordinator from the round's state
                PartitionBucketShuffleCoordinator restored = newCoordinator(2, 2);
                restored.start();
                restored.resetToCheckpoint(round, state);
                coordinator.close();
                coordinator = restored;
                gateways = registerGateways(coordinator, 2);

                long version = coordinator.getCurrentAssignment().getVersion();
                Assert.assertTrue(
                        "version must be strictly monotonic across both failure modes",
                        version > lastVersion);
                lastVersion = version;
                Assert.assertEquals(
                        "both live attempts must converge on the restored version",
                        version, registryAssignment(uid).getVersion());
            }
            coordinator.close();
        } finally {
            op0.close();
            op1.close();
        }
        Assert.assertTrue(
                "no registry entry may survive the job",
                registryAssignment(uid).isEmpty());
    }

    // ==========================================================================
    // helpers
    // ==========================================================================

    private String newUid(String prefix) {
        String uid = "DynamicShuffleFaultRecoveryMatrixTest/" + prefix + "-" + System.nanoTime();
        usedUids.add(uid);
        return uid;
    }

    private static PartitionBucketKey key(String partitionSpec, int bucketId) {
        return new PartitionBucketKey(partitionSpec, bucketId);
    }

    private static PartitionBucketShuffleCoordinator newCoordinator(int statsP, int sinkP) {
        return new PartitionBucketShuffleCoordinator(new OperatorID(), statsP, sinkP);
    }

    private static void reportStats(
            PartitionBucketShuffleCoordinator coordinator,
            int subtask,
            PartitionBucketKey key,
            long records) throws Exception {
        PartitionBucketStatistics stats = new PartitionBucketStatistics(subtask);
        stats.increment(key, records);
        coordinator.handleEventFromOperator(subtask, 0, new PartitionBucketStatisticsEvent(stats));
    }

    private static byte[] checkpoint(PartitionBucketShuffleCoordinator coordinator, long ckptId)
            throws Exception {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        coordinator.checkpointCoordinator(ckptId, future);
        Assert.assertTrue("checkpointCoordinator must complete the future", future.isDone());
        byte[] state = future.get();
        Assert.assertNotNull("coordinator state must be serialized for recovery", state);
        Assert.assertTrue("coordinator state must not be empty", state.length > 0);
        return state;
    }

    private static List<OperatorCoordinator.SubtaskGateway> registerGateways(
            PartitionBucketShuffleCoordinator coordinator, int parallelism) throws Exception {
        List<OperatorCoordinator.SubtaskGateway> gateways = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            RecordingGateway gateway = new RecordingGateway(i, 0);
            gateways.add(gateway);
            coordinator.executionAttemptReady(i, 0, gateway);
        }
        return gateways;
    }

    private static PartitionBucketAssignment latestReceived(RecordingGateway gateway) {
        List<PartitionBucketAssignment> received = gateway.assignments();
        Assert.assertFalse("gateway should have received an assignment", received.isEmpty());
        return received.get(received.size() - 1);
    }

    /**
     * Delivers the latest broadcast assignment to each operator and lets it take effect the way
     * a live subtask does: queued on receipt, applied at the snapshot barrier named by its
     * {@code validFromCheckpointId} (design §5.1 / §5.2). Bootstrap assignments (vfci == 0)
     * apply on receipt, which is what a freshly restarted attempt does.
     */
    private static void applyAll(
            List<OperatorCoordinator.SubtaskGateway> gateways,
            List<OperatorAttempt> operators) throws Exception {
        for (int i = 0; i < operators.size(); i++) {
            RecordingGateway gateway = (RecordingGateway) gateways.get(i % gateways.size());
            if (gateway.deliveredCount() > 0) {
                applyAtBarrier(operators.get(i), latestReceived(gateway));
            }
        }
    }

    private static void applyAtBarrier(OperatorAttempt op, PartitionBucketAssignment assignment)
            throws Exception {
        op.deliver(assignment);
        if (assignment.getValidFromCheckpointId()
                != PartitionBucketAssignment.APPLY_IMMEDIATELY) {
            op.barrier(assignment.getValidFromCheckpointId());
        }
    }

    private static void applyAssignmentDirectly(String uid, PartitionBucketAssignment assignment) {
        PartitionBucketAssignmentRegistry.getInstance().updateAssignment(uid, assignment);
    }

    private static PartitionBucketAssignment registryAssignment(String uid) {
        return PartitionBucketAssignmentRegistry.getInstance().getAssignment(uid);
    }

    /** The routing table as any one upstream subtask computes it for the given sink bound. */
    private static Map<PartitionBucketKey, Integer> routingTable(String uid, int numPartitions) {
        PartitionBucketPartitioner partitioner =
                new PartitionBucketPartitioner(new RegistryAssignmentSource(uid));
        Map<PartitionBucketKey, Integer> table = new LinkedHashMap<>();
        for (PartitionBucketKey key : registryAssignment(uid).getAssignment().keySet()) {
            table.put(key, partitioner.partition(key, numPartitions));
        }
        return table;
    }

    private static PartitionBucketKeyExtractor keyExtractor() {
        RowType flinkSchema = new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("ds", new VarCharType())));
        List<Column> odpsColumns = Arrays.asList(
                new Column("id", TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT)),
                new Column("name", TypeInfoFactory.getVarcharTypeInfo(255)),
                new Column("ds", TypeInfoFactory.getVarcharTypeInfo(255)));
        return new PartitionBucketKeyExtractor(
                odpsColumns,
                Collections.singletonList("id"),
                Collections.singletonList("ds"),
                BUCKET_NUM,
                Collections.emptyMap(),
                null,
                flinkSchema);
    }

    private static RowData row(long id, String partitionValue) {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, id);
        row.setField(1, StringData.fromString("name_" + id));
        row.setField(2, StringData.fromString(partitionValue));
        return row;
    }

    private static OperatorAttempt newOperator(String uid, PartitionBucketKeyExtractor extractor)
            throws Exception {
        return new OperatorAttempt(uid, extractor, 0);
    }

    private static OperatorAttempt newOperator(
            String uid, PartitionBucketKeyExtractor extractor, int subtaskIndex) throws Exception {
        return new OperatorAttempt(uid, extractor, subtaskIndex);
    }

    /**
     * A live {@link PartitionBucketStatisticsOperator} under Flink's operator test harness:
     * real snapshot boundaries, real registry calls, and a recorded stats channel so the test
     * can see exactly what the subtask reported.
     */
    private static final class OperatorAttempt implements AutoCloseable {
        private final String uid;
        private final PartitionBucketKeyExtractor extractor;
        private final int subtaskIndex;
        private final RecordingEventGateway eventGateway = new RecordingEventGateway();
        private OneInputStreamOperatorTestHarness<RowData, RowData> harness;
        private PartitionBucketStatisticsOperator operator;
        private boolean closed;

        OperatorAttempt(String uid, PartitionBucketKeyExtractor extractor, int subtaskIndex)
                throws Exception {
            this.uid = uid;
            this.extractor = extractor;
            this.subtaskIndex = subtaskIndex;
            start();
        }

        private void start() throws Exception {
            operator = new PartitionBucketStatisticsOperator(
                    extractor, eventGateway, uid);
            // (maxParallelism, operatorParallelism, subtaskIndex)
            harness = new OneInputStreamOperatorTestHarness<>(operator, 4, 2, subtaskIndex);
            harness.initializeEmptyState();
            harness.open();
            closed = false;
        }

        void send(RowData record) throws Exception {
            harness.processElement(new StreamRecord<>(record));
        }

        void barrier(long checkpointId) throws Exception {
            harness.snapshot(checkpointId, checkpointId * 1000L);
        }

        void deliver(PartitionBucketAssignment assignment) {
            operator.handleOperatorEvent(new PartitionBucketAssignmentEvent(assignment));
        }

        List<PartitionBucketStatistics> reportedStats() {
            return eventGateway.statistics();
        }

        /** Simulates a task-level failover: close the attempt, then start a fresh one. */
        void closeAndReopen() throws Exception {
            close();
            start();
        }

        @Override
        public void close() throws Exception {
            if (!closed) {
                harness.close();
                closed = true;
            }
        }
    }

    private static final class RecordingEventGateway implements OperatorEventGateway {
        private final List<OperatorEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void sendEventToCoordinator(OperatorEvent event) {
            events.add(event);
        }

        List<PartitionBucketStatistics> statistics() {
            List<PartitionBucketStatistics> out = new ArrayList<>();
            for (OperatorEvent event : events) {
                if (event instanceof PartitionBucketStatisticsEvent) {
                    out.add(((PartitionBucketStatisticsEvent) event).getStatistics());
                } else {
                    throw new IllegalStateException(
                            "unexpected event sent by the statistics operator: " + event);
                }
            }
            return out;
        }
    }

    /** Fake coordinator-side gateway that records delivered events and can lose them. */
    private static final class RecordingGateway implements OperatorCoordinator.SubtaskGateway {
        private final int subtask;
        private final int attempt;
        private final List<OperatorEvent> received = new CopyOnWriteArrayList<>();
        private volatile boolean failDelivery = false;

        RecordingGateway(int subtask, int attempt) {
            this.subtask = subtask;
            this.attempt = attempt;
        }

        @Override
        public CompletableFuture<Acknowledge> sendEvent(OperatorEvent event) {
            if (failDelivery) {
                CompletableFuture<Acknowledge> failed = new CompletableFuture<>();
                failed.completeExceptionally(
                        new RuntimeException("injected: operator event lost in transit"));
                return failed;
            }
            received.add(event);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        @Override
        public ExecutionAttemptID getExecution() {
            return new ExecutionAttemptID(
                    new ExecutionGraphID(),
                    new ExecutionVertexID(new JobVertexID(), subtask),
                    attempt);
        }

        @Override
        public int getSubtask() {
            return subtask;
        }

        void reset() {
            received.clear();
        }

        void failDelivery(boolean fail) {
            failDelivery = fail;
        }

        int deliveredCount() {
            return received.size();
        }

        List<PartitionBucketAssignment> assignments() {
            List<PartitionBucketAssignment> out = new ArrayList<>();
            for (OperatorEvent event : received) {
                out.add(((PartitionBucketAssignmentEvent) event).getAssignment());
            }
            return out;
        }
    }
}
