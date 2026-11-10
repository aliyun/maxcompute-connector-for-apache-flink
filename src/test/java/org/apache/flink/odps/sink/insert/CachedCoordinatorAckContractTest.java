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
 */

package org.apache.flink.odps.sink.insert;

import static org.junit.Assert.*;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.utils.AsyncExecuteThread;
import org.apache.flink.odps.sink.utils.ActiveSessionInfo;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.junit.Test;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/** A finished task must not receive checkpoint ACKs; pending-end tasks still need them. */
public class CachedCoordinatorAckContractTest {
    @Test(timeout = 5000)
    public void endedEmptyTaskRetainsTheSharedSessionsCheckpoint() throws Exception {
        OperatorCoordinator.Context context =
                (OperatorCoordinator.Context)
                        Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[] {OperatorCoordinator.Context.class},
                                (p, m, a) -> m.getName().equals("currentParallelism") ? 3 : null);
        StubCoordinator coordinator = new StubCoordinator(context);
        coordinator.start();
        try {
            coordinator.configure(new int[3]);
            coordinator.addPendingSession(5);
            coordinator.inputCommitted(1, 6);
            coordinator.drain();
            assertEquals(1, coordinator.requests.size());
            assertEquals(5, coordinator.requests.get(0).getCheckpointId());
        } finally {
            coordinator.close();
        }
    }

    @Test(timeout = 5000)
    public void endedTaskDoesNotHoldLaterCheckpointAck() throws Exception {
        OperatorCoordinator.Context context =
                (OperatorCoordinator.Context)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {OperatorCoordinator.Context.class},
                                (proxy, method, args) ->
                                        method.getName().equals("currentParallelism") ? 3 : null);
        StubCoordinator coordinator = new StubCoordinator(context);
        coordinator.start();
        try {
            int[] delivered = new int[3];
            coordinator.configure(delivered);
            coordinator.success(2, 6);
            coordinator.inputCommitted(1, 6);
            coordinator.drain();
            assertArrayEquals(new int[] {0, 0, 1}, delivered);
        } finally {
            coordinator.close();
        }
    }

    @Test(timeout = 5000)
    public void checkpointAckSkipsFinishedTaskButUnblocksPendingEndTask() throws Exception {
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        OperatorCoordinator.Context context =
                (OperatorCoordinator.Context)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {OperatorCoordinator.Context.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("currentParallelism")) return 3;
                                    if (method.getName().equals("failJob"))
                                        failures.add((Throwable) args[0]);
                                    return null;
                                });
        StubCoordinator coordinator = new StubCoordinator(context);
        coordinator.start();
        try {
            int[] delivered = new int[3];
            coordinator.configure(delivered);
            coordinator.success(2, 5);
            coordinator.notifyCheckpointComplete(5);
            coordinator.success(1, 5);
            coordinator.drain();
            assertArrayEquals(new int[] {0, 1, 1}, delivered);
            assertTrue("coordinator failure " + failures, failures.isEmpty());
        } finally {
            coordinator.close();
        }
    }

    @Test(timeout = 5000)
    public void failedSharedWriterRequestsGlobalReset() throws Exception {
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        OperatorCoordinator.Context context =
                (OperatorCoordinator.Context)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {OperatorCoordinator.Context.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("currentParallelism")) return 3;
                                    if (method.getName().equals("failJob"))
                                        failures.add((Throwable) args[0]);
                                    return null;
                                });
        StubCoordinator coordinator = new StubCoordinator(context);
        coordinator.start();
        try {
            java.io.IOException exhausted = new java.io.IOException("upload retries exhausted");
            coordinator.executionAttemptFailed(1, 0, exhausted);
            assertEquals(1, failures.size());
            assertSame(exhausted, failures.get(0).getCause());
            coordinator.executionAttemptFailed(2, 0, null);
            assertEquals(
                    "cancellation notification must not request another restart",
                    1,
                    failures.size());
        } finally {
            coordinator.close();
        }
    }

    @Test(timeout = 5000)
    public void checkpointCompletionBeforeLastWriterDoesNotLoseParticipants() throws Exception {
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        OperatorCoordinator.Context context =
                (OperatorCoordinator.Context)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {OperatorCoordinator.Context.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("currentParallelism")) return 3;
                                    if (method.getName().equals("failJob"))
                                        failures.add((Throwable) args[0]);
                                    return null;
                                });
        StubCoordinator coordinator = new StubCoordinator(context);
        coordinator.start();
        try {
            int[] delivered = new int[3];
            coordinator.configure(delivered);
            coordinator.clearCompletionMarkers();
            coordinator.success(0, 5);
            coordinator.notifyCheckpointComplete(5);
            coordinator.success(1, 5);
            coordinator.success(2, 5);
            coordinator.drain();
            assertArrayEquals(new int[] {1, 1, 1}, delivered);
            coordinator.success(2, 5);
            coordinator.drain();
            assertArrayEquals(
                    "duplicate success cannot resend to a task that already finished",
                    new int[] {1, 1, 1},
                    delivered);
            assertTrue(failures.toString(), failures.isEmpty());
        } finally {
            coordinator.close();
        }
    }

    private static final class StubCoordinator extends FileCachedInsertCoordinator {
        private final List<AsyncExecuteThread.ExecuteRequest> requests = new ArrayList<>();
        StubCoordinator(OperatorCoordinator.Context context) {
            super(
                    "cached-ack",
                    new Configuration(),
                    context,
                    new OdpsConf("test", "test", "http://127.0.0.1:1", "test_project"),
                    TableIdentifier.of("test_project", "default", "table"),
                    "",
                    DataSchema.newBuilder()
                            .columns(Collections.emptyList())
                            .partitionBy(Collections.emptyList())
                            .build(),
                    true,
                    false,
                    null);
        }

        @Override
        protected OdpsMetaDataProvider getTableMetaProvider() {
            return null;
        }

        @Override
        protected void initEnv() {
            set("executeThreads", new AsyncExecuteThread[0]);
            set("taskCheckpointStatus", new HashMap<Long, Set<Integer>>());
            set("taskStatus", new HashMap<>());
            set("checkpointTimes", new HashMap<>());
            set("commitRowCount", new HashMap<>());
            set("currentActiveSessions", new HashMap<>());
            pendingCompletedTaskIds = new HashSet<>();
        }

        @Override
        protected void addExecuteRequest(String partition, AsyncExecuteThread.ExecuteRequest request) {
            requests.add(request);
        }

        void addPendingSession(long checkpoint) {
            ActiveSessionInfo session = new ActiveSessionInfo(checkpoint);
            session.addCompleteTask(2);
            Map<String, ActiveSessionInfo> sessions = new HashMap<>();
            sessions.put("session", session);
            Map<String, Map<String, ActiveSessionInfo>> partitions = new HashMap<>();
            partitions.put("", sessions);
            set("currentActiveSessions", partitions);
        }

        private void set(String name, Object value) {
            try {
                Field field = FileCachedInsertCoordinator.class.getDeclaredField(name);
                field.setAccessible(true);
                field.set(this, value);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        void clearCompletionMarkers() {
            completedTaskIds.clear();
            pendingCompletedTaskIds.clear();
        }

        void configure(int[] delivered) {
            completedTaskIds.add(0);
            pendingCompletedTaskIds.add(1);
            for (int id = 0; id < 3; id++) {
                final int task = id;
                gateways[id] =
                        (OperatorCoordinator.SubtaskGateway)
                                Proxy.newProxyInstance(
                                        getClass().getClassLoader(),
                                        new Class<?>[] {OperatorCoordinator.SubtaskGateway.class},
                                        (proxy, method, args) -> {
                                            if (method.getName().equals("sendEvent")) {
                                                delivered[task]++;
                                                return CompletableFuture.completedFuture(
                                                        Acknowledge.get());
                                            }
                                            if (method.getName().equals("getSubtask")) return task;
                                            return null;
                                        });
            }
        }

        void success(int task, long checkpoint) throws Exception {
            executor.submit(
                            () ->
                                    handleCheckpointSuccessEvent(
                                            SinkTaskEvent.builder()
                                                    .taskID(task)
                                                    .checkpointID(checkpoint)
                                                    .checkpointSuccess(true)
                                                    .build()),
                            "checkpoint success")
                    .get();
        }

        void inputCommitted(int task, long checkpoint) throws Exception {
            executor.submit(
                            () -> handleEndInputEvent(SinkTaskEvent.builder()
                                    .taskID(task).checkpointID(checkpoint)
                                    .endInput(true).inputCommitted(true).build()),
                            "fully committed input")
                    .get();
        }

        void drain() throws Exception {
            executor.submit(() -> {}, "drain").get();
        }
    }
}
