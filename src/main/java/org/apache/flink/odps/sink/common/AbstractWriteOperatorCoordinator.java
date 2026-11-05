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

package org.apache.flink.odps.sink.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.adapter.OperatorCoordinatorAdapter;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.utils.NonThrownExecutor;
import org.apache.flink.odps.util.VersionUtils;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public abstract class AbstractWriteOperatorCoordinator implements OperatorCoordinatorAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWriteOperatorCoordinator.class);

    protected final String operatorName;
    protected final Configuration conf;
    protected final OperatorCoordinator.Context context;
    protected transient OperatorCoordinator.SubtaskGateway[] gateways;
    protected transient SinkTaskEvent[] eventBuffer;
    protected final int parallelism;
    protected NonThrownExecutor executor;
    protected boolean started;
    protected volatile long attemptGeneration;
    protected volatile boolean allSubtaskReady;
    protected Object lock;
    protected transient Set<Integer> completedTaskIds;

    /**
     * Constructs a StreamingSinkOperatorCoordinator.
     *
     * @param conf    The config options
     * @param context The coordinator context
     */
    public AbstractWriteOperatorCoordinator(
            String operatorName,
            Configuration conf,
            OperatorCoordinator.Context context) {
        this.operatorName = operatorName;
        this.conf = conf;
        this.context = context;
        this.parallelism = context.currentParallelism();
        this.started = false;
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting odps write operator {}.", operatorName);

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        this.eventBuffer = new SinkTaskEvent[this.parallelism];
        this.gateways = new OperatorCoordinator.SubtaskGateway[this.parallelism];
        // start the executor
        this.executor = NonThrownExecutor.builder(LOG)
                .exceptionHook((errMsg, t) -> this.context.failJob(new FlinkOdpsException(errMsg, t)))
                .threadNum(1)
                .waitForTasksFinish(true).build();
        initEnv();
        this.started = true;
        this.allSubtaskReady = false;
        this.lock = new Object();
        this.completedTaskIds = new HashSet<>();

        VersionUtils.logVersions();
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.close();
        }
        this.eventBuffer = null;
        LOG.info("Odps write coordinator {} closed.", operatorName);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception {
        executor.execute(
                () -> {
                    try {
                        result.complete(new byte[0]);
                    } catch (Throwable throwable) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(throwable);
                        // when a checkpoint fails, throws directly.
                        result.completeExceptionally(
                                new CompletionException(
                                        String.format("Failed to checkpoint Session %s for source %s",
                                                checkpointId, this.getClass().getSimpleName()), throwable));
                    }
                }, "taking checkpoint %d", checkpointId
        );
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no operation
        LOG.info("Notify the checkpoint {} complete", checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        LOG.info("Reset to the checkpoint {}", checkpointId);
        attemptGeneration++;
        if (started) {
            executor.execute(() -> {
                completedTaskIds.clear();
                Arrays.fill(eventBuffer, null);
            }, "reset sink task state for checkpoint " + checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // no operation
        LOG.info("Notify the checkpoint {} aborted", checkpointId);
    }

    // For flink 1.15
    // public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
    @Override
    public void handleEventFromOperator(int i, int attemptNumber, OperatorEvent operatorEvent) {
        Preconditions.checkState(operatorEvent instanceof SinkTaskEvent,
                "The coordinator can only handle SinkTaskEvent");
        SinkTaskEvent event = (SinkTaskEvent) operatorEvent;
        final long generation = attemptGeneration;
        executor.execute(() -> {
            if (generation != attemptGeneration) {
                return;
            }
            if (event.isEndInput()) {
                handleEndInputEvent(event);
            } else if (event.isBootstrap()) {
                handleBootstrapEvent(event);
            } else if (event.isCheckpointSuccess()) {
                handleCheckpointSuccessEvent(event);
            } else {
                handleCommitEvent(event);
            }
        }, "handle sink event for task " + i);

    }

    public void subtaskFailed(int i, @Nullable Throwable throwable) {
        // reset the event
        this.eventBuffer[i] = null;
        LOG.warn("Reset the event for task [" + i + "]", throwable);
    }

    @Override
    public void subtaskReset(int i, long l) {
        // reset the event
        this.eventBuffer[i] = null;
        LOG.warn("Subtask reset for task [" + i + "], checkpoint [" + l + "]");
    }

    public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
        this.gateways[i] = subtaskGateway;
    }

    @Override
    public void executionAttemptReady(int i, int attemptNumber, SubtaskGateway subtaskGateway) {
        LOG.info("Execution attempt ready for task [" + i + "]");
        subtaskReady(i, subtaskGateway);
        if (!allSubtaskReady
                && Arrays.stream(gateways).allMatch(Objects::nonNull)) {
            allSubtaskReady = true;
            synchronized (lock) {
                lock.notifyAll();
            }
        }
    }

    @Override
    public void executionAttemptFailed(int i, int attemptNumber, Throwable throwable) {
        LOG.warn("Execution attempt failed for task [" + i + "]", throwable);
        subtaskReady(i, null);
        subtaskFailed(i, throwable);
        allSubtaskReady = false;
    }

    abstract protected void initEnv();

    abstract protected void handleBootstrapEvent(SinkTaskEvent event) throws IOException;

    abstract protected void handleCommitEvent(SinkTaskEvent event) throws IOException;

    abstract protected void handleEndInputEvent(SinkTaskEvent event) throws IOException;

    protected void handleCheckpointSuccessEvent(SinkTaskEvent event) throws IOException {
        // Do nothing
    }

    protected void sendAllTaskAckEvents(long checkpointId,
                                      String partition,
                                      String sessionId,
                                      boolean committed,
                                      boolean endInput) {
        final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                .checkpointId(checkpointId)
                .committed(committed)
                .endInput(endInput)
                .partition(partition)
                .sessionId(sessionId).build();
        CompletableFuture<?>[] futures = Arrays.stream(this.gateways).filter(Objects::nonNull)
                .map(gw -> gw.sendEvent(taskAckEvent))
                .toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(futures).whenComplete((resp, error) -> {
            if (!sendToFinishedTasks(error)) {
                throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
            }
        });
    }

    protected void sendAllTaskAckEvents(long checkpointId,
                                        String partition,
                                        String sessionId,
                                        boolean committed,
                                        boolean endInput,
                                        boolean bootstrap,
                                        boolean checkpoint) {
        final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                .checkpointId(checkpointId)
                .bootstrap(bootstrap)
                .committed(committed)
                .endInput(endInput)
                .partition(partition)
                .checkpoint(checkpoint)
                .sessionId(sessionId).build();
        CompletableFuture<?>[] futures = Arrays.stream(this.gateways).filter(Objects::nonNull)
                .map(gw -> gw.sendEvent(taskAckEvent))
                .toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(futures).whenComplete((resp, error) -> {
            if (!sendToFinishedTasks(error)) {
                throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
            }
        });
    }

    protected void sendSingleTaskAckEvents(long checkpointId,
                                           int taskId,
                                           String partition,
                                           String sessionId,
                                           boolean committed) {
        final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                .checkpointId(checkpointId)
                .committed(committed)
                .partition(partition)
                .sessionId(sessionId).build();
        if (gateways[taskId] == null) {
            LOG.warn("Send event {} failed, Gateway {} is null: ", taskAckEvent, taskId);
        } else {
            gateways[taskId].sendEvent(taskAckEvent).whenComplete((resp, error) -> {
                if (!sendToFinishedTasks(error)) {
                    throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
                }
            });
        }
    }

    protected boolean sendToFinishedTasks(Throwable throwable) {
        if (throwable == null) {
            return true;
        }
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof TaskNotRunningException) {
                return true;
            }
        }
        return false;
    }

    private boolean allEventsReceived() {
        return Arrays.stream(eventBuffer)
                .allMatch(event -> event != null);
    }

}
