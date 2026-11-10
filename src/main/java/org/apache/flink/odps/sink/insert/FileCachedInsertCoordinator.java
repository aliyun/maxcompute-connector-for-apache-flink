/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.sink.insert;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.utils.*;
import org.apache.flink.odps.sink.common.OdpsWriteCoordinator;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.flink.odps.table.OdpsOptions.*;

public class FileCachedInsertCoordinator extends OdpsWriteCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(FileCachedInsertCoordinator.class);

    protected boolean isGroupPartition;

    protected int executeThreadNum;
    protected boolean useStreamingTunnel;

    protected transient Map<String, AsyncExecuteThread> executeThreadMap;
    protected transient AsyncExecuteThread[] executeThreads;
    protected transient ScheduledExecutorService bootstrapCheckExecutor;
    protected transient ScheduledExecutorService partitionCheckExecutor;

    protected transient Map<String, List<AsyncExecuteThread.BootstrapRequest>> pendingBootstrapRequest;

    // partition -> session id -> session info
    protected transient Map<String, Map<String, ActiveSessionInfo>> currentActiveSessions;

    protected transient Map<Long, Boolean[]> taskStatus;

    protected transient Map<Long, Set<Integer>> taskCheckpointStatus;

    protected long latestCheckpoint;

    private final AtomicLong totalCommit = new AtomicLong(0L);
    private final AtomicLong totalCommitTimes = new AtomicLong(0L);
    private final AtomicLong totalRowCount = new AtomicLong(0L);
    private transient Map<Long, Long> checkpointTimes;
    private transient Map<Long, AtomicLong> commitRowCount;

    protected transient Set<Integer> pendingCompletedTaskIds;
    private final long[] checkpointAcknowledged;

    public FileCachedInsertCoordinator(String operatorName,
                                       Configuration config,
                                       Context context,
                                       OdpsConf odpsConf,
                                       TableIdentifier identifier,
                                       String partition,
                                       DataSchema dataSchema,
                                       boolean isDynamicPartition,
                                       boolean isGroupPartition,
                                       OdpsWriteOptions writeOptions) {
        super(operatorName, config, context, odpsConf, identifier, partition,
                dataSchema, isDynamicPartition, writeOptions);
        this.isGroupPartition = isGroupPartition;
        this.executeThreadNum = config.getInteger(INSERT_COMMIT_THREAD_NUM);
        this.useStreamingTunnel = config.getBoolean(INSERT_STREAMING_TUNNEL_ENABLE);
        this.latestCheckpoint = -1L;
        this.checkpointAcknowledged = new long[parallelism];
        Arrays.fill(checkpointAcknowledged, Long.MIN_VALUE);
    }

    @Override
    protected void initEnv() {
        this.settings = TableUtils.getEnvironmentSettings(getOdps(), odpsConf.getTunnelEndpoint(), conf);
        this.executeThreads = new AsyncExecuteThread[executeThreadNum];
        this.executeThreadMap = new HashMap<>();
        for (int i = 0; i < executeThreadNum; i++) {
            final AsyncExecuteThread t = new AsyncExecuteThread();
            this.executeThreads[i] = t;
            t.setName("OdpsBucket commit thread #" + (i + 1));
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((t1, e) -> LOG.error(
                    "IO Thread '"
                            + t1.getName()
                            + "' terminated due to an exception. Shutting down I/O Manager.",
                    e));
            t.start();
        }

        this.currentActiveSessions = new HashMap<>();
        this.pendingBootstrapRequest = new HashMap<>();
        if (!isGroupPartition) {
            this.bootstrapCheckExecutor = Executors.newSingleThreadScheduledExecutor();
            this.bootstrapCheckExecutor.scheduleAtFixedRate(() ->
                            executor.execute(() -> handleBootstrapCheck(1),
                                    "handle partition create event"),
                    5000, 15000, TimeUnit.MILLISECONDS);

            if (isPartitioned) {
                this.partitionCheckExecutor = Executors.newSingleThreadScheduledExecutor();
                this.partitionCheckExecutor.scheduleAtFixedRate(() ->
                                executor.execute(this::handlePartitionCheck,
                                        "handle partition check event"),
                        0, metaCacheExpireTime - 30, TimeUnit.SECONDS);
            }
        }

        this.taskStatus = new HashMap<>();
        this.taskCheckpointStatus = new HashMap<>();
        this.checkpointTimes = new HashMap<>();
        this.commitRowCount = new ConcurrentHashMap<>();
        this.pendingCompletedTaskIds = new HashSet<>();
    }

    @Override
    protected void handleBootstrapEvent(SinkTaskEvent event) {
        if (!isGroupPartition) {
            final long generation = attemptGeneration;
            final String partition = event.getPartitionSpec();

            final AsyncExecuteThread.BootstrapRequest bootstrapRequest = new AsyncExecuteThread.BootstrapRequest() {

                private String sessionId = null;
                private boolean isCreatePartition = false;

                @Override
                public void execute() throws IOException {
                    sessionId = initDynamicWriteSession(partition);
                }

                @Override
                public void requestDone(IOException ex, String sessionId) {
                    if (generation != attemptGeneration) {
                        return;
                    }
                    if (ex != null) {
                        if (ex.getMessage().contains("Request has been cancelled")) {
                            // ignore thread shutdown
                            return;
                        }
                        // if bootstrap failed, should fail job
                        context.failJob(new FlinkOdpsException("Create session error", ex));
                    }

                    if (!allSubtaskReady) {
                        LOG.info("Waiting for subtask ready");
                        synchronized (lock) {
                            try {
                                while (!allSubtaskReady) {
                                    lock.wait();
                                }
                                // wait for INITIALIZING to RUNNING
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                throw new FlinkOdpsException("Waiting for subtask ready error", e);
                            }
                        }
                    }

                    executor.execute(() -> {
                        if (generation != attemptGeneration) {
                            return;
                        }
                        sendAckEventsWithoutCompletedTask(event.getCheckpointID(), partition,
                                sessionId, false, false, true, false, false);
                    }, "Create session response");
                }

                @Override
                public boolean isReUpload() {
                    return false;
                }

                @Override
                public long getCheckpointId() {
                    return event.getCheckpointID();
                }

                @Override
                public String getPartition() {
                    return partition;
                }

                @Override
                public String getSessionId() {
                    return sessionId;
                }

                @Override
                public boolean isCreatePartition() {
                    return isCreatePartition;
                }

                @Override
                public void createPartition(boolean createPartition) {
                    isCreatePartition = createPartition;
                }
            };

            boolean pendingRequest = false;

            if (isPartitioned
                    && !OdpsUtils.partitionExist(getTableMetaProvider(), tableIdentifier, partition, false)) {
                bootstrapRequest.createPartition(true);
                pendingRequest = true;
            }

            if (event.getCheckpointID() != Long.MAX_VALUE && event.getCheckpointID() > latestCheckpoint) {
                pendingRequest = true;
            }

            addBootstrapRequest(partition, bootstrapRequest, pendingRequest);

        } else {
            throw new UnsupportedOperationException(
                    "Unsupported bootstrap event for non group partition");
        }
    }

    @Override
    protected void handleCommitEvent(SinkTaskEvent event) {
        if (event.isWriteSuccess()) {
            List<WriterStatus> statusList = event.getWriterStatuses();

            for (WriterStatus status: statusList) {

                final String partition = status.getPartitionSpec();
                final String sessionId = status.getSessionId();
                final long checkpointId = event.getCheckpointID();
                final ActiveSessionInfo activeSessionInfo =
                        addPendingCommitSession(partition, sessionId, event.getTaskID(),
                                status.getWriterMessage(), status.getTotalRecords(), checkpointId);

                commitSessionIfReady(partition, sessionId, activeSessionInfo, checkpointId, event.getTaskID());
            }
        } else {
            // if write task failed, should fail job
            LOG.error("Write task failed! Event: {}", event);
            throw new FlinkOdpsException("Write task failed!");
        }
    }

    private void commitSessionIfReady(final String partition, final String sessionId,
                              final ActiveSessionInfo activeSessionInfo,
                              final long checkpointId, final int taskId) {
        boolean commit = false;
        if (!isGroupPartition) {
            Set<Integer> allCompleteTask = new HashSet<>(completedTaskIds);
            allCompleteTask.addAll(activeSessionInfo.getTaskIds());
            if (checkpointId == Long.MAX_VALUE) {
                // Bounded tasks have no later checkpoint-complete callback.
                // Ended empty subtasks must not hold a shared session open.
                allCompleteTask.addAll(pendingCompletedTaskIds);
            }

            if (allCompleteTask.size() == this.parallelism) {
                Map<String, ActiveSessionInfo> currentActiveSession =
                        currentActiveSessions.get(partition);
                currentActiveSession.remove(sessionId);

                commit = true;
            } else {
                LOG.debug("Wait commit: task Ids {}, session {}", allCompleteTask, sessionId);
            }
        } else {
            commit = true;
        }

        if (commit) {

            final AsyncExecuteThread.CommitRequest commitRequest = new AsyncExecuteThread.CommitRequest() {

                @Override
                public void execute() throws IOException {
                    if (commitRowCount.containsKey(checkpointId)) {
                        commitRowCount.get(checkpointId).addAndGet(activeSessionInfo.getTotalRowCount());
                    }

                    commitWriteSession(activeSessionInfo.getCommitMessageList(), partition, sessionId,
                            activeSessionInfo.getTotalRowCount());
                }

                @Override
                public String getSessionId() {
                    return sessionId;
                }

                @Override
                public void requestDone(IOException ex) {
                    if (ex != null) {
                        if (ex.getMessage().contains("Request has been cancelled")) {
                            // ignore thread shutdown
                            return;
                        }
                        LOG.error("Commit session error", ex);

                        if (isGroupPartition) {
                            executor.execute(() -> {
                                sendSingleTaskAckEvents(checkpointId, taskId, partition,
                                        sessionId, false);
                            }, "Commit session response");
                        } else {
                            executor.execute(() -> {
                                handleReUploadBootStrap(partition, checkpointId);
                            }, "Re upload session boot strap");
                        }
                    } else {
                        if (isGroupPartition) {
                            executor.execute(() -> {
                                sendSingleTaskAckEvents(checkpointId, taskId, partition,
                                        sessionId, true);
                            }, "Commit session response");
                        } else {
                            executor.execute(() -> {
                                sendAllTaskAckEvents(checkpointId, partition,
                                        sessionId, true, false, false, false);
                            }, "Commit session response");
                        }
                    }
                }

                @Override
                public long getCheckpointId() {
                    return checkpointId;
                }

                @Override
                public String getPartition() {
                    return partition;
                }
            };

            addExecuteRequest(partition, commitRequest);
        }
    }

    @Override
    protected void handleEndInputEvent(SinkTaskEvent event) {
        LOG.info("Handle end input event for task[{}], checkpoint {}", event.getTaskID(), event.getCheckpointID());
        if (event.isInputCommitted()) {
            completedTaskIds.add(event.getTaskID());
            pendingCompletedTaskIds.remove(event.getTaskID());
        } else {
            pendingCompletedTaskIds.add(event.getTaskID());
        }
        if ((event.isInputCommitted() || event.getCheckpointID() == Long.MAX_VALUE) && !isGroupPartition) {
            // Re-evaluate sessions when an empty task ends after a data task's commit event.
            for (Map.Entry<String, Map<String, ActiveSessionInfo>> partition : currentActiveSessions.entrySet()) {
                for (Map.Entry<String, ActiveSessionInfo> session : new HashMap<>(partition.getValue()).entrySet()) {
                    int writerTask = session.getValue().getTaskIds().iterator().next();
                    commitSessionIfReady(partition.getKey(), session.getKey(), session.getValue(),
                            session.getValue().getCheckpointId(), writerTask);
                }
            }
        }
        if (event.isInputCommitted()) {
            // Active writers may already be waiting for this finished participant.
            for (long checkpoint : new ArrayList<>(taskCheckpointStatus.keySet())) {
                Set<Integer> participants = taskCheckpointStatus.get(checkpoint);
                if (participants != null && !participants.isEmpty()) {
                    triggerCheckpointSuccess(checkpoint, participants.iterator().next());
                }
            }
            return;
        }
        executor.execute(() -> {
            final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                    .checkpointId(event.getCheckpointID())
                    .endInput(true).build();
            if (gateways[event.getTaskID()] == null) {
                LOG.warn("Send event {} failed, Gateway {} is null: ", taskAckEvent, event.getTaskID());
            } else {
                gateways[event.getTaskID()].sendEvent(taskAckEvent).whenComplete((resp, error) -> {
                    if (!sendToFinishedTasks(error)) {
                        throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
                    }
                });
            }
        }, "EndInputEvent response");
    }

    private void triggerCheckpointSuccess(long checkpointId, int taskId) {
        Set<Integer> taskIds =
                taskCheckpointStatus.computeIfAbsent(checkpointId, key -> new HashSet<>());
        taskIds.add(taskId);

        Set<Integer> readyTasks = new HashSet<>(taskIds);
        readyTasks.addAll(completedTaskIds);
        for (int task = 0; task < parallelism; task++) {
            if (checkpointAcknowledged[task] >= checkpointId) readyTasks.add(task);
        }
        if (readyTasks.size() == this.parallelism) {
            // Queue the SDK snapshot marker once; late participants only need ACKs.
            if (checkpointId > latestCheckpoint) {
                for (AsyncExecuteThread tr : executeThreads) {
                    final AsyncExecuteThread.SnapshotRequest snapshotRequest =
                            new AsyncExecuteThread.SnapshotRequest() {

                                @Override
                                public boolean isStarted() {
                                    return false;
                                }

                                @Override
                                public void execute() throws IOException {
                                    // Do nothing
                                }

                                @Override
                                public void requestDone(IOException ioex) {}

                                @Override
                                public long getCheckpointId() {
                                    return checkpointId;
                                }
                            };
                    tr.addRequest(snapshotRequest);
                }
                latestCheckpoint = checkpointId;
            }
            // Flink can report checkpoint completion before the local writer finishes.
            // Only actual success-event participants need this ACK, even if end-input
            // bookkeeping already calls them complete. Each task gets it once.
            Set<Integer> targets = new HashSet<>();
            for (int task : taskIds) {
                if (checkpointAcknowledged[task] < checkpointId) {
                    targets.add(task);
                    checkpointAcknowledged[task] = checkpointId;
                }
            }
            taskCheckpointStatus.remove(checkpointId);
            final long generation = attemptGeneration;
            executor.execute(
                    () -> {
                        if (generation != attemptGeneration) return;
                        TaskAckEvent ack =
                                TaskAckEvent.builder()
                                        .checkpointId(checkpointId)
                                        .checkpoint(true)
                                        .build();
                        CompletableFuture<?>[] futures =
                                targets.stream()
                                        .map(task -> gateways[task])
                                        .filter(Objects::nonNull)
                                        .map(gateway -> gateway.sendEvent(ack))
                                        .toArray(CompletableFuture<?>[]::new);
                        CompletableFuture.allOf(futures)
                                .whenComplete(
                                        (response, error) -> {
                                            if (!sendToFinishedTasks(error))
                                                context.failJob(
                                                        new FlinkOdpsException(
                                                                "Error sending checkpoint"
                                                                    + " acknowledgment",
                                                                error));
                                        });
                    },
                    "Checkpoint response");

            LOG.info("Set latestCheckpoint {}", checkpointId);
        } else {
            LOG.info(
                    "Wait checkpoint complete event : task Ids {}, checkpoint {}",
                    taskIds,
                    checkpointId);
        }
    }

    @Override
    protected void handleCheckpointSuccessEvent(SinkTaskEvent event) throws IOException {
        if (event.isCheckpointSuccess()) {
            triggerCheckpointSuccess(event.getCheckpointID(), event.getTaskID());
        }
    }

    protected void handleReUploadBootStrap(String partition, long checkpointId) {

        final AsyncExecuteThread.BootstrapRequest bootstrapRequest = new AsyncExecuteThread.BootstrapRequest() {

            private String sessionId = null;
            private boolean isCreatePartition = false;

            @Override
            public void execute() throws IOException {
                sessionId = initDynamicWriteSession(partition);
            }

            @Override
            public void requestDone(IOException ex, String sessionId) {
                if (ex != null) {
                    if (ex.getMessage().contains("Request has been cancelled")) {
                        // ignore thread shutdown
                        return;
                    }
                    // if bootstrap failed, should fail job
                    context.failJob(new FlinkOdpsException("ReUpload create session error", ex));
                }
                executor.execute(() -> {
                    sendAckEventsWithoutCompletedTask(checkpointId, partition,
                            sessionId, false, false, false, false, true);
                }, "Create session response");
            }

            @Override
            public boolean isReUpload() {
                return true;
            }

            @Override
            public long getCheckpointId() {
                return checkpointId;
            }

            @Override
            public String getPartition() {
                return partition;
            }

            @Override
            public String getSessionId() {
                return sessionId;
            }

            @Override
            public boolean isCreatePartition() {
                return isCreatePartition;
            }

            @Override
            public void createPartition(boolean createPartition) {
                isCreatePartition = createPartition;
            }
        };

        addBootstrapRequest(partition, bootstrapRequest,
                isPartitioned &&
                        !OdpsUtils.partitionExist(getTableMetaProvider(), tableIdentifier, partition, true));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        super.notifyCheckpointComplete(checkpointId);

        // TODO: should save completed taskIds
        final long generation = attemptGeneration;
        executor.execute(
                () -> {
                    if (generation != attemptGeneration) return;
                    completedTaskIds.addAll(pendingCompletedTaskIds);
                    pendingCompletedTaskIds.clear();

                    taskStatus.remove(checkpointId);
                    // Local success events can still be in flight after Flink's completion
                    // callback.
                    // Retire them only when their checkpoint ACKs have been scheduled.

                    if (checkpointTimes.containsKey(checkpointId)) {
                        totalCommit.incrementAndGet();

                        long timesTaken =
                                NANOSECONDS.toMillis(
                                        System.nanoTime() - checkpointTimes.get(checkpointId));

                        totalCommitTimes.addAndGet(timesTaken);
                        totalRowCount.addAndGet(commitRowCount.get(checkpointId).get());

                        LOG.info(
                                "Commit times: {}, time taken ms avg: {}, total row count: {}",
                                totalCommit.get(),
                                ((double) totalCommitTimes.get() / (double) totalCommit.get()),
                                totalRowCount.get());

                        LOG.info(
                                "Checkpoint: {}, time taken ms avg: {}, row count: {}",
                                checkpointId,
                                timesTaken,
                                commitRowCount.get(checkpointId).get());
                    }
                },
                "checkpoint completion " + checkpointId);
    }

    @Override
    public void subtaskReset(int i, long checkpointId) {
        super.subtaskReset(i, checkpointId);

        Boolean[] resetStatus = taskStatus.computeIfAbsent(checkpointId,
                key -> new Boolean[this.parallelism]);
        resetStatus[i] = true;

        if (Arrays.stream(resetStatus).allMatch(event -> event != null && event.equals(true))) {
            clearState(checkpointId);
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attempt, Throwable reason) {
        super.executionAttemptFailed(subtask, attempt, reason);
        if (!isGroupPartition && reason != null) {
            // A null reason is a cancellation notification during another failover.
            // Escalating it would consume an additional restart.
            // Shared sessions and checkpoint acknowledgements involve every subtask.
            // A regional retry cannot reset the other writers' protocol state.
            context.failJob(new FlinkOdpsException(
                    "File-cached shared session requires global recovery after writer failure", reason));
        }
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        super.resetToCheckpoint(checkpointId, checkpointData);

        clearState(checkpointId);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception {
        checkpointTimes.put(checkpointId, System.nanoTime());
        commitRowCount.put(checkpointId, new AtomicLong(0L));

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
    public void close() throws Exception {
        List<AutoCloseable> closeables = new ArrayList<>(executeThreadNum);

        for (AsyncExecuteThread tr : executeThreads) {
            closeables.add(() -> {
                try {
                    tr.shutdown();
                } catch (Throwable t) {
                    throw new IOException("Error while shutting down IO Manager writer thread.", t);
                }
            });
        }

        IOUtils.closeAll(closeables);

        if (partitionCheckExecutor != null) {
            partitionCheckExecutor.shutdown();
        }

        if (bootstrapCheckExecutor != null) {
            bootstrapCheckExecutor.shutdown();
        }

        super.close();
    }

    protected void handleBootstrapCheck(int limit) {
        List<String> submitPartitions = new ArrayList<>();
        pendingBootstrapRequest.forEach((part, requests) -> {
            if (requests.stream().anyMatch(AsyncExecuteThread.BootstrapRequest::isCreatePartition)) {
                submitPartitions.add(part);
            }
        });

        if (submitPartitions.size() >= limit) {
            OdpsUtils.createPartitions(getOdps(), tableIdentifier, submitPartitions.toArray(new String[0]));
            submitPartitions.forEach(part ->
                    pendingBootstrapRequest.get(part).forEach(req -> req.createPartition(false)));
        }

        Iterator<Map.Entry<String, List<AsyncExecuteThread.BootstrapRequest>>> iterator
                = pendingBootstrapRequest.entrySet().iterator();
        Map.Entry<String, List<AsyncExecuteThread.BootstrapRequest>> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();

            List<AsyncExecuteThread.BootstrapRequest> requests = entry.getValue();
            Iterator<AsyncExecuteThread.BootstrapRequest> requestIterator = requests.iterator();
            while (requestIterator.hasNext()) {
                AsyncExecuteThread.BootstrapRequest req = requestIterator.next();
                if (req.getCheckpointId() <= latestCheckpoint
                        && !req.isCreatePartition()) {
                    addExecuteRequest(req.getPartition(), req);
                    requestIterator.remove();
                }
            }
            if (requests.isEmpty()) {
                iterator.remove();
            }
        }
    }

    protected void handlePartitionCheck() {
        getTableMetaProvider()
                .loadPartitions(tableIdentifier.getProject(), tableIdentifier.getSchema(), tableIdentifier.getTable());
    }

    protected void addBootstrapRequest(String partition,
                                       AsyncExecuteThread.BootstrapRequest bootstrapRequest,
                                       boolean pendingRequest) {
        if (pendingRequest) {
            if (pendingBootstrapRequest.containsKey(partition)) {
                pendingBootstrapRequest.get(partition).add(bootstrapRequest);
            } else {
                List<AsyncExecuteThread.BootstrapRequest> requests = new LinkedList<>();
                requests.add(bootstrapRequest);

                pendingBootstrapRequest.put(partition, requests);
            }
            handleBootstrapCheck(64);
        } else {
            addExecuteRequest(partition, bootstrapRequest);
        }
    }

    protected void addExecuteRequest(String partition, AsyncExecuteThread.ExecuteRequest request) {
        final AsyncExecuteThread executeThread;
        if (executeThreadMap.containsKey(partition)) {
            executeThread = executeThreadMap.get(partition);
        } else {
            executeThread =
                    Arrays.stream(executeThreads).min(Comparator.comparingInt(AsyncExecuteThread::queueSize)).get();
            executeThreadMap.put(partition, executeThread);
        }
        executeThread.addRequest(request);
    }

    protected void commitWriteSession(List<WriterCommitMessage> commitMessages, String partition, String sessionId, long rowCount) throws IOException {
        TableBatchWriteSession session = OdpsUtils.getWriteSession(tableIdentifier, partition, sessionId, settings);
        try {
            if (session.getStatus().equals(SessionStatus.COMMITTED)) {
                LOG.info("Session has committed {}, partition {}", sessionId, partition);
                return;
            }

            LOG.info("Start to commit session {}, partition {}, row count {}",
                    session.getId(), partition, rowCount);

            long startTime = System.nanoTime();
            session.commit(commitMessages.toArray(new WriterCommitMessage[0]));

            LOG.info("Commit session {}, time taken ms: {}", sessionId,
                    NANOSECONDS.toMillis(System.nanoTime() - startTime));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected String initDynamicWriteSession(String partitionSpec) throws IOException {
        TableBatchWriteSession batchWriteSession =
                OdpsUtils.getOrCreateWriteSession(tableIdentifier, partitionSpec, null, settings, useStreamingTunnel);
        return batchWriteSession.getId();
    }

    private ActiveSessionInfo addPendingCommitSession(final String partition,
                                                      final String sessionId,
                                                      final int taskId,
                                                      final WriterCommitMessage commitMessage,
                                                      final long rowCount,
                                                      final long checkpointId) {
        Map<String, ActiveSessionInfo> partitionCreatedSession =
                currentActiveSessions.computeIfAbsent(partition, key -> new HashMap<>());

        ActiveSessionInfo sessionInfo =
                partitionCreatedSession.computeIfAbsent(sessionId, key -> new ActiveSessionInfo(checkpointId));

        sessionInfo.addCommitMessage(commitMessage);
        sessionInfo.addCompleteTask(taskId);
        sessionInfo.addRowCount(rowCount);

        return sessionInfo;
    }

    private void clearState(long checkpointId) {
        if (started) {
            executor.execute(() -> {
                pendingCompletedTaskIds.clear();
                currentActiveSessions.clear();
                pendingBootstrapRequest.clear();
                taskStatus.clear();
                taskCheckpointStatus.clear();
                Arrays.fill(checkpointAcknowledged, Long.MIN_VALUE);

                CompletableFuture<?>[] resets = Arrays.stream(executeThreads)
                        .map(ct -> ct.reset(checkpointId)).toArray(CompletableFuture<?>[]::new);
                CompletableFuture.allOf(resets).get(30, TimeUnit.SECONDS);
                latestCheckpoint = checkpointId;
            }, "Reset to checkpoint " + checkpointId);
        } else {
            latestCheckpoint = checkpointId;
        }
    }

    @Override
    protected void initStaticWriteSession(int taskId) throws IOException {
        throw new UnsupportedOperationException("Unsupported init static write session");
    }

    @Override
    protected void initDynamicWriteSession(int taskId, String partitionSpec) throws IOException {
        throw new UnsupportedOperationException("Unsupported init dynamic write session");
    }

    @Override
    protected boolean commitWriteSession() throws IOException {
        throw new UnsupportedOperationException("Unsupported commit write session");
    }

    protected void sendAckEventsWithoutCompletedTask(long checkpointId,
                                                     String partition,
                                                     String sessionId,
                                                     boolean committed,
                                                     boolean endInput,
                                                     boolean bootstrap,
                                                     boolean checkpoint,
                                                     boolean withPendingTask) {
        final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                .checkpointId(checkpointId)
                .bootstrap(bootstrap)
                .committed(committed)
                .endInput(endInput)
                .partition(partition)
                .checkpoint(checkpoint)
                .sessionId(sessionId).build();
        final IntStream targetTasks;
        if (withPendingTask) {
            targetTasks = IntStream.range(0, this.parallelism)
                    .filter(i -> !completedTaskIds.contains(i));
        } else {
            targetTasks = IntStream.range(0, this.parallelism)
                    .filter(i -> !completedTaskIds.contains(i)
                            && !pendingCompletedTaskIds.contains(i));
        }
        CompletableFuture<?>[] futures = targetTasks.mapToObj(i -> gateways[i])
                .filter(Objects::nonNull)
                .map(gw -> gw.sendEvent(taskAckEvent))
                .toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(futures).whenComplete((resp, error) -> {
            if (!sendToFinishedTasks(error)) {
                throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
            }
        });
    }
}
