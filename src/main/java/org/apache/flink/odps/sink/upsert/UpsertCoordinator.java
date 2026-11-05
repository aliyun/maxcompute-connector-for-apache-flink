/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.common.OdpsWriteCoordinator;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.sink.utils.ExecuteThread;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.flink.odps.table.OdpsOptions.*;

public class UpsertCoordinator extends OdpsWriteCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(UpsertCoordinator.class);

    protected final int dynamicPartitionLimit;
    protected boolean enableMajorCompact;
    protected final boolean isGroupPartition;
    protected final boolean supportsGrouping;

    protected transient Map<String, String> tableUpsertSessionIdMap;
    protected transient Map<String, Long> upsertCommitTimes;
    protected transient Map<String, ExecuteThread> compactThreadMap;
    protected transient ExecuteThread[] executeThreads;
    protected transient LinkedBlockingQueue<ExecuteThread.ExecuteRequest> requestQueue;

    protected transient int compactThreshold = 1;

    private final AtomicLong totalCommit = new AtomicLong(0L);
    private final AtomicLong totalCommitTimes = new AtomicLong(0L);
    private final AtomicLong totalRowCount = new AtomicLong(0L);
    private final AtomicLong totalCompact = new AtomicLong(0L);
    private final AtomicLong totalCompactTimes = new AtomicLong(0L);

    public UpsertCoordinator(
            String operatorName,
            Configuration config,
            OperatorCoordinator.Context context,
            OdpsConf odpsConf,
            TableIdentifier identifier,
            String partition,
            DataSchema dataSchema,
            boolean isDynamicPartition,
            boolean isGroupPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions) {
        super(operatorName, config, context, odpsConf, identifier,
                partition, dataSchema, isDynamicPartition, writeOptions);
        this.isGroupPartition = isGroupPartition;
        this.supportsGrouping = supportsGrouping;
        this.dynamicPartitionLimit = config.get(SINK_DYNAMIC_PARTITION_LIMIT);
    }

    @Override
    protected void initEnv() {
        this.settings = TableUtils.getEnvironmentSettings(getOdps(), odpsConf.getTunnelEndpoint(), conf);
        this.tableUpsertSessionIdMap = new ConcurrentHashMap<>();
        this.upsertCommitTimes = new ConcurrentHashMap<>();

        int threadNum = conf.getInteger(UPSERT_COMMIT_THREAD_NUM);
        this.enableMajorCompact = conf.getBoolean(UPSERT_MAJOR_COMPACT_ENABLE);
        this.compactThreshold = conf.getInteger(UPSERT_MAJOR_COMPACT_MIN_COMMITS);

        this.requestQueue = new LinkedBlockingQueue<>();
        this.executeThreads = new ExecuteThread[threadNum];
        this.compactThreadMap = new ConcurrentHashMap<>();
        for (int i = 0; i < threadNum; i++) {
            final ExecuteThread t = new ExecuteThread(requestQueue);
            this.executeThreads[i] = t;
            t.setName("OdpsBucket commit thread #" + (i + 1));
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((t12, e) -> LOG.error(
                    "IO Thread '"
                            + t12.getName()
                            + "' terminated due to an exception. Shutting down I/O Manager.",
                    e));
            t.start();
        }
    }

    @Override
    public void close() throws Exception {
        List<AutoCloseable> closeables = new ArrayList<>(executeThreads.length);
        for (ExecuteThread et : executeThreads) {
            closeables.add(() -> {
                try {
                    et.shutdown();
                } catch (Throwable t) {
                    throw new IOException("Error while shutting down commit thread.", t);
                }
            });
        }
        IOUtils.closeAll(closeables);

        super.close();
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        super.notifyCheckpointAborted(checkpointId);
        clearState();
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        super.resetToCheckpoint(checkpointId, checkpointData);
        if (started) {
            executor.execute(this::clearState, "reset upsert sessions");
        }
    }

    @Override
    protected void initStaticWriteSession(int taskId) throws IOException {
        executor.execute(() -> {
            String sessionId;
            if (tableUpsertSessionIdMap.containsKey(staticPartition)) {
                sessionId = tableUpsertSessionIdMap.get(staticPartition);
            } else {
                TableUpsertSessionImpl session = null;
                try {
                    session = OdpsUtils.getOrCreateUpsertSession(tableIdentifier,
                            staticPartition, null, settings);
                    sessionId = session.getId();
                } finally {
                    if (session != null) {
                        session.close();
                    }
                }
                tableUpsertSessionIdMap.put(staticPartition, sessionId);
            }
            sendSingleTaskAckEvents(-1, taskId,  staticPartition, sessionId, false);
        }, "initialize dynamic partition session");
    }

    @Override
    protected void initDynamicWriteSession(int taskId, String partitionSpec) throws IOException {
        executor.execute(() -> {
            String sessionId;
            if (tableUpsertSessionIdMap.containsKey(partitionSpec)) {
                sessionId = tableUpsertSessionIdMap.get(partitionSpec);
            } else {
                if (tableUpsertSessionIdMap.size() >= dynamicPartitionLimit) {
                    throw new FlinkOdpsException("Dynamic partition limit: " + dynamicPartitionLimit);
                }

                OdpsUtils.createPartitionIfNeeded(getTableMetaProvider(),
                        tableIdentifier, partitionSpec);

                TableUpsertSessionImpl session = null;
                try {
                    session = OdpsUtils.getOrCreateUpsertSession(tableIdentifier,
                            partitionSpec, null, settings);
                    sessionId = session.getId();
                } finally {
                    if (session != null) {
                        session.close();
                    }
                }

                tableUpsertSessionIdMap.put(partitionSpec, sessionId);
            }
            sendSingleTaskAckEvents(-1, taskId, partitionSpec, sessionId, false);
        }, "initialize dynamic partition session");
    }

    @Override
    protected boolean commitWriteSession() throws IOException {
        final Map<String, String> sessionIdsMap = new ConcurrentHashMap<>();
        final Map<String, Long> commitRowCount = new HashMap<>();
        final List<WriterStatus> writerStatuses = Arrays.stream(eventBuffer)
                .filter(Objects::nonNull)
                .map(SinkTaskEvent::getWriterStatuses)
                .flatMap(Collection::stream).collect(Collectors.toList());

        for (WriterStatus status : writerStatuses) {
            final String partitionSpec = status.getPartitionSpec();
            if (!commitRowCount.containsKey(partitionSpec)) {
                commitRowCount.put(partitionSpec, status.getTotalRecords());
            } else if (isGroupPartition) {
                throw new FlinkOdpsException("Duplicated partition " + partitionSpec
                        + " for group partition mode! ");
            } else {
                commitRowCount.put(partitionSpec,
                        commitRowCount.get(partitionSpec) + status.getTotalRecords());
            }

            if (!sessionIdsMap.containsKey(partitionSpec)) {
                sessionIdsMap.put(partitionSpec, status.getSessionId());
            } else if (!sessionIdsMap.get(partitionSpec).equals(status.getSessionId())) {
                throw new FlinkOdpsException("Partition " + partitionSpec + " has different session id: "
                        + status.getSessionId() + ", " + sessionIdsMap.get(partitionSpec));
            }
        }

        Set<String> partitionResults = commitRowCount.keySet();

        if (!isGroupPartition &&
                !tableUpsertSessionIdMap.keySet().equals(partitionResults)) {
            throw new IllegalArgumentException("Expected partition " + tableUpsertSessionIdMap.keySet()
                    + ", actual partition " + partitionResults);
        }

        Set<Long> checkpointIds =
                Arrays.stream(eventBuffer)
                        .filter(Objects::nonNull)
                        .map(SinkTaskEvent::getCheckpointID).collect(Collectors.toSet());
        if (checkpointIds.size() != 1) {
            throw new IllegalArgumentException("Expected 1 checkpointId, actual checkpointIds "
                    + checkpointIds);
        }

        final CountDownLatch latch = new CountDownLatch(partitionResults.size());
        final long startCommitTime = System.nanoTime();
        final Map<String, Boolean> commitStatus = new ConcurrentHashMap<>();

        for (String partition : partitionResults) {
            commitStatus.put(partition, false);

            final ExecuteThread.ExecuteRequest commitRequest = new ExecuteThread.ExecuteRequest() {

                @Override
                public void execute() throws IOException {
                    long startTime = System.nanoTime();
                    TableUpsertSessionImpl session = null;

                    try {
                        if (isGroupPartition) {
                            session = OdpsUtils.getOrCreateUpsertSession(tableIdentifier,
                                    partition, sessionIdsMap.get(partition), settings);
                        } else {
                            final String sessionId = tableUpsertSessionIdMap.get(partition);
                            session = OdpsUtils.getOrCreateUpsertSession(tableIdentifier,
                                    partition, sessionId, settings);

                            if (!session.getId().equals(sessionIdsMap.get(partition))) {
                                throw new FlinkOdpsException("Partition " + partition + " has different session id: "
                                        + session.getId() + ", " + sessionIdsMap.get(partition));
                            }
                        }

                        LOG.info("Start to commit session {}, partition {}, row count {}",
                                session.getId(), partition, commitRowCount.get(partition));

                        session.commit();

                        LOG.info("Commit session {}, time taken ms: {}", session.getId(),
                                NANOSECONDS.toMillis(System.nanoTime() - startTime));

                        commitStatus.put(partition, true);

                        totalCommit.incrementAndGet();
                        totalCommitTimes.addAndGet(NANOSECONDS.toMillis(System.nanoTime() - startTime));
                        totalRowCount.addAndGet(commitRowCount.get(partition));

                    } catch (Exception e) {
                        // TODO: abort session
                        commitStatus.put(partition, false);
                        throw new IOException(e);

                    } finally {
                        if (session != null) {
                            session.close();
                        }
                    }
                }

                @Override
                public void requestDone(IOException ioex) {
                    latch.countDown();

                    if (ioex != null) {
                        LOG.error("Commit session error", ioex);
                        if (ioex.getMessage().contains("Request has been cancelled.")) {
                            // ignore request cancelled
                            return;
                        }
                        context.failJob(new FlinkOdpsException(ioex));
                    } else {
                        long commitTimes = upsertCommitTimes.getOrDefault(partition, 0L);
                        upsertCommitTimes.put(partition, ++commitTimes);
                    }
                }

                @Override
                public ExecuteType getExecuteType() {
                    return ExecuteType.COMMIT;
                }
            };

            if (compactThreadMap.containsKey(partition)) {
                final ExecuteThread executeThread = compactThreadMap.get(partition);
                executeThread.addRequest(commitRequest);
            } else {
                requestQueue.add(commitRequest);
            }
        }

        try {
            latch.await();
            if (!commitStatus.values().stream().allMatch(status -> status == true)) {
                throw new IOException("Commit error");
            }
            LOG.info("Checkpoint commit table partition size {}, time taken ms: {}", partitionResults.size(),
                    NANOSECONDS.toMillis(System.nanoTime() - startCommitTime));

            LOG.info("Commit times: {}, time taken ms avg: {}, total row count: {}", totalCommit.get(),
                    ((double)totalCommitTimes.get() / (double)totalCommit.get()),
                    totalRowCount.get());
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            clearRequest();
            clearSessionInfo();
        }

        if (enableMajorCompact) {
            boolean compactAsync = true;
            if (Arrays.stream(eventBuffer).allMatch(evt -> evt != null
                    && evt.isEndInput())) {
                compactAsync = false;
            }
            doCompactPartitions(compactAsync);
        }

        return true;
    }

    protected void doCompactPartitions(boolean compactAsync) throws IOException {
        List<String> compactPartitions = new LinkedList<>();

        for (Map.Entry<String, Long> entry : upsertCommitTimes.entrySet()) {
            if (entry.getValue() >= compactThreshold &&
                    !compactThreadMap.containsKey(entry.getKey())) {
                // only one compact thread
                compactPartitions.add(entry.getKey());
            }
        }

        if (compactPartitions.size() > 0) {

            final CountDownLatch compactLatch = new CountDownLatch(compactPartitions.size());
            final long startCompactTime = System.nanoTime();

            for (String partition : compactPartitions) {
                final ExecuteThread.ExecuteRequest compactRequest = new ExecuteThread.ExecuteRequest() {

                    @Override
                    public void execute() throws IOException {
                        try {
                            long startTime = System.nanoTime();

                            OdpsUtils.majorCompact(odps, tableIdentifier, partition);

                            LOG.info("Checkpoint compact table partition {}, time taken ms: {}", partition,
                                    NANOSECONDS.toMillis(System.nanoTime() - startTime));

                            totalCompact.incrementAndGet();
                            totalCompactTimes.addAndGet(NANOSECONDS.toMillis(System.nanoTime() - startTime));
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                    }

                    @Override
                    public void requestDone(IOException ioex) {
                        compactThreadMap.remove(partition);

                        if (!compactAsync) {
                            compactLatch.countDown();
                        }

                        if (ioex != null) {
                            LOG.error("Checkpoint compact table partition {} failed",
                                    partition, ioex);
                        } else {
                            // if success, remove upsert commit times
                            upsertCommitTimes.remove(partition);
                        }
                    }

                    @Override
                    public ExecuteType getExecuteType() {
                        return ExecuteType.COMPACT;
                    }
                };

                final ExecuteThread executeThread = compactThreadMap.computeIfAbsent(partition,
                        key -> executeThreads[compactThreadMap.size() % executeThreads.length]);
                executeThread.addRequest(compactRequest);
            }

            if (!compactAsync) {
                try {
                    compactLatch.await();
                    LOG.info("Checkpoint compact table partition size {}, time taken ms: {}", compactPartitions.size(),
                            NANOSECONDS.toMillis(System.nanoTime() - startCompactTime));

                    LOG.info("Compact times: {}, time taken ms avg: {}", totalCompact.get(),
                            ((double)totalCompactTimes.get() / (double)totalCompact.get()));

                } catch (InterruptedException e) {
                    LOG.error("Compact interrupted exception", e);
                }
            }
        }
    }

    private void clearSessionInfo() {
        // Clear session info
        tableUpsertSessionIdMap.clear();
    }

    private void clearRequest() {
        while (!requestQueue.isEmpty()) {
            IOException ioex = new IOException("Request has been cancelled.");

            ExecuteThread.ExecuteRequest request = requestQueue.poll();
            if (request != null) {
                try {
                    request.requestDone(ioex);
                } catch (Throwable t) {
                    LOG.error(
                            "The handler of the request complete callback threw an exception"
                                    + (t.getMessage() == null
                                    ? "."
                                    : ": " + t.getMessage()),
                            t);
                }
            }
        }
    }

    private void clearState() {
        if (started) {
            clearRequest();
            clearSessionInfo();

            for (ExecuteThread et : executeThreads) {
                et.cancel();
            }
        }
    }
}