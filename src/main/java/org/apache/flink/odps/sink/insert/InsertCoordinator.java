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
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.write.TableBatchWriteSession;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.common.OdpsWriteCoordinator;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.sink.utils.NonThrownExecutor;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_COMMIT_THREAD_NUM;
import static org.apache.flink.odps.table.OdpsOptions.INSERT_STREAMING_TUNNEL_ENABLE;
import static org.apache.flink.odps.table.OdpsOptions.SINK_DYNAMIC_PARTITION_LIMIT;

public class InsertCoordinator extends OdpsWriteCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(InsertCoordinator.class);

    protected final int dynamicPartitionLimit;
    protected final boolean isGroupPartition;
    protected final boolean supportsGrouping;
    protected final boolean useStreamingTunnel;
    protected transient Map<String, TableBatchWriteSession> tableWriteSessionMap;
    protected transient NonThrownExecutor commitExecutor;
    protected transient List<Future<?>> commitTasks;

    public InsertCoordinator(
            String operatorName,
            Configuration config,
            Context context,
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
        this.useStreamingTunnel = config.getBoolean(INSERT_STREAMING_TUNNEL_ENABLE);
    }

    @Override
    protected void initEnv() {
        this.settings = TableUtils.getEnvironmentSettings(getOdps(), odpsConf.getTunnelEndpoint(), conf);
        this.tableWriteSessionMap = new ConcurrentHashMap<>();

        int threadNum = conf.getInteger(INSERT_COMMIT_THREAD_NUM);
        this.commitExecutor = NonThrownExecutor.builder(LOG)
                .exceptionHook((errMsg, t) -> this.context.failJob(new FlinkOdpsException(errMsg, t)))
                .threadNum(threadNum)
                .waitForTasksFinish(true).build();
        this.commitTasks = new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        if (commitExecutor != null) {
            commitExecutor.close();
        }
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
        clearState();
    }

    @Override
    protected void initStaticWriteSession(int taskId) throws IOException {
        executor.execute(() -> {
            String sessionId;
            if (tableWriteSessionMap.containsKey(staticPartition)) {
                sessionId = tableWriteSessionMap.get(staticPartition).getId();
            } else {
                TableBatchWriteSession writeSession =
                        OdpsUtils.getOrCreateWriteSession(tableIdentifier, staticPartition, null, settings, useStreamingTunnel);
                sessionId = writeSession.getId();
                tableWriteSessionMap.put(staticPartition, writeSession);
            }
            sendSingleTaskAckEvents(-1, taskId,  staticPartition, sessionId, false);
        }, "initialize dynamic partition session");
    }

    @Override
    protected void initDynamicWriteSession(int taskId, String partitionSpec) throws IOException {
        executor.execute(() -> {
            String sessionId;
            if (tableWriteSessionMap.containsKey(partitionSpec)) {
                sessionId = tableWriteSessionMap.get(partitionSpec).getId();
            } else {
                if (tableWriteSessionMap.size() >= dynamicPartitionLimit) {
                    throw new FlinkOdpsException("Dynamic partition limit: " + dynamicPartitionLimit);
                }

                OdpsUtils.createPartitionIfNeeded(getTableMetaProvider(),
                        tableIdentifier, partitionSpec);
                TableBatchWriteSession writeSession =
                        OdpsUtils.getOrCreateWriteSession(tableIdentifier, partitionSpec, null, settings, useStreamingTunnel);;
                sessionId = writeSession.getId();
                tableWriteSessionMap.put(partitionSpec, writeSession);
            }
            sendSingleTaskAckEvents(-1, taskId, partitionSpec, sessionId, false);
        }, "initialize dynamic partition session");
    }

    @Override
    protected boolean commitWriteSession() throws IOException {
        final Map<String, List<WriterCommitMessage>> commitResults = new ConcurrentHashMap<>();
        final Map<String, String> sessionIdsMap = new ConcurrentHashMap<>();
        final Map<String, Long> commitRowCount = new HashMap<>();

        final List<WriterStatus> writerStatuses = Arrays.stream(eventBuffer)
                .filter(Objects::nonNull)
                .map(SinkTaskEvent::getWriterStatuses)
                .flatMap(Collection::stream).collect(Collectors.toList());

        for (WriterStatus status : writerStatuses) {
            final String partitionSpec = status.getPartitionSpec();
            if (!commitResults.containsKey(partitionSpec)) {
                commitResults.put(partitionSpec, new ArrayList<>());
            } else if (isGroupPartition) {
                throw new FlinkOdpsException("Duplicated partition " + partitionSpec
                        + " for group partition mode! ");
            }

            if (!sessionIdsMap.containsKey(partitionSpec)) {
                sessionIdsMap.put(partitionSpec, status.getSessionId());
            } else if (!sessionIdsMap.get(partitionSpec).equals(status.getSessionId())) {
                throw new FlinkOdpsException("Partition " + partitionSpec + " has different writer session id: "
                        + status.getSessionId() + ", " + sessionIdsMap.get(partitionSpec));
            }

            commitResults.get(partitionSpec).add(status.getWriterMessage());
            commitRowCount.put(partitionSpec,
                    commitRowCount.getOrDefault(partitionSpec, 0L) + status.getTotalRecords());
        }

        Set<String> partitionResults = commitResults.keySet();

        if (!isGroupPartition &&
                !tableWriteSessionMap.keySet().equals(partitionResults)) {
            throw new IllegalArgumentException("Expected partition " + tableWriteSessionMap.keySet()
                    + ", actual partition " + partitionResults);
        }

        Set<Long> checkpointIds =
                Arrays.stream(eventBuffer).filter(Objects::nonNull)
                        .map(SinkTaskEvent::getCheckpointID).collect(Collectors.toSet());
        if (checkpointIds.size() != 1) {
            throw new IllegalArgumentException("Expected 1 checkpointId, actual checkpointIds "
                    + checkpointIds);
        }

        final CountDownLatch latch = new CountDownLatch(partitionResults.size());
        final Map<String, Boolean> commitStatus = new ConcurrentHashMap<>();
        final long startCommitTime = System.nanoTime();

        for (String partition : partitionResults) {
            commitStatus.put(partition, false);

            Future<?> commitTask = commitExecutor.submit(() -> {
                TableBatchWriteSession session;

                if (isGroupPartition) {
                    session = OdpsUtils.getOrCreateWriteSession(tableIdentifier, partition, sessionIdsMap.get(partition), settings,
                                                                useStreamingTunnel);
                } else {
                    session = tableWriteSessionMap.get(partition);
                    if (!session.getId().equals(sessionIdsMap.get(partition))) {
                        throw new FlinkOdpsException("Partition " + partition + " has different session id: "
                                + session.getId() + ", " + sessionIdsMap.get(partition));
                    }
                }

                try {
                    LOG.info("Start to commit session {}, partition {}, row count {}",
                            session.getId(), partition, commitRowCount.get(partition));
                    long startTime = System.nanoTime();

                    session.commit(commitResults.get(partition).toArray(new WriterCommitMessage[0]));

                    LOG.info("Commit session {}, time taken ms: {}", session.getId(),
                            NANOSECONDS.toMillis(System.nanoTime() - startTime));
                    commitStatus.put(partition, true);

                } catch (Exception e) {
                    commitStatus.put(partition, false);
                    throw new IOException(e);
                } finally {
                    latch.countDown();
                }
            }, "Commit write session for partition " + partition);
            commitTasks.add(commitTask);
        }

        try {
            latch.await();
            if (!commitStatus.values().stream().allMatch(status -> status == true)) {
                throw new IOException("Commit error");
            }
            LOG.info("Checkpoint commit table partition size {}, time taken ms: {}", partitionResults.size(),
                    NANOSECONDS.toMillis(System.nanoTime() - startCommitTime));
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            clearState();
        }
        return true;
    }

    private void clearState() {
        if (started) {
            tableWriteSessionMap.clear();

            for (Future<?> task : commitTasks) {
                if (!task.isDone()) {
                    task.cancel(true);
                }
            }
            commitTasks.clear();
        }
    }
}