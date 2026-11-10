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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.cache.Bucket;
import org.apache.flink.odps.sink.cache.BucketLifeCycleListener;
import org.apache.flink.odps.sink.cache.Buckets;
import org.apache.flink.odps.sink.cache.PartitionBucketAssigner;
import org.apache.flink.odps.sink.cache.PartitionBucketRollingPolicy;
import org.apache.flink.odps.sink.cache.PartitionBucketWriter;
import org.apache.flink.odps.sink.cache.PartitionBucketsBuilder;
import org.apache.flink.odps.sink.cache.WriterThread;
import org.apache.flink.odps.sink.common.OdpsWriteFunction;
import org.apache.flink.odps.sink.common.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.partition.PartitionAssigner;
import org.apache.flink.odps.sink.partition.PartitionComputer;
import org.apache.flink.odps.sink.utils.MemoryUtils;
import org.apache.flink.odps.sink.utils.NonThrownExecutor;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.sink.writer.PartitionCommitInfo;
import org.apache.flink.odps.sink.writer.PartitionWriterInfo;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCachedInsertFunction extends OdpsWriteFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(FileCachedInsertFunction.class);

    protected transient Buckets<RowData, String> buckets;
    protected transient NonThrownExecutor partitionCreateExecutor;
    protected transient TypeSerializer<RowData> inputSerializer;
    protected transient BufferPool bufferPool;

    protected final boolean isGroupPartition;

    protected transient long lastCommittingCheckpointId;

    protected transient Map<Long, StreamStateHandle> stateHandleMap;

    // -------------------------- state descriptors ---------------------------
    private ListState<byte[]> bucketStates;
    private static final ListStateDescriptor<byte[]> BUCKET_STATE_DESC =
            new ListStateDescriptor<>("bucket-states", BytePrimitiveArraySerializer.INSTANCE);

    private ListState<Long> lastCommittingCheckpointState;
    private static final ListStateDescriptor<Long> LAST_COMMITTING_CKP_DESC =
            new ListStateDescriptor<>("last-ckp-counter", LongSerializer.INSTANCE);

    private ListState<StreamStateHandle> commitStates;
    private static final ListStateDescriptor<StreamStateHandle> COMMIT_STATE_DESC =
            new ListStateDescriptor<>("commit-states", new JavaSerializer<>());

    private ListState<Boolean> endInputState;
    private static final ListStateDescriptor<Boolean> END_INPUT_STATE_DESC =
            new ListStateDescriptor<>("end-input-state_desc", BooleanSerializer.INSTANCE);

    protected transient volatile boolean sendInputEnded;

    public FileCachedInsertFunction(Configuration config,
                                    OdpsConf odpsConf,
                                    TableIdentifier identifier,
                                    String partition,
                                    DataSchema schema,
                                    boolean isDynamicPartition,
                                    boolean isGroupPartition,
                                    OdpsWriteOptions writeOptions,
                                    PartitionAssigner<RowData> partitionAssigner,
                                    WriteOperationType writeOperationType,
                                    RowType rowType) {
        super(config, odpsConf, identifier, partition, schema,
                isDynamicPartition, writeOptions, partitionAssigner, writeOperationType, rowType);
        this.isGroupPartition = isGroupPartition;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);

        this.endInputState = context.getOperatorStateStore().getListState(END_INPUT_STATE_DESC);
        if (context.isRestored()) {
            for (Boolean endInput : endInputState.get()) {
                inputEnded = endInput;
            }
            LOG.info("Initialize sendInputEnded {}", inputEnded);
        }

        LOG.info("Initialize FileCachedFunction, context is restore: {}", context.isRestored());

        this.stateHandleMap = new HashMap<>();
        this.inputSerializer = new RowDataSerializer(rowType);
        this.bufferPool = MemoryUtils.getGlobalBufferPool(config).createBufferPool(1, Integer.MAX_VALUE);
        this.partitionCreateExecutor = NonThrownExecutor.builder(LOG)
                .threadNum(1)
                .waitForTasksFinish(false).build();

        PartitionBucketsBuilder bucketsBuilder = new PartitionBucketsBuilder(config);
        bucketsBuilder.withRollingPolicy(PartitionBucketRollingPolicy.builder()
                .withMaxPartSize(config.get(OdpsOptions.SINK_FILE_ROLLING_MAX_SIZE))
                .build());
        bucketsBuilder.withProcessingTimeService(processingTimeService);
        bucketsBuilder.withBucketAssigner(new PartitionBucketAssigner(
                PartitionComputer.instance(dataSchema, rowType, partition, config.get(OdpsOptions.PARTITION_DEFAULT_VALUE))));
        bucketsBuilder.withBucketWriter(new PartitionBucketWriter(config,
                odpsConf,
                tableIdentifier,
                dataSchema,
                writeOperationType,
                taskID,
                taskParallel,
                bufferPool,
                inputSerializer,
                rowType));
        bucketsBuilder.withBucketComparator(Comparator.naturalOrder());

        this.lastCommittingCheckpointState = context.getOperatorStateStore().getListState(LAST_COMMITTING_CKP_DESC);
        this.lastCommittingCheckpointId = -1L;
        if (context.isRestored()) {
            // Coordinator bootstrap events refer to the checkpoint Flink restored,
            // not the initial -1 generation of a fresh writer.
            lastCommittingCheckpointId = context.getRestoredCheckpointId().orElse(-1L);
            for (long checkpoint : lastCommittingCheckpointState.get()) {
                lastCommittingCheckpointId = Math.max(lastCommittingCheckpointId, checkpoint);
            }
            LOG.info("Initialize lastCommittingCheckpointId {}", lastCommittingCheckpointId);
        }

        this.commitStates = context.getOperatorStateStore().getListState(COMMIT_STATE_DESC);
        this.buckets = bucketsBuilder.createBuckets(taskID);
        this.bucketStates = context.getOperatorStateStore().getListState(BUCKET_STATE_DESC);

        buckets.setBucketLifeCycleListener(
                new BucketLifeCycleListener<RowData, String>() {

                    private Set<String> pendingPartitions = new HashSet<>();
                    private final AtomicInteger requestsNotReturned = new AtomicInteger(0);

                    @Override
                    public void bucketCreated(Bucket<RowData, String> bucket) {
                        if (isGroupPartition) {
                            pendingPartitions.add(bucket.getBucketId());
                            if (pendingPartitions.size() >= 64) {
                                final String[] submitPartitions = pendingPartitions.toArray(new String[0]);
                                requestsNotReturned.incrementAndGet();
                                partitionCreateExecutor.execute(() -> {
                                            OdpsUtils.createPartitions(getOdps(), tableIdentifier, submitPartitions);
                                            requestsNotReturned.decrementAndGet();
                                        },
                                        "initialize partition");
                                pendingPartitions = new HashSet<>();
                            }
                        }
                    }

                    @Override
                    public void bucketBootstrap(Bucket<RowData, String> bucket) {
                        if (!isGroupPartition) {
                            sendBootstrapEvent(bucket.getBucketId());
                        } else {
                            buckets.bootstrapForBucket(bucket.getBucketId(), null);
                        }
                    }

                    @Override
                    public void bucketCommitting(Bucket<RowData, String> bucket) {
                        if (!pendingPartitions.isEmpty()) {
                            final String[] submitPartitions = pendingPartitions.toArray(new String[0]);
                            requestsNotReturned.incrementAndGet();
                            partitionCreateExecutor.execute(() -> {
                                OdpsUtils.createPartitions(getOdps(), tableIdentifier, submitPartitions);
                                requestsNotReturned.decrementAndGet();
                            },"committing partition");

                            pendingPartitions = new HashSet<>();
                        }

                        while (this.requestsNotReturned.get() > 0) {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                throw new FlinkOdpsException(e);
                            }
                        }
                    }

                    @Override
                    public void bucketInactive(Bucket<RowData, String> bucket) {
                        // Do nothing
                    }

                    @Override
                    public boolean bucketsWait() throws Exception {
                        return executor.tryYield();
                    }
                });

        buckets.setFileLifeCycleListener((bucketID, commitInfo) -> {
            Preconditions.checkArgument(commitInfo instanceof PartitionCommitInfo,
                    "The write function can only handle PartitionCommitInfo");

            PartitionCommitInfo partCommitInfo = (PartitionCommitInfo) commitInfo;
            List<WriterStatus> statusList = new ArrayList<>();

            WriterStatus writerStatus = new WriterStatus();
            writerStatus.setSessionId(partCommitInfo.getSessionId());
            writerStatus.setPartitionSpec(partCommitInfo.getPartitionSpec());
            writerStatus.setTotalRecords(partCommitInfo.getTotalRecords());
            writerStatus.setWriterMessage(partCommitInfo.getWriterMessage());

            statusList.add(writerStatus);

            sendCommitEvent(statusList, partCommitInfo.getCheckpointId(), true);
        });
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close fileCachedFunction");

        super.close();
        if (buckets != null) {
            buckets.close();
        }

        if (bufferPool != null) {
            bufferPool.lazyDestroy();
        }
    }

    @Override
    public void endInput() throws Exception {
        super.endInput();
        // Streaming tasks must return so Flink can deliver the final checkpoint
        // barrier and reopen the coordinator event gate before waiting for ACKs.
        if (streamConfig.isCheckpointingEnabled()) {
            return;
        }
        LOG.info("Receive end input event for task[{}]", taskID);
        // A bounded job has no checkpoint trigger after its input finishes.
        // Complete the same bucket/ack protocol before allowing termination.
        snapshotState(Long.MAX_VALUE);
    }

    @Override
    public void processElement(RowData rowData,
                               Context context,
                               Collector<Object> collector) throws Exception {
        if (inputEnded) {
            // For task failover
            return;
        }
        buckets.onElement(rowData,
                processingTimeService.getCurrentProcessingTime(),
                null,
                Long.MAX_VALUE);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        sendInputEnded = false;
        if (!buckets.isCommittedForCheckpoint(checkpointId)) {
            throw new IOException("Task failed for checkpoint " + checkpointId + " aborted");
        }
    }

    @Override
    protected void snapshotState(long checkpointId) throws IOException {
        if (checkpointId != Long.MAX_VALUE) {
            acknowledgeCheckpoint(checkpointId);
        }
        buckets.flush(checkpointId);
        checkForCommittingCheckpoint(checkpointId);
    }

    protected void checkForEndInput(long checkpointId) throws IOException {
        if (inputEnded) {
            sendEndInputEvent(checkpointId);
            while (!sendInputEnded) {
                try {
                    this.executor.yield();
                } catch (InterruptedException e) {
                    LOG.error("Snapshot state error: ", e);
                    throw new IOException(e);
                }
            }
        }
    }

    protected void checkForCommittingCheckpoint(long checkpointId) throws IOException {
        checkForEndInput(checkpointId);
        sendCheckpointSuccessEvent(checkpointId);
        // For empty data: should wait all task checkpoint success
        // Yielding can finish a nested, later checkpoint before this wait resumes.
        while (lastCommittingCheckpointId < checkpointId) {
            try {
                this.executor.yield();
            } catch (InterruptedException e) {
                LOG.error("Snapshot state error: ", e);
                throw new IOException(e);
            }
        }
        buckets.addEmptyBuckets(checkpointId);

        // wait for current checkpoint
        while (!buckets.isCommittedForCheckpoint(checkpointId)) {
            try {
                this.executor.yield();
            } catch (InterruptedException e) {
                LOG.error("Snapshot state error: ", e);
                throw new IOException(e);
            }
        }
        // delete file for current checkpoint
        buckets.disposeCommittingFileForCurrentCheckpoint();
        LOG.info("All partition for checkpoint {} has committed ", checkpointId);

        try {
            lastCommittingCheckpointState.clear();
            lastCommittingCheckpointState.add(lastCommittingCheckpointId);

            endInputState.clear();
            endInputState.add(inputEnded);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        Preconditions.checkArgument(event instanceof TaskAckEvent,
                "The write function can only handle TaskAckEvent");
        TaskAckEvent ackEvent = (TaskAckEvent) event;

        LOG.info("Handle operator event for task[{}], {}", taskID, ackEvent);

        if (ackEvent.isBootstrap()) {
            String sessionId = ackEvent.getSessionId();
            String partition = ackEvent.getPartition();

            if (!StringUtils.isNullOrWhitespaceOnly(sessionId)) {
                PartitionWriterInfo partitionWriterInfo =
                        new PartitionWriterInfo(ackEvent.getCheckpointId(), ackEvent.getSessionId(), ackEvent.getPartition());
                if (ackEvent.getCheckpointId() == lastCommittingCheckpointId
                        || (inputEnded && ackEvent.getCheckpointId() == Long.MAX_VALUE)) {
                    buckets.bootstrapForBucket(ackEvent.getPartition(), partitionWriterInfo);
                } else {
                    LOG.error("Ignore bootstrap event for task[{}], {}", taskID, ackEvent);
                }
            } else {
                LOG.error("Create session failed! " + partition);
            }
        } else if (ackEvent.isCheckpoint()) {
            lastCommittingCheckpointId = Math.max(lastCommittingCheckpointId, ackEvent.getCheckpointId());

        } else if (ackEvent.isEndInput()) {
            sendInputEnded = true;

        } else if (ackEvent.isCommitted()) {
            try {
                long checkpointId = ackEvent.getCheckpointId();
                buckets.setCommittedForBucket(ackEvent.getPartition(), checkpointId);
            } catch (Exception e) {
                LOG.error("Dispose Committing file failed", e);
            }
        } else {
            boolean reUpload = false;
            try {
                PartitionWriterInfo partitionWriterInfo = null;

                if (!isGroupPartition) {
                    Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(ackEvent.getSessionId()),
                            "Write session id is null");
                    partitionWriterInfo =
                            new PartitionWriterInfo(ackEvent.getCheckpointId(), ackEvent.getSessionId(), ackEvent.getPartition());
                }

                reUpload = buckets.reUploadCommittingFileForBucket(ackEvent.getPartition(),
                        ackEvent.getCheckpointId(), partitionWriterInfo, new WriterThread.RequestDoneCallback() {
                            @Override
                            public void requestSuccessful() {
                                LOG.error("Reupload committing file success");
                            }

                            @Override
                            public void requestFailed(IOException e) {
                                LOG.error("Reupload committing file failed", e);
                                sendCommitEvent(new ArrayList<>(), ackEvent.getCheckpointId(), false);
                            }
                        });
            } catch (Exception e) {
                LOG.error("Reupload committing file failed", e);
            }
            if (!reUpload) {
                sendCommitEvent(new ArrayList<>(), ackEvent.getCheckpointId(), false);
            }
        }
    }

    private void sendBootstrapEvent(String partition) {
        LOG.info("Send bootstrap event to coordinator, task[{}], partition[{}], checkpoint[{}]",
                taskID, partition, lastCommittingCheckpointId);
        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .checkpointID(lastCommittingCheckpointId)
                .bootstrap(true)
                .endInput(false)
                .requiredPartition(partition)
                .writeStatus(new ArrayList<>())
                .build();
        eventGateway.sendEventToCoordinator(event);
    }

    private void sendCommitEvent(List<WriterStatus> statusList, long checkpointId, boolean writeSuccess) {
        LOG.info("Send commit event to coordinator, task[{}], checkpoint[{}].",
                taskID, checkpointId);

        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .checkpointID(checkpointId)
                .bootstrap(false)
                .endInput(false)
                .writeSuccess(writeSuccess)
                .writeStatus(statusList)
                .build();
        eventGateway.sendEventToCoordinator(event);
    }

    private void sendCheckpointSuccessEvent(long checkpointId) {
        LOG.info("Send checkpoint success event to coordinator, task[{}], checkpoint[{}].",
                taskID, checkpointId);

        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .checkpointID(checkpointId)
                .bootstrap(false)
                .endInput(false)
                .writeSuccess(false)
                .checkpointSuccess(true)
                .writeStatus(new ArrayList<>())
                .build();
        eventGateway.sendEventToCoordinator(event);
    }

    private void sendEndInputEvent(long checkpointId) {
        LOG.info("Send end input event to coordinator, task[{}], checkpoint[{}].", taskID, checkpointId);

        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .checkpointID(checkpointId)
                .bootstrap(false)
                .endInput(true)
                .writeSuccess(true)
                .build();
        eventGateway.sendEventToCoordinator(event);
    }
}
