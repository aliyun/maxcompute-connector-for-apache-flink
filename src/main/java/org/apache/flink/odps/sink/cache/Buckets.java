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

package org.apache.flink.odps.sink.cache;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.odps.sink.writer.WriterInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.odps.table.OdpsOptions.*;

public class Buckets<IN, BucketID> implements ProcessingTimeCallback {

    private static final Logger LOG = LoggerFactory.getLogger(Buckets.class);

    private final int subtaskIndex;

    private final Map<BucketID, Bucket<IN, BucketID>> activeBuckets;

    private final BucketWriter<IN, BucketID> bucketWriter;

    private final RollingPolicy<IN, BucketID> rollingPolicy;

    private final BucketAssigner<IN, BucketID> bucketAssigner;

    private final BucketerContext bucketerContext;

    @Nullable private BucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener;

    @Nullable private FileLifeCycleListener<BucketID> fileLifeCycleListener;

    private final ProcessingTimeService procTimeService;

    private final long bucketCheckInterval;

    private final WriterThread<BucketID>[] writerThreads;

    private final int threadNum;

    private final BucketStateSerializer<IN, BucketID> bucketStateSerializer;

    private final Map<BucketID, WriterInfo> bucketsWriterInfo;

    public Buckets(
            final Configuration conf,
            final BucketWriter<IN, BucketID> bucketWriter,
            final BucketAssigner<IN, BucketID> bucketAssigner,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            final int subtaskIndex,
            final ProcessingTimeService procTimeService,
            final Comparator<BucketID> comparator) {
        this.bucketWriter = Preconditions.checkNotNull(bucketWriter);
        this.bucketAssigner = Preconditions.checkNotNull(bucketAssigner);
        this.rollingPolicy = Preconditions.checkNotNull(rollingPolicy);
        this.subtaskIndex = subtaskIndex;
        this.activeBuckets = new TreeMap<>(comparator);
        this.bucketerContext = new BucketerContext();

        this.procTimeService = procTimeService;
        this.bucketCheckInterval = conf.get(SINK_BUCKET_CHECK_INTERVAL);
        long currentProcessingTime = procTimeService.getCurrentProcessingTime();
        procTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);

        this.threadNum = conf.getInteger(SINK_THREAD_NUM);
        this.writerThreads = new WriterThread[threadNum];
        for (int i = 0; i < this.threadNum; i++) {
            final WriterThread<BucketID> t = new WriterThread<>(comparator,
                    conf.getInteger(SINK_FILE_WRITER_MAX_RETRIES));
            this.writerThreads[i] = t;
            t.setName("OdpsBucket writer thread #" + (i + 1));
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error(
                            "IO Thread '"
                                    + t.getName()
                                    + "' terminated due to an exception. Shutting down I/O Manager.",
                            e);
                }
            });
            t.start();
        }
        this.bucketStateSerializer =
                new BucketStateSerializer<>(bucketAssigner.getSerializer(),
                        bucketWriter.getCommitInfoSerializer(),
                        bucketWriter.getCommittingFileSerializer());
        this.bucketsWriterInfo = new HashMap<>();
    }

    public void setBucketLifeCycleListener(
            BucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener) {
        this.bucketLifeCycleListener = Preconditions.checkNotNull(bucketLifeCycleListener);
    }

    public void setFileLifeCycleListener(FileLifeCycleListener<BucketID> fileLifeCycleListener) {
        this.fileLifeCycleListener = Preconditions.checkNotNull(fileLifeCycleListener);
    }

    public void initializeState(final ListState<byte[]> bucketStates) throws Exception {
        for (byte[] serializedRecoveredState : bucketStates.get()) {
            final BucketState<IN, BucketID> recoveredState =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            bucketStateSerializer, serializedRecoveredState);
            handleRestoredBucketState(recoveredState);
        }
    }

    public Bucket<IN, BucketID> onElement(
            final IN value,
            final long currentProcessingTime,
            @Nullable final Long elementTimestamp,
            final long currentWatermark)
            throws Exception {
        // setting the values in the bucketer context
        bucketerContext.update(elementTimestamp, currentWatermark, currentProcessingTime);

        final BucketID bucketId = bucketAssigner.getBucketId(value, bucketerContext);
        final Bucket<IN, BucketID> bucket = getOrCreateBucketForBucketId(bucketId);
        bucket.write(value, currentProcessingTime);

        return bucket;
    }

    public void snapshotState(final long checkpointId,
                              final ListState<byte[]> bucketStatesContainer) throws IOException {
        try {
            snapshotActiveBuckets(checkpointId, bucketStatesContainer);
            LOG.info("Subtask {} checkpoint with id={} finished.", subtaskIndex, checkpointId);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void flush(final long checkpointId) throws IOException {
        LOG.info("Subtask {} checkpointing for checkpoint with id={}.", subtaskIndex, checkpointId);

        final CountDownLatch executionLatch = new CountDownLatch(activeBuckets.size());
        final Map<BucketID, Boolean> writeStatus = new ConcurrentHashMap<>();

        for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
            boolean flushData = false;
            if (!bucket.hasData()) {
                if (bucketsWriterInfo.containsKey(bucket.getBucketId())) {
                    // means other task has data, should commit empty data
                    flushData = true;
                }
            } else {
                flushData = true;
            }

            if (flushData) {
                if (bucketLifeCycleListener != null) {
                    bucketLifeCycleListener.bucketCommitting(bucket);
                }
                writeStatus.put(bucket.getBucketId(), false);
                bucket.onReceptionOfCheckpoint(checkpointId, new WriterThread.RequestDoneCallback() {
                    @Override
                    public void requestSuccessful() {
                        writeStatus.put(bucket.getBucketId(), true);
                        executionLatch.countDown();
                    }

                    @Override
                    public void requestFailed(IOException e) {
                        LOG.info("Subtask {} checkpointing for checkpoint with id={} failed. {}.",
                                subtaskIndex, checkpointId, e.getMessage());
                        writeStatus.put(bucket.getBucketId(), false);
                        executionLatch.countDown();
                    }
                });
                LOG.info("Subtask {} checkpoint for {} with bucket id={} snapshot.",
                        subtaskIndex, checkpointId, bucket.getBucketId());
            } else {
                executionLatch.countDown();
            }
        }

        try {
            while (true) {
                if (bucketLifeCycleListener != null
                        && bucketLifeCycleListener.bucketsWait()) {
                    continue;
                }
                if (executionLatch.await(500, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }

            if (!writeStatus.values().stream().allMatch(status -> status)) {
                throw new IOException("Write error");
            }
        } catch (Exception e) {
            // Clear all
            bucketsWriterInfo.clear();
            throw new IOException(e);
        } finally {
            bucketsWriterInfo.entrySet().removeIf(e -> writeStatus.containsKey(e.getKey()));
        }
    }

    public void addEmptyBuckets(final long checkpointId) throws IOException {
        for (Map.Entry<BucketID, WriterInfo> writerInfo : bucketsWriterInfo.entrySet()) {
            if (!activeBuckets.containsKey(writerInfo.getKey())) {
                Bucket<IN, BucketID> newBucket = createBucketForBucketId(writerInfo.getKey());
                newBucket.bootstrap(writerInfo.getValue());
            }
        }

        // A bootstrap callback can arrive while flush() yields to the mailbox and
        // register a bucket that already uploaded this checkpoint. Only genuinely
        // unflushed buckets need an empty commit; repeating a completed upload
        // corrupts its pending/committing-file accounting.
        final Set<BucketID> emptyBuckets = new HashSet<>();
        for (BucketID id : bucketsWriterInfo.keySet()) {
            if (!activeBuckets.get(id).hasCheckpoint(checkpointId)) {
                emptyBuckets.add(id);
            }
        }
        final CountDownLatch executionLatch = new CountDownLatch(emptyBuckets.size());
        final Map<BucketID, Boolean> writeStatus = new ConcurrentHashMap<>();
        for (BucketID newBuckedId: emptyBuckets) {
            writeStatus.put(newBuckedId, false);

            Bucket<IN, BucketID> newBucket = activeBuckets.get(newBuckedId);
            newBucket.onReceptionOfCheckpoint(checkpointId, new WriterThread.RequestDoneCallback() {
                @Override
                public void requestSuccessful() {
                    writeStatus.put(newBuckedId, true);
                    executionLatch.countDown();
                }

                @Override
                public void requestFailed(IOException e) {
                    LOG.info("Subtask {} checkpointing for checkpoint with id={} failed.",
                            subtaskIndex, checkpointId, e);
                    writeStatus.put(newBuckedId, false);
                    executionLatch.countDown();
                }
            });
            LOG.info("Subtask {} checkpoint for {} with bucket id={} snapshot.",
                    subtaskIndex, checkpointId, newBuckedId);
        }

        try {
            while (true) {
                if (bucketLifeCycleListener != null
                        && bucketLifeCycleListener.bucketsWait()) {
                    continue;
                }
                if (executionLatch.await(500, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
            if (!writeStatus.values().stream().allMatch(status -> status)) {
                throw new IOException("Write empty bucket error");
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            bucketsWriterInfo.clear();
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        final long currentTime = procTimeService.getCurrentProcessingTime();
        for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
            bucket.onProcessingTime(timestamp);
        }
        procTimeService.registerTimer(currentTime + bucketCheckInterval, this);
    }

    public void bootstrapForBucket(final BucketID bucketID,
                                   final WriterInfo writerInfo) {
        // Register the acknowledged session before creating a restored empty bucket.
        // Bucket creation must not request a second bootstrap for the same session.
        bucketsWriterInfo.put(bucketID, writerInfo);
        Bucket<IN, BucketID> bucket = activeBuckets.get(bucketID);
        if (bucket == null) {
            bucket = createBucketForBucketId(bucketID);
        }
        bucket.bootstrap(writerInfo);
    }

    public void setCommittedForBucket(final BucketID bucketID,
                                      final long checkpointId) throws Exception {
        Bucket<IN, BucketID> bucket = activeBuckets.get(bucketID);
        if (bucket != null) {
            bucket.setCommitted(checkpointId);
        }
    }

    public boolean isCommittedForCheckpoint(final long checkpointId) {
        boolean isCommitted = true;
        for (Map.Entry<BucketID, Bucket<IN, BucketID>> bucket : activeBuckets.entrySet()) {
            if (!bucket.getValue().isCommitted(checkpointId)) {
                isCommitted = false;
                break;
            }
        }
        return isCommitted;
    }

    public boolean hasUncommittedData() {
        return activeBuckets.values().stream().anyMatch(Bucket::hasUncommittedData);
    }

    public void disposeCommittingFileForCurrentCheckpoint() throws IOException {
        for (Map.Entry<BucketID, Bucket<IN, BucketID>> bucket : activeBuckets.entrySet()) {
            bucket.getValue().disposeCurrentCommittingFile();
        }
    }

    public boolean reUploadCommittingFileForBucket(final BucketID bucketID,
                                                   final long checkpointId,
                                                   final WriterInfo writerInfo,
                                                   WriterThread.RequestDoneCallback callback) throws Exception {
        Bucket<IN, BucketID> bucket = activeBuckets.get(bucketID);
        if (bucket != null) {
            return bucket.reUploadCommittingFile(checkpointId, writerInfo, callback);
        } else {
            LOG.error("Cannot find buckets {} ", bucketID);
            return false;
        }
    }

    public void close() throws Exception {
        List<AutoCloseable> closeables = new ArrayList<>(threadNum);

        for (WriterThread wt : writerThreads) {
            closeables.add(() -> {
                try {
                    wt.shutdown();
                } catch (Throwable t) {
                    throw new IOException("Error while shutting down IO Manager writer thread.", t);
                }
            });
        }
        IOUtils.closeAll(closeables);

        try {
            for (WriterThread wt : writerThreads) {
                wt.join();
            }
        } finally {
            if (activeBuckets != null) {
                for (Map.Entry<BucketID, Bucket<IN, BucketID>> bucket :
                        activeBuckets.entrySet()) {
                    bucket.getValue().dispose();
                }
                activeBuckets.clear();
            }
        }
    }

    private void snapshotActiveBuckets(
            final long checkpointId, final ListState<byte[]> bucketStatesContainer)
            throws Exception {
        bucketStatesContainer.clear();

        Iterator<Map.Entry<BucketID, Bucket<IN, BucketID>>> iterator = activeBuckets.entrySet().iterator();
        Map.Entry<BucketID, Bucket<IN, BucketID>> entry;
        while (iterator.hasNext()) {
            entry = iterator.next();

            entry.getValue().snapshot(checkpointId);

            if (!entry.getValue().isActive()) {
                LOG.info("Inactive bucket {}, checkpoint {}", entry.getKey(), checkpointId);
                iterator.remove();

            } else {
                final BucketState<IN, BucketID> bucketState = entry.getValue().getBucketState();

                final byte[] serializedBucketState =
                        SimpleVersionedSerialization.writeVersionAndSerialize(
                                bucketStateSerializer, bucketState);
                bucketStatesContainer.add(serializedBucketState);

                LOG.info("Subtask {} checkpointing: {}", subtaskIndex, bucketState);
            }
        }
    }

    private void handleRestoredBucketState(final BucketState<IN, BucketID> recoveredState)
            throws Exception {
        final BucketID bucketId = recoveredState.getBucketId();

        if (LOG.isInfoEnabled()) {
            LOG.info("Subtask {} restoring: {} bucketId: {}", subtaskIndex, recoveredState, bucketId);
        }

        final Bucket<IN, BucketID> restoredBucket =
                Bucket.restore(
                        subtaskIndex,
                        recoveredState,
                        bucketWriter,
                        rollingPolicy,
                        fileLifeCycleListener);
        updateActiveBucketId(bucketId, restoredBucket);
    }

    private void updateActiveBucketId(
            final BucketID bucketId, final Bucket<IN, BucketID> restoredBucket) throws IOException {

        final WriterThread<BucketID> writerThread = writerThreads[activeBuckets.size() % threadNum];
        restoredBucket.setWriterThread(writerThread);

        final Bucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
        if (bucket != null) {
            throw new UnsupportedOperationException("Need merge bucket");
        } else {
            activeBuckets.put(bucketId, restoredBucket);
        }
    }

    private Bucket<IN, BucketID> getOrCreateBucketForBucketId(final BucketID bucketId) throws IOException {
        Bucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
        if (bucket == null) {
            bucket = createBucketForBucketId(bucketId);
            notifyBucketCreate(bucket);
            
            if (bucketsWriterInfo.containsKey(bucketId)) {
                WriterInfo writerInfo = bucketsWriterInfo.get(bucketId);
                if (writerInfo != null) {
                    bucket.bootstrap(writerInfo);
                }
            }
        }

        // For bootstrap
        if (!bucketsWriterInfo.containsKey(bucketId)) {
            bucketsWriterInfo.put(bucketId, null);
            notifyBucketBootstrap(bucket);
        }

        return bucket;
    }

    private Bucket<IN, BucketID> createBucketForBucketId(final BucketID bucketId) {
        final WriterThread<BucketID> writerThread = writerThreads[activeBuckets.size() % threadNum];
        Bucket<IN, BucketID> bucket = Bucket.getNew(
                subtaskIndex,
                bucketId,
                bucketWriter,
                rollingPolicy,
                fileLifeCycleListener);
        bucket.setWriterThread(writerThread);
        activeBuckets.put(bucketId, bucket);
        return bucket;
    }

    private void notifyBucketCreate(Bucket<IN, BucketID> bucket) {
        if (bucketLifeCycleListener != null) {
            bucketLifeCycleListener.bucketCreated(bucket);
        }
    }

    private void notifyBucketBootstrap(Bucket<IN, BucketID> bucket) {
        if (bucketLifeCycleListener != null) {
            bucketLifeCycleListener.bucketBootstrap(bucket);
        }
    }

    /**
     * The {@link BucketAssigner.Context} exposed to the {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} whenever a new incoming element arrives.
     */
    private static final class BucketerContext implements BucketAssigner.Context {

        @Nullable private Long elementTimestamp;

        private long currentWatermark;

        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = Long.MIN_VALUE;
            this.currentProcessingTime = Long.MIN_VALUE;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = processingTime;
        }

        @Override
        public long currentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return elementTimestamp;
        }
    }
}
