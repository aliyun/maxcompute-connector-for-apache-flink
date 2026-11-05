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

import org.apache.flink.odps.sink.writer.CommitInfo;
import org.apache.flink.odps.sink.writer.DataWriter;
import org.apache.flink.odps.sink.writer.WriterInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class Bucket<IN, BucketID> {

    private static final Logger LOG = LoggerFactory.getLogger(Bucket.class);

    private final BucketID bucketId;

    private final int subtaskIndex;

    private final BucketWriter<IN, BucketID> bucketWriter;

    private final RollingPolicy<IN, BucketID> rollingPolicy;

    private InProgressFileWriter<IN, BucketID> inProgressPart;

    // Should snapshot
    private final Map<Long, List<InProgressFileWriter.CommittingFile<IN>>>
            committingFilePerCheckpoint;

    // Should snapshot
    private final Map<Long, CommitInfo> committingInfoPerCheckpoint;

    private final Map<Long, Boolean> commitStatus;

    private List<InProgressFileWriter.CommittingFile<IN>> committingFileForCurrentCheckpoint;

    private final List<InProgressFileWriter.PendingFile<IN>> pendingFileForCurrentCheckpoint;

    private DataWriter<IN> dataWriterForCurrentCheckpoint;

    private WriterThread<BucketID> writerThread;

    private WriterInfo writerInfoForCurrentCheckpoint;

    private volatile boolean bootstrap;

    @Nullable private final FileLifeCycleListener<BucketID> fileListener;

    /** Constructor to create a new empty bucket. */
    private Bucket(
            final int subtaskIndex,
            final BucketID bucketId,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener) {
        this.subtaskIndex = subtaskIndex;
        this.bucketId = checkNotNull(bucketId);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.fileListener = fileListener;

        this.pendingFileForCurrentCheckpoint = new ArrayList<>();
        this.committingFileForCurrentCheckpoint = new ArrayList<>();

        this.committingFilePerCheckpoint = new TreeMap<>();
        this.committingInfoPerCheckpoint = new ConcurrentHashMap<>();
        this.commitStatus = new TreeMap<>();
    }

    /** Constructor to create a new empty bucket. */
    private Bucket(
            final int subtaskIndex,
            final BucketState<IN, BucketID> bucketState,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener) {
        this(subtaskIndex, bucketState.getBucketId(), bucketWriter, rollingPolicy, fileListener);
        commitRecoveredPendingFiles(bucketState);
    }

    public void commitRecoveredPendingFiles(final BucketState<IN, BucketID> state) {
        this.committingInfoPerCheckpoint.putAll(state.getCommittingInfoPerCheckpoint());
        this.committingFilePerCheckpoint.putAll(state.getCommittingFilePerCheckpoint());

        if (!committingInfoPerCheckpoint.isEmpty()) {
            Preconditions.checkArgument(committingInfoPerCheckpoint.size() == 1,
                    "Commit info size: " + committingInfoPerCheckpoint.size());

            Preconditions.checkArgument(committingFilePerCheckpoint.size() == 1,
                    "Commit file size: " + committingFilePerCheckpoint.size());

            for (Long ckp : committingInfoPerCheckpoint.keySet()) {
                commitStatus.put(ckp, false);
            }
            // Try to commit pending file
            for (CommitInfo commitInfo :
                    committingInfoPerCheckpoint.values()) {
                if (fileListener != null) {
                    fileListener.onPendingFileCommitting(bucketId, commitInfo);
                }
            }
        }
    }

    public void bootstrap(final WriterInfo writerInfo) {
        this.writerInfoForCurrentCheckpoint = writerInfo;
        this.bootstrap = true;
        this.writerThread.movePendingRequest(bucketId);
    }

    public void snapshot(final long checkpointId) {
        committingFilePerCheckpoint.put(checkpointId, committingFileForCurrentCheckpoint);
        committingFileForCurrentCheckpoint = new ArrayList<>();
    }

    public void setWriterThread(WriterThread<BucketID> writerThread) {
        this.writerThread = writerThread;
    }

    public BucketID getBucketId() {
        return bucketId;
    }

    public BucketState<IN, BucketID> getBucketState() {
        Map<Long, CommitInfo> committingInfoPerCheckpoint =
                new TreeMap<>(this.committingInfoPerCheckpoint);

        Map<Long, List<InProgressFileWriter.CommittingFile<IN>>> committingFilePerCheckpoint =
                new TreeMap<>(this.committingFilePerCheckpoint);

        return new BucketState<>(bucketId, committingFilePerCheckpoint, committingInfoPerCheckpoint);
    }

    public boolean isActive() {
        return inProgressPart != null
                || !pendingFileForCurrentCheckpoint.isEmpty()
                || !committingFileForCurrentCheckpoint.isEmpty()
                || !committingInfoPerCheckpoint.isEmpty();
    }

    public boolean hasData() {
        return inProgressPart != null
                || !pendingFileForCurrentCheckpoint.isEmpty()
                || !committingFileForCurrentCheckpoint.isEmpty();
    }

    public void dispose() throws IOException {
        if (inProgressPart != null) {
            inProgressPart.dispose();
            inProgressPart = null;
        }

        disposeCurrentPendingFile();
        disposeCurrentCommittingFile();
    }

    public void write(IN element, long currentTime) throws IOException {
        if (inProgressPart == null ||
                rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Subtask {} closing in-progress part file for bucket id={} due to element {}.",
                        subtaskIndex,
                        bucketId,
                        element);
            }
            inProgressPart = closeAndCreatePartFile(currentTime);
        }
        inProgressPart.write(element, currentTime);
    }

    public void onProcessingTime(long timestamp) throws IOException {
        if (inProgressPart != null
                && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp)) {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy "
                                + "(in-progress file created @ {}, last updated @ {} and current time is {}), (file size is {}).",
                        subtaskIndex,
                        bucketId,
                        inProgressPart.getCreationTime(),
                        inProgressPart.getLastUpdateTime(),
                        timestamp,
                        inProgressPart.getSize());
            }
            closePartFile(false, -1, null);
        }
    }

    public void onReceptionOfCheckpoint(long checkpointId,
                                        WriterThread.RequestDoneCallback callback) throws IOException {
        commitStatus.put(checkpointId, false);

        if (inProgressPart != null
                && rollingPolicy.shouldRollOnCheckpoint(inProgressPart)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Subtask {} closing in-progress part file for bucket id={} on checkpoint.",
                        subtaskIndex,
                        bucketId);
            }
            closePartFile(true, checkpointId, callback);
        } else {
            // Try to commit pending file
            submitCommitPendingFile(null, true, checkpointId, callback,
                    pendingFileForCurrentCheckpoint.size() + 1);
        }
    }

    private InProgressFileWriter<IN, BucketID> closeAndCreatePartFile(final long currentTime)
            throws IOException {
        closePartFile(false, -1, null);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Subtask {} opening new part filefor bucket id={}.",
                    subtaskIndex,
                    bucketId);
        }

        return bucketWriter.openNewInProgressFile(bucketId, currentTime);
    }

    private InProgressFileWriter.PendingFile<IN> closePartFile(boolean commit,
                                                               long checkpointId,
                                                               WriterThread.RequestDoneCallback callback) throws IOException {
        InProgressFileWriter.PendingFile<IN> pendingFile = null;
        if (inProgressPart != null) {
            pendingFile = inProgressPart.closeForUpload();
            pendingFileForCurrentCheckpoint.add(pendingFile);

            inProgressPart = null;
        }

        if (pendingFile != null || commit) {
            submitCommitPendingFile(pendingFile, commit, checkpointId, callback, pendingFileForCurrentCheckpoint.size());
        }
        return pendingFile;
    }

    private void submitCommitPendingFile(InProgressFileWriter.PendingFile<IN> pendingFile,
                                         boolean commit,
                                         long checkpointId,
                                         WriterThread.RequestDoneCallback callback,
                                         int partNumber) throws IOException {
        WritePendingFileRequest request = new WritePendingFileRequest(pendingFile, commit, checkpointId, partNumber,
                new WriterThread.RequestDoneCallback() {
                    @Override
                    public void requestSuccessful() {
                        if (pendingFile != null) {
                            InProgressFileWriter.CommittingFile<IN> committingFile = pendingFile.commit();
                            committingFileForCurrentCheckpoint.add(committingFile);
                        }

                        if (commit) {
                            boolean allCommitted =
                                    pendingFileForCurrentCheckpoint.size() == committingFileForCurrentCheckpoint.size();

                            if (!allCommitted) {
                                // Check all pending file for current checkpoint
                                IOException e = new IOException("Not all pending file committed! Pending file "
                                        + pendingFileForCurrentCheckpoint.size()
                                        + ", committing file "
                                        + committingFileForCurrentCheckpoint.size()
                                        + ", bucket " + bucketId
                                        + ", partNumber " + partNumber);
                                requestFailed(e);
                                return;
                            }

                            pendingFileForCurrentCheckpoint.clear();
                        }

                        if (callback != null) {
                            callback.requestSuccessful();
                        }
                    }

                    @Override
                    public void requestFailed(IOException e) {
                        if (dataWriterForCurrentCheckpoint != null) {
                            LOG.info("Abort data writer for bucket {}", bucketId);
                            dataWriterForCurrentCheckpoint.abort();
                        }

                        if (callback != null) {
                            callback.requestFailed(e);
                        }
                    }
                });
        writerThread.addPendingRequest(request);

        if (bootstrap) {
            writerThread.movePendingRequest(bucketId);
        }
    }

    public void disposeCurrentPendingFile() throws IOException {
        for (InProgressFileWriter.PendingFile<IN> file : pendingFileForCurrentCheckpoint) {
            file.dispose();
        }
        pendingFileForCurrentCheckpoint.clear();
    }

    public void disposeCurrentCommittingFile() throws IOException {
        for (InProgressFileWriter.PendingFile<IN> file : committingFileForCurrentCheckpoint) {
            file.dispose();
        }
        committingFileForCurrentCheckpoint.clear();
    }

    public void setCommitted(final long checkpointId) {
        for (long ckp : commitStatus.keySet()) {
            if (ckp <= checkpointId) {
                commitStatus.put(ckp, true);
            }
        }
    }

    boolean hasCheckpoint(final long checkpointId) {
        return commitStatus.containsKey(checkpointId);
    }

    public boolean isCommitted(final long checkpointId) {
        if (commitStatus.containsKey(checkpointId) && !commitStatus.get(checkpointId)) {
            return false;
        }
        return true;
    }

    public boolean reUploadCommittingFile(final long checkpointId,
                                          final WriterInfo writerInfo,
                                          WriterThread.RequestDoneCallback callback) throws IOException {
        if (committingInfoPerCheckpoint.containsKey(checkpointId)) {

            // current committing file
            final List<InProgressFileWriter.CommittingFile<IN>> committingFileList;
            if (committingFilePerCheckpoint.containsKey(checkpointId)) {
                committingFileList =
                        committingFilePerCheckpoint.get(checkpointId);
                LOG.info("Find reUpload committing file, size {}", committingFileList.size());
            } else {
                // For current committing file
                committingFileList = committingFileForCurrentCheckpoint;
                LOG.info("ReUpload current committing file, size {}", committingFileList.size());
            }

            writerThread.addWriteRequest(new ReUploadCommittingFileRequest(committingFileList, writerInfo, checkpointId,
                    new WriterThread.RequestDoneCallback() {
                        @Override
                        public void requestSuccessful() {
                            if (callback != null) {
                                callback.requestSuccessful();
                            }
                        }

                        @Override
                        public void requestFailed(IOException e) {
                            if (callback != null) {
                                callback.requestFailed(e);
                            }
                        }
                    }));
            return true;
        } else {
            LOG.error("Cannot find committing info for checkpoint {} ", checkpointId);
            return false;
        }
    }

    public static <IN, BucketID> Bucket<IN, BucketID> getNew(
            final int subtaskIndex,
            final BucketID bucketId,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener) {
        return new Bucket<>(
                subtaskIndex,
                bucketId,
                bucketWriter,
                rollingPolicy,
                fileListener);
    }

    public static <IN, BucketID> Bucket<IN, BucketID> restore(
            final int subtaskIndex,
            final BucketState<IN, BucketID> bucketState,
            final BucketWriter<IN, BucketID> bucketWriter,
            final RollingPolicy<IN, BucketID> rollingPolicy,
            @Nullable final FileLifeCycleListener<BucketID> fileListener) {
        return new Bucket<>(
                subtaskIndex,
                bucketState,
                bucketWriter,
                rollingPolicy,
                fileListener);
    }

    class WritePendingFileRequest implements WriterThread.WriteRequest<BucketID> {

        private final InProgressFileWriter.PendingFile<IN> pendingFile;
        private final boolean commit;
        private final long checkpointId;
        private final WriterThread.RequestDoneCallback callback;
        private final int partNumber;

        public WritePendingFileRequest(InProgressFileWriter.PendingFile<IN> pendingFile,
                                       boolean commit,
                                       long checkpointId,
                                       int partNumber,
                                       WriterThread.RequestDoneCallback callback) {
            this.pendingFile = pendingFile;
            this.commit = commit;
            this.checkpointId = checkpointId;
            this.callback = callback;
            this.partNumber = partNumber;
        }

        @Override
        public void write() throws IOException {
            if (pendingFile != null) {
                if (dataWriterForCurrentCheckpoint == null) {
                    dataWriterForCurrentCheckpoint = bucketWriter.openDataWriter(bucketId, writerInfoForCurrentCheckpoint);
                }
                InProgressFileWriter.FileReader<IN> reader = pendingFile.getFileReader();
                try {
                    while (reader.hasNext()) {
                        IN row = reader.next();
                        dataWriterForCurrentCheckpoint.addElement(row);
                    }
                } finally {
                    // if exception, close reader
                    reader.close();
                }
            }

            if (dataWriterForCurrentCheckpoint != null) {
                dataWriterForCurrentCheckpoint.flush();
            }

            if (commit) {
                if (dataWriterForCurrentCheckpoint == null) {
                    // For empty pending file
                    dataWriterForCurrentCheckpoint = bucketWriter.openDataWriter(bucketId, writerInfoForCurrentCheckpoint);
                }
                CommitInfo commitInfo = dataWriterForCurrentCheckpoint.finish(checkpointId);
                dataWriterForCurrentCheckpoint = null;
                writerInfoForCurrentCheckpoint = null;
                bootstrap = false;

                if (fileListener != null) {
                    fileListener.onPendingFileCommitting(bucketId, commitInfo);
                }

                committingInfoPerCheckpoint.put(checkpointId, commitInfo);
            }
        }

        @Override
        public void requestDone(IOException ex) {
            if (ex != null) {
                this.callback.requestFailed(ex);
            } else {
                this.callback.requestSuccessful();
            }
        }

        @Override
        public BucketID getBucketId() {
            return bucketId;
        }

        @Override
        public int partNumber() {
            // Pending file size
            return partNumber;
        }

        @Override
        public void reset() {
            if (dataWriterForCurrentCheckpoint != null) {
                dataWriterForCurrentCheckpoint.reset();
            }
        }
    }

    class ReUploadCommittingFileRequest implements WriterThread.WriteRequest<BucketID> {

        private final List<InProgressFileWriter.CommittingFile<IN>> committingFileList;
        private final long checkpointId;
        private final WriterThread.RequestDoneCallback callback;
        private final WriterInfo writerInfo;
        private DataWriter<IN> dataWriter;

        public ReUploadCommittingFileRequest(List<InProgressFileWriter.CommittingFile<IN>> committingFileList,
                                             WriterInfo writerInfo, long checkpointId,
                                             WriterThread.RequestDoneCallback callback) {
            this.committingFileList = committingFileList;
            this.writerInfo = writerInfo;
            this.checkpointId = checkpointId;
            this.callback = callback;
        }

        @Override
        public void write() throws IOException {
            dataWriter = bucketWriter.openDataWriter(bucketId, writerInfo);

            for (InProgressFileWriter.CommittingFile<IN> committingFile : committingFileList) {
                InProgressFileWriter.FileReader<IN> reader = committingFile.getFileReader();
                try {
                    while (reader.hasNext()) {
                        IN row = reader.next();
                        dataWriter.addElement(row);
                    }
                } finally {
                    reader.close();
                }
                dataWriter.flush();
            }

            CommitInfo commitInfo = dataWriter.finish(checkpointId);
            if (fileListener != null) {
                fileListener.onPendingFileCommitting(bucketId, commitInfo);
            }

            committingInfoPerCheckpoint.put(checkpointId, commitInfo);
        }

        @Override
        public void requestDone(IOException ex) {
            if (ex != null) {
                if (dataWriter != null) {
                    dataWriter.abort();
                }
                this.callback.requestFailed(ex);
            } else {
                this.callback.requestSuccessful();
            }
        }

        @Override
        public BucketID getBucketId() {
            return bucketId;
        }

        @Override
        public int partNumber() {
            return 1;
        }

        @Override
        public void reset() {
            if (dataWriter != null) {
                dataWriter.reset();
            }
        }
    }
}
