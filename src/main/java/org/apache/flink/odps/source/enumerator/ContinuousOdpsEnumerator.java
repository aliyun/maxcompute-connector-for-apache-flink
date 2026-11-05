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

package org.apache.flink.odps.source.enumerator;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import org.apache.flink.odps.source.partition.PartitionProgress;
import org.apache.flink.odps.source.partition.PartitionEvent;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.source.OdpsSplitStrategy;
import org.apache.flink.odps.source.OdpsStartupMode;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.source.split.PreAssignSplitAssigner;
import org.apache.flink.odps.source.split.SplitAssigner;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ContinuousOdpsEnumerator extends OdpsSplitEnumeratorBase<ContinuousOdpsSplitEnumState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContinuousOdpsEnumerator.class);

    protected final SplitEnumeratorContext<OdpsSourceSplit> context;

    protected final long discoveryInterval;

    private final List<String> prunedPartitions;
    private final List<String> requiredDataColumns;
    private final List<String> requiredPartitionColumns;

    private final OdpsSplitStrategy odpsSplitStrategy;
    private final long splitByteSize;

    private final Set<Integer> readersAwaitingSplit;

    private boolean stopTriggerScan = false;

    @Nullable private Long nextSnapshotId;

    @Nullable private String nextTxnId;

    private SplitAssigner splitAssigner;

    @Nullable private PartitionProgress partitionProgress;
    private final Set<Integer> readyReaders = new HashSet<>();
    private boolean latestScanRunning;
    private boolean closed;

    private final int splitMaxNum;

    private final OdpsStartupMode odpsStartupMode;

    private final IncrementalOptions incrementalOptions;

    public ContinuousOdpsEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context,
            OdpsConf odpsConf,
            TableIdentifier odpsTable,
            List<String> selectedColumns,
            List<String> selectedPtColumns,
            List<String> prunedPartitions,
            OdpsSplitStrategy odpsSplitStrategy,
            long splitByteSize,
            int splitMaxPerTask,
            long discoveryInterval,
            OdpsStartupMode odpsStartupMode,
            IncrementalOptions incrementalOptions,
            ContinuousOdpsSplitEnumState enumState) {
        super(odpsConf, odpsTable);

        this.context = context;
        this.prunedPartitions = prunedPartitions;
        this.odpsSplitStrategy = odpsSplitStrategy;
        this.splitByteSize = splitByteSize;
        this.splitMaxNum = context.currentParallelism() * splitMaxPerTask;
        this.splitAssigner = new PreAssignSplitAssigner(1,
                context.currentParallelism(), Collections.emptyList());
        this.readersAwaitingSplit = new LinkedHashSet<>();

        this.discoveryInterval = discoveryInterval;
        this.odpsStartupMode = odpsStartupMode;
        this.incrementalOptions = incrementalOptions;
        boolean latestPartition = odpsConf.getPropertyOrDefault("scan.partition.latest", false);
        if (enumState != null && latestPartition != (enumState.partitionProgress() != null)) {
            throw new IllegalArgumentException("Cannot change latest-partition mode when restoring a checkpoint");
        }
        partitionProgress = !latestPartition ? null : enumState == null
                ? new PartitionProgress() : enumState.partitionProgress().copy();
        TableSchema tableSchema = readTableSchema();

        // requiredDataColumns must be in order.
        requiredDataColumns = new ArrayList<>();
        tableSchema.getColumns().forEach(column -> {
                    if (selectedColumns.contains(column.getName())) {
                        requiredDataColumns.add(column.getName());
                    }
                }
        );
        requiredPartitionColumns = new ArrayList<>();
        tableSchema.getPartitionColumns().forEach(column -> {
                    if (selectedPtColumns.contains(column.getName())) {
                        requiredPartitionColumns.add(column.getName());
                    }
                }
        );

        if (enumState != null) {
            addSplits(enumState.splits());
            this.nextSnapshotId = enumState.currentSnapshotId();
            this.nextTxnId = enumState.currentTxnId();
        }
    }

    protected TableSchema readTableSchema() {
        return OdpsUtils.getTableSchema(odpsConf, odpsTable);
    }

    protected void addSplits(Collection<OdpsSourceSplit> splits) {
        splits.forEach(this::addSplit);
    }

    private void addSplit(OdpsSourceSplit split) {
        splitAssigner.addSplit(assignSuggestedTask(split), split);
    }

    @Override
    public void start() {
        if (partitionProgress != null) {
            // Keep the fixed-rate timer cheap: slow service calls must not accumulate.
            context.callAsync(() -> null, (ignored, error) -> requestLatestScan(), 0, discoveryInterval);
        } else {
            context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits, 0, discoveryInterval);
        }
    }

    @Override
    public void addReader(int reader) {
        if (partitionProgress != null) {
            readyReaders.remove(reader);
            readersAwaitingSplit.remove(reader);
            context.sendEventToSourceReader(reader, PartitionEvent.switchTo(partitionProgress.epoch));
        }
        // Normal reading remains lazy-pull-based.
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        readersAwaitingSplit.add(subtaskId);
        assignSplits();
        if (partitionProgress != null) { return; }
        // if current task assigned no split, we check conditions to scan one more time
        if (readersAwaitingSplit.contains(subtaskId)) {
            if (stopTriggerScan) {
                return;
            }
            stopTriggerScan = true;
            context.callAsync(this::scanNextSnapshot, this::processDiscoveredSplits);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (partitionProgress != null && sourceEvent instanceof PartitionEvent) {
            PartitionEvent event = (PartitionEvent) sourceEvent;
            if (event.kind == PartitionEvent.Kind.READY && event.epoch == partitionProgress.epoch) {
                readyReaders.add(subtaskId);
                assignSplits();
                if (allReadersReady()) { requestLatestScan(); }
            }
        }
    }

    @Override
    public void addSplitsBack(List<OdpsSourceSplit> splits, int subtaskId) {
        LOGGER.debug("File Source Enumerator adds splits back: {}", splits);
        if (partitionProgress != null) {
            readyReaders.remove(subtaskId);
            readersAwaitingSplit.remove(subtaskId);
            splits = splits.stream().filter(s -> s.splitId().startsWith(
                    PartitionEvent.prefix(partitionProgress.epoch))).collect(Collectors.toList());
        }
        splitAssigner.addSplitsBack(subtaskId, splits);
    }

    @Override
    public ContinuousOdpsSplitEnumState snapshotState(long checkpointId) {
        List<OdpsSourceSplit> splits = new ArrayList<>(splitAssigner.remainingSplits());
        final ContinuousOdpsSplitEnumState checkpoint =
                new ContinuousOdpsSplitEnumState(splits, nextSnapshotId, nextTxnId);
        if (partitionProgress != null) { checkpoint.withPartitionProgress(partitionProgress); }

        LOGGER.debug("Source Checkpoint is {}", checkpoint);
        return checkpoint;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
    }

    // this need to be synchronized because scan object is not thread safe. handleSplitRequest and
    // context.callAsync will invoke this. This method runs in workerExecutorThreadPool in
    // parallelism.
    protected Long latestSnapshotVersion() throws IOException {
        return OdpsUtils.getLatestVersion(getOdps(), odpsTable);
    }

    protected synchronized Optional<SessionWithNextSnapshotId> scanNextSnapshot() throws IOException {
        if (splitAssigner.numberOfRemainingSplits() >= splitMaxNum) {
            return Optional.empty();
        }

        Long latestSnapshotId = latestSnapshotVersion();
        LOGGER.info("GetLatestOdpsVersion: " + latestSnapshotId);

        if (latestSnapshotId == null) {
            return Optional.empty();
        }

        if (nextSnapshotId != null && nextSnapshotId.equals(latestSnapshotId)) {
            return Optional.empty();
        }

        if (odpsStartupMode == OdpsStartupMode.LATEST
                 && nextSnapshotId == null) {
            // For LATEST mode, set init snapshot id for the initial setup
            nextSnapshotId = latestSnapshotId;
        }

        try {
            TableReadSessionBuilder scanBuilder = sessionBuilder(prunedPartitions);

            TableBatchReadSession tableReadSession;
            if (nextSnapshotId == null &&
                    (odpsStartupMode.equals(OdpsStartupMode.LATEST_FULL)
                    || odpsStartupMode.equals(OdpsStartupMode.FROM_SNAPSHOT_FULL))) {
                // Read table once
                // TODO: Not support FROM_SNAPSHOT_FULL now
                tableReadSession = scanBuilder.buildBatchReadSession();
                LOGGER.info("Created batch read session " + tableReadSession.getId());
            } else {
                IncrementalOptions.Builder incrementalOptionsBuilder = IncrementalOptions.newBuilder();

                if (odpsStartupMode.equals(OdpsStartupMode.LATEST_FULL)
                        || odpsStartupMode.equals(OdpsStartupMode.LATEST)
                        || odpsStartupMode.equals(OdpsStartupMode.FROM_SNAPSHOT_FULL)
                        || odpsStartupMode.equals(OdpsStartupMode.FROM_SNAPSHOT)
                        || (odpsStartupMode.equals(OdpsStartupMode.FROM_TIMESTAMP) && nextSnapshotId != null)) {
                    Long startVersion = nextSnapshotId;
                    if (startVersion == null) {
                        if (odpsStartupMode.equals(OdpsStartupMode.FROM_SNAPSHOT)) {
                            if (!incrementalOptions.getType().equals(TableSnapshotSpec.Type.VERSION)) {
                                LOGGER.error("Unsupported incremental type for version start mode: "
                                        + incrementalOptions.getType());
                                throw new IllegalArgumentException("Unsupported incremental type for version start mode: "
                                        + incrementalOptions.getType());
                            }
                            startVersion = incrementalOptions.getStartVersion().get().getVersion().get();
                            if (startVersion >= latestSnapshotId) {
                                return Optional.empty();
                            }
                        } else {
                            throw new IllegalArgumentException("Illegal start version for mode: " + odpsStartupMode);
                        }
                    }
                    incrementalOptionsBuilder.startVersion(startVersion);
                } else if (odpsStartupMode.equals(OdpsStartupMode.FROM_TIMESTAMP)) {
                    if (!incrementalOptions.getType().equals(TableSnapshotSpec.Type.TIMESTAMP)) {
                        LOGGER.error("Unsupported incremental type for timestamp start mode: "
                                + incrementalOptions.getType());
                        throw new IllegalArgumentException("Unsupported incremental type for version start mode: "
                                + incrementalOptions.getType());
                    }
                    incrementalOptionsBuilder.startTimeStamp(
                            incrementalOptions.getStartTimestamp().get().getTimestamp().get());
                } else {
                    throw new IllegalArgumentException("Unsupported scan mode "
                            + odpsStartupMode + " for streaming query");
                }

                incrementalOptionsBuilder.startTxnId(nextTxnId);
                incrementalOptionsBuilder.withIncrementalMode(incrementalOptions.getIncrementalMode());
                scanBuilder.withIncrementalOptions(incrementalOptionsBuilder.build());

                tableReadSession = scanBuilder.buildIncrementalReadSession();
                LOGGER.info("Created incremental read session " + tableReadSession.getId());
            }

            if (tableReadSession.getSnapshot().getVersion().isPresent()) {
                return Optional.of(new SessionWithNextSnapshotId(tableReadSession,
                        tableReadSession.getSnapshot().getVersion().get()));
            } else {
                throw new IOException("Get version failed, session " + tableReadSession.getId());
            }
        } catch (Exception e) {
            throw new IOException("create table read session failed", e);
        }
    }

    // this method could not be synchronized, because it runs in coordinatorThread, which will make
    // it serialize.
    protected void processDiscoveredSplits(
            Optional<SessionWithNextSnapshotId> nextSnapshotIdOptional, Throwable error) {
        if (error != null) {
            LOGGER.error("Failed to enumerate files", error);
            throw new RuntimeException(error);
        }

        if (!nextSnapshotIdOptional.isPresent()) {
            return;
        }

        SessionWithNextSnapshotId sessionWithNextSnapshotId = nextSnapshotIdOptional.get();
        nextSnapshotId = sessionWithNextSnapshotId.nextSnapshotId();
        TableBatchReadSession session = sessionWithNextSnapshotId.plan();
        nextTxnId = session.getLatestTxnId().orElse(null);

        try {
            List<InputSplit> inputSplits =
                    odpsSplitStrategy.postProcessSplits(odpsSplitStrategy.getSplits(session.getInputSplitAssigner()), context);
            LOGGER.info("Get input splits count " + inputSplits.size() + " for session " + session.getId());

            if (inputSplits.size() == 0) {
                stopTriggerScan = true;
                return;
            }

            stopTriggerScan = false;

            int count = 0;
            List<OdpsSourceSplit> assignedSplits = new LinkedList<>();
            for (InputSplit split : inputSplits) {
                assignedSplits.add(
                                new OdpsSourceSplit(
                                        partitionProgress == null ? split.getSessionId() + " " + count
                                                : PartitionEvent.prefix(partitionProgress.epoch) + session.getId() + "/" + count,
                                        count,
                                        split,
                                        session));
                count += 1;
            }
            addSplits(assignedSplits);
            assignSplits();
        } catch (Exception e) {
            throw new RuntimeException("create table split failed", e);
        }
    }

    /**
     * Method should be synchronized because {@link #handleSplitRequest} and {@link
     * #processDiscoveredSplits} have thread conflicts.
     */
    protected synchronized void assignSplits() {
        if (partitionProgress != null && !allReadersReady()) { return; }
        // create assignment
        Map<Integer, List<OdpsSourceSplit>> assignment = new HashMap<>();
        Iterator<Integer> readersAwait = readersAwaitingSplit.iterator();
        Set<Integer> subtaskIds = context.registeredReaders().keySet();
        while (readersAwait.hasNext()) {
            Integer task = readersAwait.next();
            if (!subtaskIds.contains(task)) {
                readersAwait.remove();
                continue;
            }
            List<OdpsSourceSplit> splits = splitAssigner.getNext(task);
            if (!splits.isEmpty()) {
                assignment.put(task, splits);
            }
        }

        assignment.keySet().forEach(readersAwaitingSplit::remove);
        context.assignSplits(new SplitsAssignment<>(assignment));
    }

    protected int assignSuggestedTask(OdpsSourceSplit split) {
        if (split.getSplit() instanceof InputSplitWithBucket) {
            return ((InputSplitWithBucket)split.getSplit()).getBucketId() % context.currentParallelism();
        }
        return split.getSplitIndex() % context.currentParallelism();
    }

    protected TableReadSessionBuilder sessionBuilder(List<String> partitions) {
        TableReadSessionBuilder builder = new TableReadSessionBuilder().identifier(odpsTable)
                .requiredDataColumns(requiredDataColumns).requiredPartitionColumns(requiredPartitionColumns)
                .withSettings(TableUtils.getEnvironmentSettings(odpsConf))
                .withSplitOptions(odpsSplitStrategy.init(splitByteSize).build())
                .withArrowOptions(ArrowOptions.newBuilder()
                        .withDatetimeUnit(ArrowOptions.TimestampUnit.MILLI)
                        .withTimestampUnit(ArrowOptions.TimestampUnit.MICRO).build());
        if (!partitions.isEmpty()) {
            builder.requiredPartitions(partitions.stream().map(PartitionSpec::new).collect(Collectors.toList()));
        }
        return builder;
    }

    private boolean allReadersReady() {
        return context.registeredReaders().size() == context.currentParallelism()
                && readyReaders.containsAll(context.registeredReaders().keySet());
    }

    protected void requestLatestScan() {
        if (closed || latestScanRunning) { return; }
        latestScanRunning = true;
        // Workers only mutate a copy. The coordinator commits progress and pending splits together.
        PartitionProgress progress = partitionProgress.copy();
        boolean ready = allReadersReady();
        boolean canPlan = splitAssigner.numberOfRemainingSplits() < splitMaxNum;
        context.callAsync(() -> scanLatestPartition(progress, ready, canPlan), (result, error) -> {
            latestScanRunning = false;
            if (closed) { return; }
            if (error != null) { throw new IllegalStateException("Latest-partition scan failed", error); }
            boolean switched = progress.epoch != partitionProgress.epoch;
            partitionProgress = progress;
            nextSnapshotId = progress.cursor;
            if (switched) {
                LOGGER.warn("Switching to {}: unread and future old-partition updates are abandoned", progress.active);
                splitAssigner = new PreAssignSplitAssigner(1, context.currentParallelism(), Collections.emptyList());
                readersAwaitingSplit.clear();
                readyReaders.clear();
                for (Integer reader : context.registeredReaders().keySet()) {
                    context.sendEventToSourceReader(reader, PartitionEvent.switchTo(progress.epoch));
                }
            }
            processDiscoveredSplits(result, null);
        });
    }

    protected Optional<SessionWithNextSnapshotId> scanLatestPartition(
            PartitionProgress progress, boolean ready, boolean canPlan) throws Exception {
        long cutoff = latestReadableVersion();
        SortedSet<String> partitions = latestPartitions();
        progress.discover(partitions, odpsStartupMode == OdpsStartupMode.FROM_SNAPSHOT
                ? incrementalOptions.getStartVersion().get().getVersion().get() : cutoff);
        if (cutoff < progress.cursor) { throw new IOException("CDC version moved backwards"); }
        if (progress.active != null && !partitions.contains(progress.active)) {
            throw new IOException("Active partition disappeared: " + progress.active);
        }
        // Plan at least one window for each queued partition, without waiting for its readers to drain.
        if (ready && !progress.pending.isEmpty()
                && (progress.active == null || progress.active.equals(progress.initialPartition)
                    || progress.cursor > progress.initialStart || progress.cursor >= cutoff)) {
            progress.advancePartition();
            return Optional.empty(); // Wait for every reader to stop its old delegate.
        }
        if (!ready || !canPlan || progress.active == null || progress.cursor >= cutoff) { return Optional.empty(); }
        TableBatchReadSession session = sessionBuilder(Collections.singletonList(progress.active))
                .withIncrementalOptions(IncrementalOptions.newBuilder().startVersion(progress.cursor).endVersion(cutoff)
                        .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC).build())
                .buildIncrementalReadSession();
        if (!session.getSnapshot().getVersion().isPresent() || session.getSnapshot().getVersion().get() != cutoff) {
            throw new IOException("Session did not honor CDC cutoff " + cutoff);
        }
        progress.cursor = cutoff;
        return Optional.of(new SessionWithNextSnapshotId(session, cutoff));
    }

    protected long latestReadableVersion() throws Exception {
        Long latest = OdpsUtils.getLatestVersion(getOdps(), odpsTable);
        if (latest == null) { throw new IOException("Latest table version is unavailable"); }
        // Synchronous CDC metadata can report cdcLatestVersion=0. Probe its readable cutoff instead.
        TableBatchReadSession probe = sessionBuilder(Collections.emptyList())
                .withIncrementalOptions(IncrementalOptions.newBuilder().startVersion(latest)
                        .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC).build())
                .buildIncrementalReadSession();
        long cutoff = probe.getSnapshot().getVersion()
                .orElseThrow(() -> new IOException("CDC session returned no version"));
        return cutoff;
    }

    protected SortedSet<String> latestPartitions() throws Exception {
        Table table = getOdps().tables().get(odpsTable.getProject(), odpsTable.getSchema(), odpsTable.getTable());
        table.reload();
        if (!table.isTransactional() || table.getPrimaryKey() == null || table.getPrimaryKey().isEmpty()
                || table.getSchema().getPartitionColumns().isEmpty()) {
            throw new IOException("Latest-partition requires a partitioned PK Delta table or PK DLMV");
        }
        SortedSet<String> partitions = new TreeSet<>();
        for (Partition partition : table.getPartitions()) { partitions.add(partition.getPartitionSpec().toString()); }
        return partitions;
    }

    protected static class SessionWithNextSnapshotId {
        private final TableBatchReadSession session;
        private final Long nextSnapshotId;

        public SessionWithNextSnapshotId(TableBatchReadSession session, Long nextSnapshotId) {
            this.session = session;
            this.nextSnapshotId = nextSnapshotId;
        }

        public TableBatchReadSession plan() {
            return session;
        }

        public Long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}
