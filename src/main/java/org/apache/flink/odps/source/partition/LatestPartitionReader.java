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

package org.apache.flink.odps.source.partition;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Stops and discards an old reader (including prefetched rows) before acknowledging a switch. */
public final class LatestPartitionReader implements SourceReader<RowData, OdpsSourceSplit> {
    private final SourceReaderContext context;
    private final Supplier<SourceReader<RowData, OdpsSourceSplit>> factory;
    private SourceReader<RowData, OdpsSourceSplit> delegate;
    private long epoch = -1;
    private boolean ready;
    private final List<OdpsSourceSplit> restored = new ArrayList<>();
    private CompletableFuture<Void> available = new CompletableFuture<>();
    private final Set<String> outputSplits = new HashSet<>();
    private ReaderOutput<RowData> lastOutput;

    public LatestPartitionReader(SourceReaderContext context,
            Supplier<SourceReader<RowData, OdpsSourceSplit>> factory) {
        this.context = context;
        this.factory = factory;
    }

    @Override
    public void start() {
        // Wait for coordinator registration/epoch handshake before emitting restored records.
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (!ready) { return InputStatus.NOTHING_AVAILABLE; }
        lastOutput = output;
        return delegate.pollNext(new ReaderOutput<RowData>() {
            public void collect(RowData row) { output.collect(row); }
            public void collect(RowData row, long timestamp) { output.collect(row, timestamp); }
            public void emitWatermark(Watermark watermark) { output.emitWatermark(watermark); }
            public void markIdle() { output.markIdle(); }
            public void markActive() { output.markActive(); }
            public SourceOutput<RowData> createOutputForSplit(String id) {
                outputSplits.add(id);
                return output.createOutputForSplit(id);
            }
            public void releaseOutputForSplit(String id) {
                outputSplits.remove(id);
                output.releaseOutputForSplit(id);
            }
        });
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (available.isDone()) { available = new CompletableFuture<>(); }
        CompletableFuture<Void> waiting = available;
        if (ready) {
            delegate.isAvailable().whenComplete((ignored, error) -> {
                if (error == null) { waiting.complete(null); }
                else { waiting.completeExceptionally(error); }
            });
        }
        return waiting;
    }

    @Override
    public void handleSourceEvents(SourceEvent event) {
        if (!(event instanceof PartitionEvent)) {
            throw new IllegalArgumentException("Unexpected latest-partition event: " + event);
        }
        PartitionEvent change = (PartitionEvent) event;
        if (!ready && change.kind == PartitionEvent.Kind.SWITCH && change.epoch < epoch) {
            throw new IllegalStateException("Reader checkpoint epoch is newer than coordinator checkpoint");
        }
        if (change.kind != PartitionEvent.Kind.SWITCH || change.epoch < epoch) {
            return;
        }
        try {
            if (!ready || change.epoch != epoch) {
                ready = false;
                if (delegate != null) {
                    // No old buffered record may be emitted after READY is sent.
                    delegate.close();
                    if (lastOutput != null) {
                        for (String id : outputSplits) { lastOutput.releaseOutputForSplit(id); }
                    }
                    outputSplits.clear();
                }
                epoch = change.epoch;
                delegate = factory.get();
                List<OdpsSourceSplit> matching = restored.stream()
                        .filter(s -> s.splitId().startsWith(PartitionEvent.prefix(epoch)))
                        .collect(Collectors.toList());
                if (!matching.isEmpty()) {
                    delegate.addSplits(matching);
                }
                restored.clear();
                ready = true;
                available.complete(null);
                context.sendSourceEventToCoordinator(new PartitionEvent(PartitionEvent.Kind.READY,
                        epoch));
                delegate.start();
            } else {
                context.sendSourceEventToCoordinator(new PartitionEvent(PartitionEvent.Kind.READY,
                        epoch));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to stop old partition reader", e);
        }
    }

    @Override
    public void addSplits(List<OdpsSourceSplit> splits) {
        List<OdpsSourceSplit> data = new ArrayList<>();
        for (OdpsSourceSplit split : splits) {
            if (PartitionEvent.isMarker(split)) {
                if (!ready) {
                    epoch = Math.max(epoch, PartitionEvent.markerEpoch(split));
                }
            } else {
                data.add(split);
            }
        }
        if (!ready) {
            restored.addAll(data);
        } else {
            data.removeIf(s -> !s.splitId().startsWith(PartitionEvent.prefix(epoch)));
            if (!data.isEmpty()) {
                delegate.addSplits(data);
            }
        }
    }

    @Override
    public List<OdpsSourceSplit> snapshotState(long checkpointId) {
        List<OdpsSourceSplit> result = new ArrayList<>(ready
                ? delegate.snapshotState(checkpointId) : restored);
        result.add(PartitionEvent.marker(epoch));
        return result;
    }

    @Override
    public void notifyNoMoreSplits() {
        throw new IllegalStateException("Latest-partition source is unbounded");
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (ready) {
            delegate.notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        ready = false;
        if (delegate != null) {
            delegate.close();
        }
    }
}
