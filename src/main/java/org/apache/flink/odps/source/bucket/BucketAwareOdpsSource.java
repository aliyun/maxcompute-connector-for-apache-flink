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

package org.apache.flink.odps.source.bucket;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.source.OdpsSourceBase;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.table.data.RowData;

import java.util.function.Supplier;

/**
 * A Source wrapper that outputs {@link BucketTaggedRow} (RowData tagged with bucket ID)
 * instead of plain RowData. Delegates all enumerator and split logic to the underlying
 * ODPS source, only replacing the reader with a bucket-aware version.
 *
 * <p>Usage:
 * <pre>{@code
 * BatchOdpsSource source = ...; // configured with splitStrategy=BUCKET
 * BucketAwareOdpsSource bucketSource = new BucketAwareOdpsSource(source);
 * DataStream<BucketTaggedRow> stream = env.fromSource(bucketSource, ...);
 * }</pre>
 *
 * <p>Output guarantees (within a single subtask):
 * <ul>
 *   <li>Bucket IDs are monotonically increasing (empty buckets are skipped)</li>
 *   <li>All data records of a bucket precede the endOfBucket marker for that bucket</li>
 *   <li>After endOfBucket(N), the next record belongs to a bucket with ID &gt; N</li>
 *   <li>The last bucket's endOfBucket marker is NOT emitted; downstream operators
 *       should handle final bucket completion via {@code endInput()}</li>
 * </ul>
 *
 * @param <CheckpointT> the enumerator checkpoint type of the delegate source
 */
public class BucketAwareOdpsSource<CheckpointT>
        implements Source<BucketTaggedRow, OdpsSourceSplit, CheckpointT> {

    private static final long serialVersionUID = 1L;

    private final OdpsSourceBase<CheckpointT> delegate;

    public BucketAwareOdpsSource(OdpsSourceBase<CheckpointT> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Boundedness getBoundedness() {
        return delegate.getBoundedness();
    }

    @Override
    public SourceReader<BucketTaggedRow, OdpsSourceSplit> createReader(SourceReaderContext context) {
        Supplier<SplitReader<RowData, OdpsSourceSplit>> splitReaderSupplier =
                delegate.newSplitReaderSupplier();
        BucketTaggedRecordEmitter emitter = new BucketTaggedRecordEmitter();
        return new BucketAwareSourceReader(
                splitReaderSupplier, emitter, context.getConfiguration(), context);
    }

    @Override
    public SplitEnumerator<OdpsSourceSplit, CheckpointT> createEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context) throws Exception {
        return delegate.createEnumerator(context);
    }

    @Override
    public SplitEnumerator<OdpsSourceSplit, CheckpointT> restoreEnumerator(
            SplitEnumeratorContext<OdpsSourceSplit> context, CheckpointT checkpoint) throws Exception {
        return delegate.restoreEnumerator(context, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<OdpsSourceSplit> getSplitSerializer() {
        return delegate.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<CheckpointT> getEnumeratorCheckpointSerializer() {
        return delegate.getEnumeratorCheckpointSerializer();
    }
}
