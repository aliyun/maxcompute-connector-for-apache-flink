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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.odps.source.split.OdpsSourceSplitState;
import org.apache.flink.table.data.RowData;

/**
 * RecordEmitter that tags each RowData with its bucket ID and emits
 * end-of-bucket markers when the bucket changes between consecutive records.
 *
 * <p>Note: the last bucket's end-of-bucket marker is NOT emitted by this emitter.
 * Downstream operators should handle the final bucket completion via Flink's
 * {@code endInput()} callback.
 */
public class BucketTaggedRecordEmitter
        implements RecordEmitter<RowData, BucketTaggedRow, OdpsSourceSplitState> {

    private int previousBucketId = -1;

    @Override
    public void emitRecord(
            RowData element,
            SourceOutput<BucketTaggedRow> output,
            OdpsSourceSplitState splitState) {
        int bucketId = splitState.getBucketId();
        if (previousBucketId >= 0 && bucketId != previousBucketId) {
            output.collect(BucketTaggedRow.endOfBucket(previousBucketId));
        }
        previousBucketId = bucketId;
        output.collect(BucketTaggedRow.of(bucketId, element));
        splitState.updateRecordsToSkip();
    }
}
