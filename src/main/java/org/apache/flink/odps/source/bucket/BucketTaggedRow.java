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

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * A RowData record tagged with its source bucket ID.
 *
 * <p>When {@link #isEndOfBucket()} returns true, this is a boundary marker
 * indicating that all records for the given bucket have been emitted.
 * In that case, {@link #getRow()} returns null.
 */
public class BucketTaggedRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int bucketId;
    @Nullable
    private final RowData row;
    private final boolean endOfBucket;

    private BucketTaggedRow(int bucketId, @Nullable RowData row, boolean endOfBucket) {
        this.bucketId = bucketId;
        this.row = row;
        this.endOfBucket = endOfBucket;
    }

    public static BucketTaggedRow of(int bucketId, RowData row) {
        return new BucketTaggedRow(bucketId, row, false);
    }

    public static BucketTaggedRow endOfBucket(int bucketId) {
        return new BucketTaggedRow(bucketId, null, true);
    }

    public int getBucketId() {
        return bucketId;
    }

    @Nullable
    public RowData getRow() {
        return row;
    }

    public boolean isEndOfBucket() {
        return endOfBucket;
    }

    @Override
    public String toString() {
        if (endOfBucket) {
            return "BucketTaggedRow{END_OF_BUCKET=" + bucketId + "}";
        }
        return "BucketTaggedRow{bucket=" + bucketId + ", row=" + row + "}";
    }
}
