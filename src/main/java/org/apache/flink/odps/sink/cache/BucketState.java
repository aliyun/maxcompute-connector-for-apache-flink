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
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

public class BucketState<IN, BucketID> {

    private final BucketID bucketId;

    /**
     * The files pending to be committed, organized by checkpoint id.
     */
    private final Map<Long, List<InProgressFileWriter.CommittingFile<IN>>> committingFilePerCheckpoint;

    private final Map<Long, CommitInfo> committingInfoPerCheckpoint;

    public BucketState(
            final BucketID bucketId,
            final Map<Long, List<InProgressFileWriter.CommittingFile<IN>>> committingFilePerCheckpoint,
            final Map<Long, CommitInfo> committingInfoPerCheckpoint) {
        this.bucketId = Preconditions.checkNotNull(bucketId);
        this.committingFilePerCheckpoint = Preconditions.checkNotNull(committingFilePerCheckpoint);
        this.committingInfoPerCheckpoint = Preconditions.checkNotNull(committingInfoPerCheckpoint);
    }

    public BucketID getBucketId() {
        return bucketId;
    }

    public Map<Long, List<InProgressFileWriter.CommittingFile<IN>>> getCommittingFilePerCheckpoint() {
        return committingFilePerCheckpoint;
    }

    public Map<Long, CommitInfo> getCommittingInfoPerCheckpoint() {
        return committingInfoPerCheckpoint;
    }

    @Override
    public String toString() {
        final StringBuilder strBuilder = new StringBuilder();

        strBuilder
                .append("BucketState for bucketId=")
                .append(bucketId);

        if (!committingInfoPerCheckpoint.isEmpty()) {
            strBuilder.append(", has pending files for checkpoints: { ");
            for (long checkpointId : committingInfoPerCheckpoint.keySet()) {
                strBuilder.append(checkpointId).append(' ');
            }
            strBuilder.append('}');
        }
        return strBuilder.toString();
    }
}