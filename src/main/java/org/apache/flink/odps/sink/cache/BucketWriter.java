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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.writer.CommitInfo;
import org.apache.flink.odps.sink.writer.DataWriter;
import org.apache.flink.odps.sink.writer.WriterInfo;

import java.io.IOException;

public interface BucketWriter<IN, BucketID> {
    /**
     * Used to create a new {@link InProgressFileWriter}.
     */
    InProgressFileWriter<IN, BucketID> openNewInProgressFile(
            final BucketID bucketID, final long creationTime) throws IOException;
    /**
     * Used to create a new {@link DataWriter}.
     */
    DataWriter<IN> openDataWriter(final BucketID bucketID, final WriterInfo sessionInfo) throws IOException;


    SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<IN>>
        getCommittingFileSerializer();


    SimpleVersionedSerializer<CommitInfo> getCommitInfoSerializer();
}
