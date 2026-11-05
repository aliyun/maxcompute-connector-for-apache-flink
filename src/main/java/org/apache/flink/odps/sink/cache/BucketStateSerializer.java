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

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.odps.sink.writer.CommitInfo;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class BucketStateSerializer<IN, BucketID> implements SimpleVersionedSerializer<BucketState<IN, BucketID>> {

    private static final int MAGIC_NUMBER = 0x1e764b78;

    private static final Logger LOG = LoggerFactory.getLogger(BucketStateSerializer.class);

    private final SimpleVersionedSerializer<BucketID> bucketIdSerializer;
    private final SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<IN>> commitFileSerializer;
    private final SimpleVersionedSerializer<CommitInfo> commitInfoSerializer;

    public BucketStateSerializer(
            final SimpleVersionedSerializer<BucketID> bucketIdSerializer,
            final SimpleVersionedSerializer<CommitInfo> commitInfoSerializer,
            final SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<IN>> commitFileSerializer) {
        this.bucketIdSerializer = Preconditions.checkNotNull(bucketIdSerializer);
        this.commitInfoSerializer = Preconditions.checkNotNull(commitInfoSerializer);
        this.commitFileSerializer = Preconditions.checkNotNull(commitFileSerializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BucketState<IN, BucketID> state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(state, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public BucketState<IN, BucketID> deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV1(BucketState<IN, BucketID> state, DataOutputView dataOutputView)
            throws IOException {
        SimpleVersionedSerialization.writeVersionAndSerialize(
                bucketIdSerializer, state.getBucketId(), dataOutputView);

        // put the map of committing files per checkpoint
        final Map<Long, List<InProgressFileWriter.CommittingFile<IN>>> commitFiles =
                state.getCommittingFilePerCheckpoint();
        dataOutputView.writeInt(commitFileSerializer.getVersion());

        // LOG.info("SerializeV1 commitFiles:" + commitFiles.size());
        dataOutputView.writeInt(commitFiles.size());

        for (Map.Entry<Long, List<InProgressFileWriter.CommittingFile<IN>>>
                 commitFile : commitFiles.entrySet()) {
            final List<InProgressFileWriter.CommittingFile<IN>> commitFileList =
                    commitFile.getValue();

            dataOutputView.writeLong(commitFile.getKey());

            // LOG.info("SerializeV1 commitFileList:" + commitFileList.size());

            dataOutputView.writeInt(commitFileList.size());

            for (InProgressFileWriter.CommittingFile<IN> file : commitFileList) {
                byte[] serialized =
                        commitFileSerializer.serialize(file);
                dataOutputView.writeInt(serialized.length);
                dataOutputView.write(serialized);
            }
        }

        // put the map of committing info per checkpoint
        final Map<Long, CommitInfo> commitInfoMap = state.getCommittingInfoPerCheckpoint();
        dataOutputView.writeInt(commitInfoSerializer.getVersion());

        // LOG.info("SerializeV1 commitInfoMap:" + commitInfoMap.size());

        dataOutputView.writeInt(commitInfoMap.size());

        for (Map.Entry<Long, CommitInfo> commitInfo : commitInfoMap.entrySet()) {
            dataOutputView.writeLong(commitInfo.getKey());
            final CommitInfo info = commitInfo.getValue();
            byte[] serialized =
                    commitInfoSerializer.serialize(info);
            // LOG.info("SerializeV1 serialized length:" + serialized.length);
            dataOutputView.writeInt(serialized.length);
            dataOutputView.write(serialized);
        }
    }

    private BucketState<IN, BucketID> deserializeV1(DataInputView dataInputView) throws IOException {
        final BucketID bucketId =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        bucketIdSerializer, dataInputView);

        final int commitFileSerializerVersion = dataInputView.readInt();
        final int numCheckpoints = dataInputView.readInt();
        // LOG.info("deserializeV1 commitFiles:" + numCheckpoints);

        final Map<Long, List<InProgressFileWriter.CommittingFile<IN>>>
                commitFilePerCheckpoint = new TreeMap<>();
        for (int i = 0; i < numCheckpoints; i++) {
            final long checkpointId = dataInputView.readLong();
            final int numOfCommitFiles = dataInputView.readInt();

            final List<InProgressFileWriter.CommittingFile<IN>> commitFiles =
                    new ArrayList<>(numOfCommitFiles);
            // LOG.info("deserializeV1 commitFileList:" + numOfCommitFiles);

            for (int j = 0; j < numOfCommitFiles; j++) {
                final byte[] bytes = new byte[dataInputView.readInt()];
                dataInputView.readFully(bytes);
                commitFiles.add(commitFileSerializer.deserialize(
                                commitFileSerializerVersion, bytes));
            }
            commitFilePerCheckpoint.put(checkpointId, commitFiles);
        }

        final int commitInfoSerializerVersion = dataInputView.readInt();
        final int numCommitCheckpoints = dataInputView.readInt();
        // LOG.info("deserializeV1 commitInfoMap:" + numCommitCheckpoints);

        final Map<Long, CommitInfo> commitInfoPerCheckpoint = new TreeMap<>();
        for (int i = 0; i < numCommitCheckpoints; i++) {
            final long checkpointId = dataInputView.readLong();
            final byte[] bytes = new byte[dataInputView.readInt()];
            dataInputView.readFully(bytes);

            // LOG.info("deserializeV1 serialized length:" + bytes.length);
            final CommitInfo commitInfo = commitInfoSerializer.deserialize(
                    commitInfoSerializerVersion, bytes);
            commitInfoPerCheckpoint.put(checkpointId, commitInfo);
        }

        return new BucketState<>(
                bucketId,
                commitFilePerCheckpoint,
                commitInfoPerCheckpoint);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}