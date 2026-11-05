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

package org.apache.flink.odps.sink.writer;

import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class PartitionCommitInfo implements CommitInfo {

    private static final long serialVersionUID = 1L;

    private long checkpointId;

    private String sessionId;

    private String partitionSpec;

    private WriterCommitMessage writerMessage;

    private long totalRecords;

    public PartitionCommitInfo() {
    }

    public PartitionCommitInfo(
            long checkpointId,
            String sessionId,
            String partitionSpec,
            long totalRecords,
            WriterCommitMessage writerMessage) {
        this.checkpointId = checkpointId;
        this.sessionId = sessionId;
        this.partitionSpec = partitionSpec;
        this.writerMessage = writerMessage;
        this.totalRecords = totalRecords;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public void setPartitionSpec(String partitionSpec) {
        this.partitionSpec = partitionSpec;
    }

    public WriterCommitMessage getWriterMessage() {
        return writerMessage;
    }

    public void setWriterMessage(WriterCommitMessage writerMessage) {
        this.writerMessage = writerMessage;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public static class PartitionCommitInfoSerializer
            implements SimpleVersionedSerializer<CommitInfo> {

        public static final PartitionCommitInfoSerializer INSTANCE =
                new PartitionCommitInfoSerializer();

        private static final Logger LOG = LoggerFactory.getLogger(PartitionCommitInfoSerializer.class);

        private static final Charset CHARSET = StandardCharsets.UTF_8;

        private static final int MAGIC_NUMBER = 0x2c853c91;

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CommitInfo commitInfo) throws IOException {
            if (!(commitInfo instanceof PartitionCommitInfo)) {
                throw new UnsupportedOperationException(
                        "Only PartitionCommitInfo is supported.");
            }

            PartitionCommitInfo partitionCommitInfo =
                    (PartitionCommitInfo) commitInfo;

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                outputStream.writeObject(partitionCommitInfo);
            }

            byte[] infoBytes =  byteArrayOutputStream.toByteArray();
            byte[] targetBytes = new byte[8 + infoBytes.length];

            ByteBuffer bb = ByteBuffer.wrap(targetBytes).order(ByteOrder.LITTLE_ENDIAN);
            bb.putInt(MAGIC_NUMBER);
            bb.putInt(infoBytes.length);
            bb.put(infoBytes);

            return targetBytes;
        }

        @Override
        public CommitInfo deserialize(int version, byte[] serialized)
                throws IOException {
            switch (version) {
                case 1:
                    return deserializeV1(serialized);
                default:
                    throw new IOException("Unrecognized version or corrupt state: " + version);
            }
        }

        private CommitInfo deserializeV1(byte[] serialized)
                throws IOException {

            final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

            if (bb.getInt() != MAGIC_NUMBER) {
                throw new IOException("Corrupt data: Unexpected magic number.");
            }

            byte[] targetBytes = new byte[bb.getInt()];
            bb.get(targetBytes);

            try (ObjectInputStream inputStream =
                         new ObjectInputStream(new ByteArrayInputStream(targetBytes))) {
                return (PartitionCommitInfo) inputStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize PartitionCommitInfo", e);
            }
        }
    }
}
