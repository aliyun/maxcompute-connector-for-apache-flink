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

package org.apache.flink.odps.source.split;

import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * JavaDoc.
 */
public class OdpsSourceSplitSerializer
        implements SimpleVersionedSerializer<OdpsSourceSplit> {
    // version 0: initial version
    // version 1: add sessionId
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(OdpsSourceSplit sourceSplit) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(sourceSplit.getSplit());
            outputStream.writeUTF(sourceSplit.splitId());
            outputStream.writeInt(sourceSplit.getSplitIndex());
            outputStream.writeObject(sourceSplit.getBatchReadSession());
            outputStream.writeLong(sourceSplit.getRecordsToSkip());
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public OdpsSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version > CURRENT_VERSION) {
            throw new IOException("Unrecognized OdpsSourceSplitSerializer version: " + version);
        }
        try (ObjectInputStream inputStream =
                     new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            InputSplit split = (InputSplit) inputStream.readObject();
            String splitId = inputStream.readUTF();
            Integer splitIndex = inputStream.readInt();
            TableBatchReadSession readSession = (TableBatchReadSession) inputStream.readObject();
            long recordsToSkip = inputStream.readLong();
            return new OdpsSourceSplit(splitId, splitIndex, split, readSession, recordsToSkip);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize OdpsSourceSplit", e);
        }
    }
}
