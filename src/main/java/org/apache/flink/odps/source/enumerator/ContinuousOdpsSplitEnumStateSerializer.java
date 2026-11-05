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

import org.apache.flink.odps.source.partition.PartitionProgress;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.source.split.OdpsSourceSplitSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link ContinuousOdpsSplitEnumState}. */
public class ContinuousOdpsSplitEnumStateSerializer
        implements SimpleVersionedSerializer<ContinuousOdpsSplitEnumState> {

    private static final int CURRENT_VERSION = 2;

    private static final long INVALID_SNAPSHOT = -1;

    private final SimpleVersionedSerializer<OdpsSourceSplit> splitSerializer =
            new OdpsSourceSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ContinuousOdpsSplitEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(state.splits().size());
            for (OdpsSourceSplit split : state.splits()) {
                out.writeInt(splitSerializer.getVersion());
                byte[] serializedSplit = splitSerializer.serialize(split);
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
            }
            Long currentSnapshotId = state.currentSnapshotId();
            out.writeLong(currentSnapshotId == null ? INVALID_SNAPSHOT : currentSnapshotId);
            String currentTxnId = state.currentTxnId();
            if (currentTxnId == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeUTF(currentTxnId);
            }
            out.writeBoolean(state.partitionProgress() != null);
            if (state.partitionProgress() != null) {
                PartitionProgress p = state.partitionProgress();
                out.writeBoolean(p.initialized);
                out.writeLong(p.initialStart);
                out.writeLong(p.cursor);
                out.writeLong(p.epoch);
                out.writeBoolean(p.active != null);
                if (p.active != null) { out.writeUTF(p.active); }
                out.writeBoolean(p.initialPartition != null);
                if (p.initialPartition != null) { out.writeUTF(p.initialPartition); }
                out.writeInt(p.pending.size());
                for (String partition : p.pending) { out.writeUTF(partition); }
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public ContinuousOdpsSplitEnumState deserialize(int version, byte[] bytes) throws IOException {
        if (version < 0 || version > CURRENT_VERSION) {
            throw new IOException(
                    "Unrecognized ContinuousOdpsSplitEnumStateSerializer version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(bais)) {

            int splitNumber = in.readInt();
            List<OdpsSourceSplit> splits = new ArrayList<>(splitNumber);
            for (int i = 0; i < splitNumber; i++) {
                int splitSerializerVersion = in.readInt();
                int byteNumber = in.readInt();
                byte[] serializedSplit = new byte[byteNumber];
                in.readFully(serializedSplit);
                splits.add(splitSerializer.deserialize(splitSerializerVersion, serializedSplit));
            }

            long currentSnapshotId;
            String currentTxnId = null;

            if (version == 0) {
                currentSnapshotId = in.readLong();
            } else if (version >= 1) {
                currentSnapshotId = in.readLong();
                boolean hasCurrentTxnId = in.readBoolean();
                if (hasCurrentTxnId) {
                    currentTxnId = in.readUTF();
                }
            } else {
                throw new IOException("Unexpected version: " + version);
            }

            ContinuousOdpsSplitEnumState state = new ContinuousOdpsSplitEnumState(splits,
                    currentSnapshotId == INVALID_SNAPSHOT ? null : currentSnapshotId,
                    currentTxnId);
            if (version >= 2 && in.readBoolean()) {
                PartitionProgress p =
                        new PartitionProgress();
                p.initialized = in.readBoolean();
                p.initialStart = in.readLong();
                p.cursor = in.readLong();
                p.epoch = in.readLong();
                p.active = in.readBoolean() ? in.readUTF() : null;
                p.initialPartition = in.readBoolean() ? in.readUTF() : null;
                int count = in.readInt();
                if (count < 0) { throw new IOException("Negative partition count"); }
                for (int i = 0; i < count; i++) { p.pending.add(in.readUTF()); }
                state.withPartitionProgress(p);
            }
            return state;
        }
    }
}
