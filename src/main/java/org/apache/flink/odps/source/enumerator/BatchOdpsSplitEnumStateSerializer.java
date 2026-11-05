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

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.source.split.OdpsSourceSplit;
import org.apache.flink.odps.source.split.OdpsSourceSplitSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/** Serializer for {@link BatchOdpsSplitEnumState}. */
public class BatchOdpsSplitEnumStateSerializer
        implements SimpleVersionedSerializer<BatchOdpsSplitEnumState> {

    private static final int CURRENT_VERSION = 0;

    private final SimpleVersionedSerializer<OdpsSourceSplit> splitSerializer =
            new OdpsSourceSplitSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(BatchOdpsSplitEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            List<LinkedList<OdpsSourceSplit>> assignedSplits = state.getAssignedSplits();

            out.writeInt(assignedSplits.size());
            for (LinkedList<OdpsSourceSplit> splits : assignedSplits) {
                out.writeInt(splits.size());
                out.writeInt(splitSerializer.getVersion());
                for (OdpsSourceSplit split : splits) {
                    byte[] serializedSplit = splitSerializer.serialize(split);
                    out.writeInt(serializedSplit.length);
                    out.write(serializedSplit);
                }
            }

            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public BatchOdpsSplitEnumState deserialize(int version, byte[] bytes) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unrecognized BatchOdpsSplitEnumStateSerializer version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(bais)) {
            int numReaders = in.readInt();
            List<LinkedList<OdpsSourceSplit>> assignedSplits = new ArrayList<>();
            for (int reader = 0; reader < numReaders; reader++) {
                int numSplits = in.readInt();
                int splitSerializerVersion = in.readInt();
                LinkedList<OdpsSourceSplit> splits = new LinkedList<>();
                for (int i = 0; i < numSplits; i++) {
                    int len = in.readInt();
                    byte[] serializedSplit = new byte[len];
                    in.readFully(serializedSplit);
                    OdpsSourceSplit split =
                            splitSerializer.deserialize(splitSerializerVersion, serializedSplit);
                    splits.add(split);
                }
                assignedSplits.add(splits);
            }

            return new BatchOdpsSplitEnumState(assignedSplits);
        }
    }
}
