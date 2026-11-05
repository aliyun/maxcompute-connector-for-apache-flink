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
 */

package org.apache.flink.odps.test.util;

import static org.junit.Assert.*;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.cache.*;
import org.apache.flink.odps.sink.writer.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

public class BucketCheckpointContractTest {
    @Test
    public void writerSessionStatePreservesUnicodeAndRejectsWrongVersion() throws Exception {
        PartitionWriterInfo.PartitionWriterInfoSerializer serializer =
                PartitionWriterInfo.PartitionWriterInfoSerializer.INSTANCE;
        PartitionWriterInfo info = new PartitionWriterInfo(7, "session-中文", "ds='中文'");
        byte[] bytes = serializer.serialize(info);
        PartitionWriterInfo restored =
                (PartitionWriterInfo) serializer.deserialize(serializer.getVersion(), bytes);
        assertEquals(info.getSessionId(), restored.getSessionId());
        assertEquals(info.getCheckpointId(), restored.getCheckpointId());
        assertEquals(info.getPartitionSpec(), restored.getPartitionSpec());
        assertThrows(IOException.class, () -> serializer.deserialize(99, bytes));
        bytes[0] ^= 1;
        assertThrows(IOException.class, () -> serializer.deserialize(1, bytes));
    }

    @Test
    public void restoresPendingFilesAndCommitMetadataAcrossCheckpoints() throws Exception {
        java.nio.file.Path file = Files.createTempFile("odps-spill-state-", ".bin");
        RowDataSerializer rows = new RowDataSerializer(RowType.of(new BigIntType()));
        PartitionInProgressFileWriter.PartitionCommittingFileSerializer files =
                new PartitionInProgressFileWriter.PartitionCommittingFileSerializer(
                        rows, file.getParent().toString(), 32768);
        SimpleVersionedSerializer<String> ids =
                new SimpleVersionedSerializer<String>() {
                    public int getVersion() {
                        return 1;
                    }

                    public byte[] serialize(String s) {
                        return s.getBytes(StandardCharsets.UTF_8);
                    }

                    public String deserialize(int version, byte[] bytes) throws IOException {
                        if (version != 1) throw new IOException("version");
                        return new String(bytes, StandardCharsets.UTF_8);
                    }
                };
        BucketStateSerializer<RowData, String> codec =
                new BucketStateSerializer<>(
                        ids, PartitionCommitInfo.PartitionCommitInfoSerializer.INSTANCE, files);
        InProgressFileWriter.CommittingFile<RowData> pending =
                new PartitionInProgressFileWriter.PartitionCommittingFile(
                        new Path(file.toUri()),
                        0,
                        0,
                        SubPartition.createForRead(file.toFile(), 32768, false),
                        rows,
                        file.getParent().toString());
        Map<Long, List<InProgressFileWriter.CommittingFile<RowData>>> byCheckpoint =
                new TreeMap<>();
        byCheckpoint.put(7L, Collections.singletonList(pending));
        byCheckpoint.put(8L, Collections.emptyList());
        Map<Long, CommitInfo> info = new TreeMap<>();
        info.put(7L, new PartitionCommitInfo(7, "session-中文", "ds='2026-10-12'", 9, null));
        byte[] bytes = codec.serialize(new BucketState<>("ds='2026-10-12'", byCheckpoint, info));
        BucketState<RowData, String> restored = codec.deserialize(codec.getVersion(), bytes);
        assertEquals("ds='2026-10-12'", restored.getBucketId());
        assertEquals(byCheckpoint.keySet(), restored.getCommittingFilePerCheckpoint().keySet());
        InProgressFileWriter.CommittingFile<RowData> restoredFile =
                restored.getCommittingFilePerCheckpoint().get(7L).get(0);
        assertEquals(pending.getPath(), restoredFile.getPath());
        assertEquals(0, restoredFile.getRowCount());
        PartitionCommitInfo commit =
                (PartitionCommitInfo) restored.getCommittingInfoPerCheckpoint().get(7L);
        assertEquals("session-中文", commit.getSessionId());
        assertEquals(9, commit.getTotalRecords());
        assertEquals(7, commit.getCheckpointId());
        assertThrows(IOException.class, () -> codec.deserialize(99, bytes));
        byte[] corrupt = bytes.clone();
        corrupt[0] ^= 1;
        assertThrows(IOException.class, () -> codec.deserialize(1, corrupt));
        assertThrows(
                IOException.class,
                () -> codec.deserialize(1, Arrays.copyOf(bytes, bytes.length - 1)));
        restoredFile.dispose();
        pending.dispose();
        Files.deleteIfExists(file);
    }
}
