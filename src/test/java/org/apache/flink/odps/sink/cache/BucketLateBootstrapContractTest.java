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

package org.apache.flink.odps.sink.cache;

import static org.junit.Assert.*;
import static org.apache.flink.odps.table.OdpsOptions.SINK_THREAD_NUM;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.writer.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** A bootstrap arriving after flush must not submit that checkpoint twice. */
public class BucketLateBootstrapContractTest {
    @Test(timeout = 10000)
    public void lateBootstrapPreservesCompletedUploadAndStillCommitsNewEmptyBucket() throws Exception {
        Map<String, List<String>> commits = new ConcurrentHashMap<>();
        BucketWriter<RowData, String> writer = new BucketWriter<RowData, String>() {
            public InProgressFileWriter<RowData, String> openNewInProgressFile(String id, long time) {
                throw new AssertionError("this journey uses empty buckets");
            }
            public DataWriter<RowData> openDataWriter(String id, WriterInfo info) {
                return new DataWriter<RowData>() {
                    public void addElement(RowData row) { throw new AssertionError(); }
                    public void flush() {}
                    public CommitInfo finish(long checkpoint) {
                        String session = ((PartitionWriterInfo) info).getSessionId();
                        commits.computeIfAbsent(id, ignored -> new ArrayList<>()).add(session);
                        return new PartitionCommitInfo(checkpoint, session, id, 0, null);
                    }
                    public void reset() {}
                    public void abort() {}
                };
            }
            public SimpleVersionedSerializer<CommitInfo> getCommitInfoSerializer() {
                return PartitionCommitInfo.PartitionCommitInfoSerializer.INSTANCE;
            }
            public SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<RowData>> getCommittingFileSerializer() {
                return new SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<RowData>>() {
                    public int getVersion() { return 1; }
                    public byte[] serialize(InProgressFileWriter.CommittingFile<RowData> value) { throw new AssertionError(); }
                    public InProgressFileWriter.CommittingFile<RowData> deserialize(int version, byte[] bytes) { throw new AssertionError(); }
                };
            }
        };
        BucketAssigner<RowData, String> assigner = new BucketAssigner<RowData, String>() {
            public String getBucketId(RowData row, Context context) { return "existing"; }
            public SimpleVersionedSerializer<String> getSerializer() { return SimpleVersionedStringSerializer.INSTANCE; }
        };
        ProcessingTimeService clock = (ProcessingTimeService) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {ProcessingTimeService.class},
                (proxy, method, args) -> method.getName().equals("getCurrentProcessingTime") ? 0L : null);
        Configuration config = new Configuration();
        config.set(SINK_THREAD_NUM, 1);
        Buckets<RowData, String> buckets = new Buckets<>(config, writer, assigner,
                PartitionBucketRollingPolicy.builder().build(), 0, clock, Comparator.naturalOrder());
        try {
            buckets.bootstrapForBucket("existing", new PartitionWriterInfo(1, "first", "existing"));
            buckets.flush(3);
            buckets.bootstrapForBucket("existing", new PartitionWriterInfo(1, "late", "existing"));
            buckets.bootstrapForBucket("new-empty", new PartitionWriterInfo(1, "empty", "new-empty"));
            buckets.addEmptyBuckets(3);
            assertEquals(Collections.singletonList("first"), commits.get("existing"));
            assertEquals(Collections.singletonList("empty"), commits.get("new-empty"));
            assertFalse(buckets.isCommittedForCheckpoint(3));
            buckets.setCommittedForBucket("existing", 3);
            buckets.setCommittedForBucket("new-empty", 3);
            assertTrue(buckets.isCommittedForCheckpoint(3));
        } finally {
            buckets.close();
        }
    }
}
