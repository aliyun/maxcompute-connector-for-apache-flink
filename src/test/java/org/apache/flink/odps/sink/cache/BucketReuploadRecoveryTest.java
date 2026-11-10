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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.writer.*;
import org.apache.flink.table.data.*;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Restored checkpoint files replay through the real queue and bucket state machine. */
public class BucketReuploadRecoveryTest {
    @Test(timeout = 10000)
    public void transientReplayFailureClosesReadersAndPublishesNewSessionOnlyAfterSuccess()
            throws Exception {
        exercise(false);
    }

    @Test(timeout = 10000)
    public void exhaustedReplayAbortsWriterAndRetainsCheckpointForAnotherRecovery()
            throws Exception {
        exercise(true);
    }

    private void exercise(boolean permanent) throws Exception {
        AtomicInteger opened = new AtomicInteger(),
                closed = new AtomicInteger(),
                attempts = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger(), resets = new AtomicInteger();
        List<Long> delivered = new ArrayList<>();
        List<String> sessions = new ArrayList<>();
        InProgressFileWriter.CommittingFile<RowData> file =
                new InProgressFileWriter.CommittingFile<RowData>() {
                    public Path getPath() {
                        return new Path("file:/unused-checkpoint-fixture");
                    }

                    public long getSize() {
                        return 24;
                    }

                    public long getRowCount() {
                        return 3;
                    }

                    public void dispose() {}

                    public InProgressFileWriter.FileReader<RowData> getFileReader() {
                        opened.incrementAndGet();
                        return new InProgressFileWriter.FileReader<RowData>() {
                            int cursor;

                            public boolean hasNext() {
                                return cursor < 3;
                            }

                            public RowData next() {
                                return GenericRowData.of((long) ++cursor);
                            }

                            public void close() {
                                closed.incrementAndGet();
                            }
                        };
                    }
                };
        BucketWriter<RowData, String> writer =
                new BucketWriter<RowData, String>() {
                    public InProgressFileWriter<RowData, String> openNewInProgressFile(
                            String id, long time) {
                        throw new AssertionError("must use restored files");
                    }

                    public SimpleVersionedSerializer<InProgressFileWriter.CommittingFile<RowData>>
                            getCommittingFileSerializer() {
                        return null;
                    }

                    public SimpleVersionedSerializer<CommitInfo> getCommitInfoSerializer() {
                        return null;
                    }

                    public DataWriter<RowData> openDataWriter(String id, WriterInfo info) {
                        int attempt = attempts.incrementAndGet();
                        delivered.clear();
                        return new DataWriter<RowData>() {
                            public void addElement(RowData row) throws IOException {
                                if (row.getLong(0) == 2 && (permanent || attempt == 1))
                                    throw new IOException("replay upload failed");
                                delivered.add(row.getLong(0));
                            }

                            public void flush() {}

                            public CommitInfo finish(long checkpoint) {
                                return new PartitionCommitInfo(
                                        checkpoint,
                                        ((PartitionWriterInfo) info).getSessionId(),
                                        id,
                                        delivered.size(),
                                        null);
                            }

                            public void reset() {
                                resets.incrementAndGet();
                                delivered.clear();
                            }

                            public void abort() {
                                aborts.incrementAndGet();
                            }
                        };
                    }
                };
        BucketState<RowData, String> state =
                new BucketState<>(
                        "ds='a'",
                        Collections.singletonMap(7L, Collections.singletonList(file)),
                        Collections.singletonMap(
                                7L, new PartitionCommitInfo(7, "old-session", "ds='a'", 3, null)));
        Bucket<RowData, String> bucket =
                Bucket.restore(
                        0,
                        state,
                        writer,
                        PartitionBucketRollingPolicy.builder().build(),
                        (id, info) -> sessions.add(((PartitionCommitInfo) info).getSessionId()));
        assertEquals(Collections.singletonList("old-session"), sessions);
        assertTrue(bucket.isActive());
        assertFalse(bucket.isCommitted(7));
        assertTrue("Restored pending data must prevent early input completion", bucket.hasUncommittedData());
        WriterThread<String> worker = new WriterThread<String>(Comparator.naturalOrder(), 1);
        bucket.setWriterThread(worker);
        CompletableFuture<IOException> completed = new CompletableFuture<>();
        WriterThread.RequestDoneCallback callback =
                new WriterThread.RequestDoneCallback() {
                    public void requestSuccessful() {
                        completed.complete(null);
                    }

                    public void requestFailed(IOException error) {
                        completed.complete(error);
                    }
                };
        assertFalse(
                bucket.reUploadCommittingFile(
                        99, new PartitionWriterInfo(99, "unused", "ds='a'"), callback));
        assertTrue(
                bucket.reUploadCommittingFile(
                        7, new PartitionWriterInfo(7, "new-session", "ds='a'"), callback));
        worker.start();
        try {
            IOException failure = completed.get(5, TimeUnit.SECONDS);
            assertEquals(2, attempts.get());
            assertEquals(
                    "every attempt must release its checkpoint reader", opened.get(), closed.get());
            assertEquals(permanent ? 2 : 1, resets.get());
            String committed =
                    ((PartitionCommitInfo)
                                    bucket.getBucketState()
                                            .getCommittingInfoPerCheckpoint()
                                            .get(7L))
                            .getSessionId();
            if (permanent) {
                assertNotNull(failure);
                assertEquals(1, aborts.get());
                assertEquals("old-session", committed);
                assertEquals(Collections.singletonList("old-session"), sessions);
                assertFalse(bucket.isCommitted(7));
                assertTrue(bucket.hasUncommittedData());
            } else {
                assertNull(failure);
                assertEquals(0, aborts.get());
                assertEquals(Arrays.asList(1L, 2L, 3L), delivered);
                assertEquals("new-session", committed);
                assertEquals(Arrays.asList("old-session", "new-session"), sessions);
                assertTrue("An upload is not yet a committed session", bucket.hasUncommittedData());
                bucket.setCommitted(7);
                assertTrue(bucket.isCommitted(7));
                assertFalse(bucket.hasUncommittedData());
            }
            assertEquals(
                    Collections.singleton(7L),
                    bucket.getBucketState().getCommittingFilePerCheckpoint().keySet());
        } finally {
            worker.shutdown();
            worker.join(2000);
            assertFalse(worker.isAlive());
        }
    }
}
