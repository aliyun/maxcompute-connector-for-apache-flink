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

import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real cache worker retries, queue ordering and shutdown callbacks with controlled I/O failures.
 */
public class WriterQueueFaultContractTest {
    @Test(timeout = 10000)
    public void transientWriteRetriesAndCompletesExactlyOnce() throws Exception {
        WriterThread<String> worker = new WriterThread<String>(Comparator.naturalOrder(), 1);
        AtomicInteger resets = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        CompletableFuture<IOException> result = new CompletableFuture<>();
        worker.addWriteRequest(
                new Request("a", 1) {
                    public void write() throws IOException {
                        if (writes.incrementAndGet() == 1) throw new IOException("transient");
                    }

                    public void reset() {
                        resets.incrementAndGet();
                    }

                    public void requestDone(IOException error) {
                        result.complete(error);
                    }
                });
        worker.start();
        try {
            assertNull(result.get(5, TimeUnit.SECONDS));
            assertEquals(2, writes.get());
            assertEquals(1, resets.get());
        } finally {
            worker.shutdown();
            worker.join(5000);
            assertFalse(worker.isAlive());
        }
    }

    @Test(timeout = 10000)
    public void pendingBucketsDrainInOrderAndShutdownCancelsUnstartedWrites() throws Exception {
        WriterThread<String> worker = new WriterThread<String>(Comparator.naturalOrder(), 0);
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(3);
        for (String key : Arrays.asList("b2", "a2", "a1"))
            worker.addPendingRequest(
                    new Request(key.substring(0, 1), Integer.parseInt(key.substring(1))) {
                        public void write() {
                            order.add(key);
                        }

                        public void requestDone(IOException error) {
                            assertNull(error);
                            done.countDown();
                        }
                    });
        worker.movePendingRequest("missing");
        worker.movePendingRequest("a");
        worker.movePendingRequest("b");
        worker.start();
        try {
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(Arrays.asList("a1", "a2", "b2"), order);
        } finally {
            worker.shutdown();
            worker.join(5000);
        }
        WriterThread<String> closed = new WriterThread<String>(Comparator.naturalOrder(), 0);
        List<IOException> errors = new ArrayList<>();
        for (boolean pending : new boolean[] {false, true}) {
            Request request =
                    new Request("a", 1) {
                        public void requestDone(IOException error) {
                            errors.add(error);
                        }
                    };
            if (pending) closed.addPendingRequest(request);
            else closed.addWriteRequest(request);
        }
        closed.shutdown();
        assertEquals(2, errors.size());
        for (IOException error : errors) assertTrue(error.getMessage().contains("closed"));
    }

    @Test(timeout = 10000)
    public void exhaustedAndTerminalErrorsReachCallerAndWorkerSurvivesBadCallback()
            throws Exception {
        WriterThread<String> worker = new WriterThread<String>(Comparator.naturalOrder(), 0);
        worker.start();
        try {
            for (String message :
                    Arrays.asList("permanent", "writer has been finished or aborted")) {
                CompletableFuture<IOException> result = new CompletableFuture<>();
                AtomicInteger resets = new AtomicInteger();
                worker.addWriteRequest(
                        new Request("a", 1) {
                            public void write() throws IOException {
                                throw new IOException(message);
                            }

                            public void reset() {
                                resets.incrementAndGet();
                            }

                            public void requestDone(IOException error) {
                                result.complete(error);
                            }
                        });
                assertEquals(message, result.get(5, TimeUnit.SECONDS).getMessage());
                assertEquals(message.equals("permanent") ? 1 : 0, resets.get());
            }
            worker.addWriteRequest(
                    new Request("a", 1) {
                        public void requestDone(IOException error) {
                            throw new IllegalStateException("bad callback");
                        }
                    });
            CompletableFuture<IOException> result = new CompletableFuture<>();
            worker.addWriteRequest(
                    new Request("b", 1) {
                        public void requestDone(IOException error) {
                            result.complete(error);
                        }
                    });
            assertNull(result.get(5, TimeUnit.SECONDS));
        } finally {
            worker.shutdown();
            worker.join(5000);
            assertFalse(worker.isAlive());
        }
    }

    static class Request implements WriterThread.WriteRequest<String> {
        final String bucket;
        final int part;

        Request(String bucket, int part) {
            this.bucket = bucket;
            this.part = part;
        }

        public void write() throws IOException {}

        public void requestDone(IOException error) {}

        public String getBucketId() {
            return bucket;
        }

        public int partNumber() {
            return part;
        }

        public void reset() {}
    }
}
