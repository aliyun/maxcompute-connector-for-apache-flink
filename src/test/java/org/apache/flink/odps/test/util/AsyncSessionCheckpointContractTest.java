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

import org.apache.flink.odps.sink.utils.AsyncExecuteThread;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercise actual asynchronous queue ordering with controlled service failures. */
public class AsyncSessionCheckpointContractTest {
    @Test(timeout = 15000)
    public void failedCommitCanBeRetriedAndDuplicateCommitIsIdempotent() throws Exception {
        AsyncExecuteThread worker = new AsyncExecuteThread();
        worker.start();
        try {
            Bootstrap bootstrap = new Bootstrap("s1", false);
            worker.addRequest(bootstrap);
            assertEquals("s1", bootstrap.result.get(5, TimeUnit.SECONDS));
            Snapshot sealed = new Snapshot(false);
            worker.addRequest(sealed);
            sealed.done.get(5, TimeUnit.SECONDS);
            Commit failed = new Commit("s1", new IOException("transient commit"));
            worker.addRequest(failed);
            assertTrue(failed.done.get(5, TimeUnit.SECONDS) instanceof IOException);
            Commit recovered = new Commit("s2", null);
            worker.addRequest(recovered);
            assertNull(recovered.done.get(5, TimeUnit.SECONDS));
            assertEquals(1, recovered.executions.get());
            Commit duplicate = new Commit("s2", null);
            worker.addRequest(duplicate);
            assertNull(duplicate.done.get(5, TimeUnit.SECONDS));
            assertEquals(0, duplicate.executions.get());
            Snapshot complete = new Snapshot(true);
            worker.addRequest(complete);
            complete.done.get(5, TimeUnit.SECONDS);
            worker.reset(1).get(5, TimeUnit.SECONDS);
            Bootstrap resumed = new Bootstrap("s3", true);
            worker.addRequest(resumed);
            assertEquals("s3", resumed.result.get(5, TimeUnit.SECONDS));
        } finally {
            worker.shutdown();
            worker.join(5000);
            assertFalse(worker.isAlive());
        }
    }

    @Test(timeout = 15000)
    public void failedBootstrapDoesNotPoisonNextRequestAndResetCancelsQueuedWork()
            throws Exception {
        AsyncExecuteThread worker = new AsyncExecuteThread();
        Bootstrap cancelled = new Bootstrap("never", false);
        worker.addRequest(cancelled);
        CompletableFuture<Void> reset = worker.reset(0);
        assertTrue(cancelled.result.isCompletedExceptionally());
        worker.start();
        try {
            reset.get(5, TimeUnit.SECONDS);
            Bootstrap failed =
                    new Bootstrap("s1", false) {
                        public void execute() throws IOException {
                            throw new IOException("service unavailable");
                        }
                    };
            worker.addRequest(failed);
            assertThrows(ExecutionException.class, () -> failed.result.get(5, TimeUnit.SECONDS));
            Bootstrap success = new Bootstrap("s2", false);
            worker.addRequest(success);
            assertEquals("s2", success.result.get(5, TimeUnit.SECONDS));
        } finally {
            worker.shutdown();
            worker.join(5000);
            assertFalse(worker.isAlive());
        }
    }

    static class Bootstrap implements AsyncExecuteThread.BootstrapRequest {
        final String session;
        final boolean reupload;
        final CompletableFuture<String> result = new CompletableFuture<>();

        Bootstrap(String session, boolean reupload) {
            this.session = session;
            this.reupload = reupload;
        }

        public void execute() throws IOException {}

        public void requestDone(IOException error, String session) {
            if (error == null) result.complete(session);
            else result.completeExceptionally(error);
        }

        public long getCheckpointId() {
            return 1;
        }

        public String getPartition() {
            return "ds=a";
        }

        public String getSessionId() {
            return session;
        }

        public boolean isReUpload() {
            return reupload;
        }

        public boolean isCreatePartition() {
            return false;
        }

        public void createPartition(boolean value) {}
    }

    static class Commit implements AsyncExecuteThread.CommitRequest {
        final String session;
        final IOException failure;
        final AtomicInteger executions = new AtomicInteger();
        final CompletableFuture<IOException> done = new CompletableFuture<>();

        Commit(String session, IOException failure) {
            this.session = session;
            this.failure = failure;
        }

        public void execute() throws IOException {
            executions.incrementAndGet();
            if (failure != null) throw failure;
        }

        public void requestDone(IOException error) {
            done.complete(error);
        }

        public long getCheckpointId() {
            return 1;
        }

        public String getPartition() {
            return "ds=a";
        }

        public String getSessionId() {
            return session;
        }
    }

    static class Snapshot implements AsyncExecuteThread.SnapshotRequest {
        final boolean started;
        final CompletableFuture<IOException> done = new CompletableFuture<>();

        Snapshot(boolean started) {
            this.started = started;
        }

        public void execute() {}

        public void requestDone(IOException error) {
            done.complete(error);
        }

        public long getCheckpointId() {
            return 1;
        }

        public boolean isStarted() {
            return started;
        }
    }
}
