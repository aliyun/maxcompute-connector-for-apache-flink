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

package org.apache.flink.odps.sink.utils;

import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

public class AsyncExecuteThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecuteThread.class);

    protected final LinkedBlockingQueue<ExecuteRequest> compactQueue;
    protected final PriorityBlockingQueue<ExecuteRequest> executeQueue;

    // For bootstrap
    protected final Map<String, String> currentActiveSessions;

    // checkpoint -> partition -> session
    protected final SortedMap<Long, Map<String, String>> successBootstrapRequest;

    // checkpoint -> partition -> session
    protected final SortedMap<Long, Map<String, String>> failedCommitRequest;

    // For task failover
    protected final Map<String, SortedMap<Long, Set<String>>> successCommitRequest;

    private volatile boolean alive;

    public AsyncExecuteThread() {
        this.compactQueue = new LinkedBlockingQueue<>();
        this.alive = true;

        this.currentActiveSessions = new HashMap<>();
        this.successCommitRequest = new HashMap<>();
        this.successBootstrapRequest = new TreeMap<>();
        this.failedCommitRequest = new TreeMap<>();

        this.executeQueue = new PriorityBlockingQueue<>(64,
                (o1, o2) -> {
                    ExecuteRequest.ExecuteType type1 = o1.getExecuteType();
                    ExecuteRequest.ExecuteType type2 = o2.getExecuteType();
                    long cp1 = o1.getCheckpointId();
                    long cp2 = o2.getCheckpointId();

                    if (cp1 == cp2) {
                        if (type1.equals(ExecuteRequest.ExecuteType.RESET)) {
                            return -1;
                        } else if (type2.equals(ExecuteRequest.ExecuteType.RESET)) {
                            return 1;
                        } else {
                            // Snapshot should execute first
                            if (type1.equals(ExecuteRequest.ExecuteType.SNAPSHOT)) {
                                return -1;
                            } else if (type2.equals(ExecuteRequest.ExecuteType.SNAPSHOT)) {
                                return 1;
                            } else {
                                if (type1.equals(ExecuteRequest.ExecuteType.COMMIT)) {
                                    if (type2.equals(ExecuteRequest.ExecuteType.BOOTSTRAP) &&
                                            ((BootstrapRequest)o2).isReUpload()) {
                                        return 1;
                                    }
                                    return -1;
                                } else if (type2.equals(ExecuteRequest.ExecuteType.COMMIT)){
                                    if (type1.equals(ExecuteRequest.ExecuteType.BOOTSTRAP) &&
                                            ((BootstrapRequest)o1).isReUpload()) {
                                        return -1;
                                    }
                                    return 1;
                                } else {
                                    return 0;
                                }
                            }
                        }
                    } else if (cp1 > cp2) {
                        return 1;
                    } else {
                        return -1;
                    }
                });
    }

    public int queueSize() {
        return executeQueue.size() + compactQueue.size();
    }

    public void addRequest(ExecuteRequest request) {
        if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.RESET)) {
            IOException ioex = new IOException("Request has been cancelled.");

            LOG.info("Clear execute queue for reset");
            clearQueue(executeQueue, ioex);
            clearQueue(compactQueue, ioex);

            interrupt();
        }

        if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.BOOTSTRAP)
                || request.getExecuteType().equals(ExecuteRequest.ExecuteType.COMMIT)
                || request.getExecuteType().equals(ExecuteRequest.ExecuteType.SNAPSHOT)
                || request.getExecuteType().equals(ExecuteRequest.ExecuteType.RESET)) {
            this.executeQueue.add(request);
        } else if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.COMPACT)) {
            this.compactQueue.add(request);
        }
    }

    private void clearQueue(Queue<ExecuteRequest> requests,
                            IOException ioex) {
        while (!requests.isEmpty()) {
            ExecuteRequest request = requests.poll();
            if (request != null) {
                try {
                    request.requestDone(ioex);
                } catch (Throwable t) {
                    LOG.error(
                            "The handler of the request complete callback threw an exception"
                                    + (t.getMessage() == null
                                    ? "."
                                    : ": " + t.getMessage()),
                            t);
                }
            }
        }
    }

    private void clearQueue(Queue<ExecuteRequest> requests,
                            IOException ioex,
                            long checkpointId) {
        List<ExecuteRequest> pendingRequest = new LinkedList<>();
        while (!requests.isEmpty()) {
            ExecuteRequest request = requests.poll();
            if (request != null) {
                if (request.getCheckpointId() > checkpointId) {
                    try {
                        request.requestDone(ioex);
                    } catch (Throwable t) {
                        LOG.error(
                                "The handler of the request complete callback threw an exception"
                                        + (t.getMessage() == null
                                        ? "."
                                        : ": " + t.getMessage()),
                                t);
                    }
                } else if (request.getCheckpointId() < checkpointId) {
                    LOG.error("The request checkpoint {} less than {}", request.getCheckpointId(), checkpointId);
                } else {
                    pendingRequest.add(request);
                }
            }
        }
        requests.addAll(pendingRequest);
    }

    public CompletableFuture<Void> reset(long checkpointId) {
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        final ResetRequest resetRequest = new ResetRequest() {

            @Override
            public void execute() throws IOException {
                currentActiveSessions.clear();
                successBootstrapRequest.entrySet().removeIf(entry -> entry.getKey() > checkpointId);
                failedCommitRequest.entrySet().removeIf(entry -> entry.getKey() > checkpointId);

                Iterator<Map.Entry<String, SortedMap<Long, Set<String>>>> iterator =
                        successCommitRequest.entrySet().iterator();
                Map.Entry<String, SortedMap<Long, Set<String>>> entry;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    entry.getValue().entrySet().removeIf(ckp -> ckp.getKey() > checkpointId);
                    if (entry.getValue().isEmpty()) {
                        iterator.remove();
                    }
                }

                if ((!failedCommitRequest.isEmpty() && failedCommitRequest.lastKey() <= checkpointId)
                        || (!successBootstrapRequest.isEmpty() && successBootstrapRequest.lastKey() <= checkpointId)) {
                    LOG.info("Reset to checkpoint {} has uncommitted session", checkpointId);
                } else {
                    // May restart job manager
                    if (successCommitRequest.isEmpty()) {
                        LOG.info("Reset to checkpoint {}, all state is empty", checkpointId);
                    } else {
                        LOG.info("Reset to checkpoint {}, all session committed", checkpointId);
                    }
                }
            }

            @Override
            public void requestDone(IOException ioex) {
                if (ioex == null) completion.complete(null);
                else completion.completeExceptionally(ioex);
            }

            @Override
            public long getCheckpointId() {
                return checkpointId;
            }
        };
        addRequest(resetRequest);
        return completion;
    }

    public void shutdown() {
        synchronized (this) {
            if (alive) {
                alive = false;
                interrupt();
            }

            try {
                join(1000);
            } catch (InterruptedException ignored) {
            }

            currentActiveSessions.clear();
            successBootstrapRequest.clear();
            successCommitRequest.clear();
            failedCommitRequest.clear();

            IOException ioex = new IOException("Execute thread has been closed.");

            clearQueue(executeQueue, ioex);
            clearQueue(compactQueue, ioex);
        }
    }

    @Override
    public void run() {

        while (this.alive) {
            ExecuteRequest request = null;

            while (alive && request == null) {

                try {
                    request = executeQueue.poll(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    if (!this.alive) {
                        return;
                    } else {
                        LOG.warn(
                                Thread.currentThread() + " was interrupted without shutdown.");
                    }
                }

                if (request == null) {
                    if (!executeQueue.isEmpty()) {
                        request = executeQueue.poll();
                    } else if (!compactQueue.isEmpty()) {
                        request = compactQueue.poll();
                    }
                }
            }

            if (request != null) {
                IOException ioex = null;

                final long checkpoint = request.getCheckpointId();

                if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.RESET)) {
                    try {
                        request.execute();
                    } catch (IOException e) {
                        ioex = e;
                    } catch (Throwable t) {
                        ioex = new IOException("Reset failed: " + t.getMessage(), t);
                        LOG.error(
                                "Execute thread encountered an error"
                                        + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                t);
                    }
                    request.requestDone(ioex);

                } else if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.SNAPSHOT)) {
                    SnapshotRequest snapshotRequest = (SnapshotRequest) request;

                    if (snapshotRequest.isStarted()) {

                        if (!failedCommitRequest.isEmpty()
                                && failedCommitRequest.lastKey() < checkpoint) {
                            addRequest(request);
                            continue;
                        }

                        if (!successBootstrapRequest.isEmpty()
                                && successBootstrapRequest.lastKey() < checkpoint) {
                            // should delay to commit latest session
                            addRequest(request);
                            continue;
                        }

                    } else {
                        // move current sessions to successBootstrapRequest
                        if (!currentActiveSessions.isEmpty()) {
                            Map<String, String> requests =
                                    successBootstrapRequest.computeIfAbsent(checkpoint, key -> new HashMap<>());
                            requests.putAll(currentActiveSessions);
                            LOG.info("Put all committing sessions for checkpoint {}", checkpoint);
                            currentActiveSessions.clear();
                        } else {
                            LOG.info("Current active session has been removed for checkpoint {}", checkpoint);
                        }
                    }

                    snapshotRequest.requestDone(null);

                } else if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.BOOTSTRAP)) {

                    BootstrapRequest bootstrapRequest = (BootstrapRequest) request;
                    final String partition = bootstrapRequest.getPartition();

                    boolean create = true;
                    String sessionId = null;

                    if (!bootstrapRequest.isReUpload()) {
                        // Session not be committed
                        if (currentActiveSessions.containsKey(partition)) {
                            sessionId = currentActiveSessions.get(partition);
                            create = false;
                        }
                    }

                    if (create) {
                        try {
                            bootstrapRequest.execute();
                            sessionId = bootstrapRequest.getSessionId();
                        } catch (IOException e) {
                            ioex = e;
                        } catch (Throwable t) {
                            ioex = new IOException("Create failed: " + t.getMessage(), t);
                            LOG.error(
                                    "Execute thread encountered an error"
                                            + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                    t);
                        }

                        if (ioex == null) {
                            if (!bootstrapRequest.isReUpload()) {
                                currentActiveSessions.put(partition, sessionId);
                            } else {
                                LOG.info("Reupload session {}, checkpoint {}, partition {}" , sessionId, checkpoint, partition);
                            }
                        }

                        try {
                            bootstrapRequest.requestDone(ioex, sessionId);
                        } catch (Throwable t) {
                            if (ExceptionUtils.findThrowable(t, InterruptedException.class).isPresent()) {
                                // Means request has been cancelled.
                                LOG.error("Bootstrap request has been interrupted");
                            } else {
                                LOG.error(
                                        "The handler of the request-complete-callback threw an exception"
                                                + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                        t);
                            }
                        }

                    } else {
                        LOG.debug("Session has created {}, partition {}", sessionId, partition);
                    }

                } else if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.COMMIT)) {

                    CommitRequest commitRequest = (CommitRequest) request;
                    final String partition = commitRequest.getPartition();

                    // May be remove it
                    if (!failedCommitRequest.isEmpty() && failedCommitRequest.lastKey() < checkpoint) {
                        Map<String, String> failedPartitionRequest =
                                failedCommitRequest.get(failedCommitRequest.lastKey());
                        if (failedPartitionRequest.containsKey(partition)) {
                            // Latest session does not committed, should delay commit
                            addRequest(commitRequest);
                            continue;
                        }
                    }

                    final String sessionId = commitRequest.getSessionId();

                    boolean committed = false;
                    if (successCommitRequest.containsKey(partition)) {
                        SortedMap<Long, Set<String>> successPartitionRequest = successCommitRequest.get(partition);
                        if (successPartitionRequest.containsKey(checkpoint)) {
                            Set<String> ids = successPartitionRequest.get(checkpoint);
                            // May be reupload session success
                            if (!ids.isEmpty()) {
                                committed = true;
                            }
                        }
                    }

                    if (!committed) {
                        try {
                            commitRequest.execute();
                        } catch (IOException e) {
                            if (ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent()) {
                                // Means request has been cancelled.
                                LOG.error("Commit request has been interrupted");
                                ioex = new IOException("Request has been cancelled.");
                            } else {
                                ioex = e;
                            }
                        } catch (Throwable t) {
                            ioex = new IOException("Commit failed: " + t.getMessage(), t);
                            LOG.error(
                                    "Commit thread encountered an error"
                                            + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                    t);
                        }
                    } else {
                        LOG.info("Session has committed {}, partition {}, checkpoint {}",
                                sessionId, partition, checkpoint);
                    }

                    if (currentActiveSessions.containsKey(partition) &&
                            currentActiveSessions.get(partition).equals(sessionId)) {
                        currentActiveSessions.remove(partition);
                    } else if (successBootstrapRequest.containsKey(checkpoint)) {
                        // For async commit request
                        Map<String, String> bootstrapRequest =  successBootstrapRequest.get(checkpoint);
                        if (bootstrapRequest.containsKey(partition) &&
                                bootstrapRequest.get(partition).equals(sessionId)) {
                            bootstrapRequest.remove(partition);
                        }
                        if (bootstrapRequest.isEmpty()) {
                            // Remove smaller checkpoint
                            LOG.info("Session has been removed for checkpoint {}", checkpoint);
                            successBootstrapRequest.entrySet().removeIf(entry -> entry.getKey() <= checkpoint);
                        }
                    }

                    if (ioex != null) {
                        if (!ioex.getMessage().contains("Request has been cancelled")) {
                            Map<String, String> failedPartitionRequest =
                                    failedCommitRequest.computeIfAbsent(checkpoint, key -> new HashMap<>());
                            failedPartitionRequest.put(partition, sessionId);

                            LOG.info("Add failed commit request for partition {}, checkpoint {}", partition, checkpoint);
                        }
                    } else {
                        if (failedCommitRequest.containsKey(checkpoint)) {
                            Map<String, String> failedPartitionRequest = failedCommitRequest.get(checkpoint);
                            if (failedPartitionRequest.containsKey(partition)) {
                                LOG.info("Failed commit request for partition {} has been removed, checkpoint {}",
                                        partition, checkpoint);
                                failedPartitionRequest.remove(partition);
                            }

                            if (failedPartitionRequest.isEmpty()) {
                                failedCommitRequest.entrySet().removeIf(entry -> entry.getKey() <= checkpoint);
                            }
                        }

                        SortedMap<Long, Set<String>> successPartitionRequest =
                                successCommitRequest.computeIfAbsent(partition, key -> new TreeMap<>());
                        Set<String> successSessions =
                                successPartitionRequest.computeIfAbsent(checkpoint, key -> new TreeSet<>());
                        successSessions.add(sessionId);
                    }

                    try {
                        request.requestDone(ioex);
                    } catch (Throwable t) {
                        LOG.error(
                                "The handler of the request-complete-callback threw an exception"
                                        + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                t);
                    }
                } else {

                    try {
                        request.execute();
                    } catch (IOException e) {
                        ioex = e;
                    } catch (Throwable t) {
                        ioex = new IOException("Compact failed: " + t.getMessage(), t);
                        LOG.error(
                                "Execute thread encountered an error"
                                        + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                t);
                    }
                    try {
                        request.requestDone(ioex);
                    } catch (Throwable t) {
                        LOG.error(
                                "The handler of the request-complete-callback threw an exception"
                                        + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                                t);
                    }
                }
            }
        }
    }

    public interface ExecuteRequest {

        void execute() throws IOException;

        void requestDone(IOException ioex);

        ExecuteType getExecuteType();

        long getCheckpointId();

        enum ExecuteType {
            BOOTSTRAP,
            COMMIT,
            COMPACT,
            SNAPSHOT,
            RESET
        }
    }

    public interface SessionRequest extends ExecuteRequest{

        String getPartition();

        String getSessionId();
    }

    public interface SnapshotRequest extends ExecuteRequest {

        @Override
        default ExecuteType getExecuteType() {
            return ExecuteType.SNAPSHOT;
        }

        boolean isStarted();
    }

    public interface ResetRequest extends ExecuteRequest {

        @Override
        default ExecuteType getExecuteType() {
            return ExecuteType.RESET;
        }
    }

    public interface CommitRequest extends SessionRequest {

        @Override
        default ExecuteType getExecuteType() {
            return ExecuteType.COMMIT;
        }
    }

    public interface CompactRequest extends SessionRequest {

        @Override
        default ExecuteType getExecuteType() {
            return ExecuteType.COMPACT;
        }
    }

    public interface BootstrapRequest extends SessionRequest {
        /**
         * Method that is called by the target I/O thread after the request has been processed.
         */
        void requestDone(IOException ioex, String sessionId);

        @Override
        default void requestDone(IOException ioex) {
            requestDone(ioex, null);
        }

        @Override
        default ExecuteType getExecuteType() {
            return ExecuteType.BOOTSTRAP;
        }

        boolean isReUpload();

        boolean isCreatePartition();

        void createPartition(boolean createPartition);
    }

}
