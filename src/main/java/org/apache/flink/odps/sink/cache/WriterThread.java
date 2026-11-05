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

package org.apache.flink.odps.sink.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

public class WriterThread<BucketID> extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(WriterThread.class);

    protected final Map<BucketID, List<WriteRequest<BucketID>>> waitingQueue;
    protected final PriorityBlockingQueue<WriteRequest<BucketID>> writeQueue;

    private volatile boolean alive;

    private final int writeRetryTimes;

    public WriterThread(Comparator<BucketID> comparator, int writeRetryTimes) {
        this.waitingQueue = new HashMap<>();
        this.writeQueue = new PriorityBlockingQueue<>(64,
                (o1, o2) -> {
                    if (comparator.compare(o1.getBucketId(), o2.getBucketId()) == 0) {
                        return o1.partNumber() - o2.partNumber();
                    } else {
                        return comparator.compare(o1.getBucketId(), o2.getBucketId());
                    }
                });
        this.alive = true;
        this.writeRetryTimes = writeRetryTimes;
    }

    public void addPendingRequest(WriteRequest<BucketID> request) {
        this.waitingQueue.computeIfAbsent(request.getBucketId(),
                r -> new ArrayList<>()).add(request);
    }

    public void movePendingRequest(BucketID bucketID) {
        if (waitingQueue.containsKey(bucketID)) {
            List<WriteRequest<BucketID>> requests = waitingQueue.remove(bucketID);
            requests.forEach(this::addWriteRequest);
            requests.clear();
        }
    }

    public void addWriteRequest(WriteRequest<BucketID> request) {
        this.writeQueue.add(request);
    }

    protected void shutdown() {
        synchronized (this) {
            if (alive) {
                alive = false;
                interrupt();
            }

            IOException ioex = new IOException("WriterThread has been closed.");

            waitingQueue.forEach(((bucketID, writeRequests) -> {
                writeRequests.forEach(request -> {
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
                });
                writeRequests.clear();
            }));
            waitingQueue.clear();

            while (!this.writeQueue.isEmpty()) {
                WriteRequest<BucketID> request = this.writeQueue.poll();
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
    }

    @Override
    public void run() {

        while (this.alive) {

            WriteRequest<BucketID> request = null;

            while (alive && request == null) {
                try {
                    request = writeQueue.take();
                } catch (InterruptedException e) {
                    if (!this.alive) {
                        return;
                    } else {
                        LOG.warn(
                                Thread.currentThread() + " was interrupted without shutdown.");
                    }
                }
            }

            IOException ioex = null;

            if (request != null) {
                int retry = 0;
                long sleep = 1000;

                while (true) {
                    ioex = null;

                    if (!alive) {
                        ioex = new IOException("WriterThread has been closed.");
                        break;
                    }

                    try {
                        request.write();
                    } catch (IOException e) {
                        ioex = e;
                    } catch (Throwable t) {
                        ioex = new IOException("The buffer could not be written: " + t.getMessage(), t);
                    }

                    if (!alive) {
                        ioex = new IOException("WriterThread has been closed.");
                        break;
                    }

                    if (ioex != null
                            && ioex.getMessage() != null
                            && !ioex.getMessage().contains("WriterThread has been closed")
                            && !ioex.getMessage().contains("writer has been finished or aborted")
                            && !ioex.getMessage().contains("data partition already released")) {
                        retry++;
                        LOG.error(
                                "I/O writing thread encountered an error"
                                        + (ioex.getMessage() == null ? "." : ": " + ioex.getMessage())
                                        + ", retry times: "
                                        + retry, ioex);
                        request.reset();

                        if (retry > writeRetryTimes) {
                            break;
                        }
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException ex) {
                            break;
                        }
                        sleep = sleep * 2;
                    } else {
                        break;
                    }
                }

                // invoke the processed buffer handler of the request issuing writer object
                try {
                    request.requestDone(ioex);
                } catch (Throwable t) {
                    LOG.error(
                            "The handler of the request-complete-callback threw an exception"
                                    + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                            t);
                }
            }
        } // end while alive
    }

    public interface WriteRequest<BucketID> {

        /**
         * Called by the target I/O thread to perform the actual writing operation.
         */
        void write() throws IOException;

        /**
         * Method that is called by the target I/O thread after the request has been processed.
         */
        void requestDone(IOException ioex);


        BucketID getBucketId();

        int partNumber();

        void reset();
    }

    public interface RequestDoneCallback {

        void requestSuccessful();

        void requestFailed(IOException e);
    }

}
