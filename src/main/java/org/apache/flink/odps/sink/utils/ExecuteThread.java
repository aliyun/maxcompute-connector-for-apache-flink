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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ExecuteThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteThread.class);

    protected final LinkedBlockingQueue<ExecuteRequest> commitQueue;
    protected final LinkedBlockingQueue<ExecuteRequest> compactQueue;

    protected final LinkedBlockingQueue<ExecuteRequest> requestQueue;

    private volatile boolean alive;

    public ExecuteThread(LinkedBlockingQueue<ExecuteRequest> requestQueue) {
        this.requestQueue = requestQueue;
        this.commitQueue = new LinkedBlockingQueue<>();
        this.compactQueue = new LinkedBlockingQueue<>();
        this.alive = true;
    }

    public void addRequest(ExecuteRequest request) {
        if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.COMMIT)) {
            this.commitQueue.add(request);
        } else if (request.getExecuteType().equals(ExecuteRequest.ExecuteType.COMPACT)) {
            this.compactQueue.add(request);
        }
    }

    private void clearQueue(LinkedBlockingQueue<ExecuteRequest> requests,
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

    public void cancel() {
        synchronized (this) {
            IOException ioex = new IOException("Request has been cancelled.");
            clearQueue(commitQueue, ioex);
            clearQueue(compactQueue, ioex);

            interrupt();
        }
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

            IOException ioex = new IOException("Execute thread has been closed.");

            clearQueue(commitQueue, ioex);
            clearQueue(compactQueue, ioex);
        }
    }

    @Override
    public void run() {

        while (this.alive) {
            ExecuteRequest request = null;

            while (alive && request == null) {

                if (!commitQueue.isEmpty()) {
                    request = commitQueue.poll();
                }

                if (request == null) {
                    try {
                        ExecuteRequest newReq = requestQueue.poll(1000, TimeUnit.MILLISECONDS);

                        if (newReq != null) {
                            addRequest(newReq);
                        }
                    } catch (InterruptedException e) {
                        if (!this.alive) {
                            return;
                        } else {
                            LOG.warn(
                                    Thread.currentThread() + " was interrupted without shutdown.");
                        }
                    }
                }

                if (request == null) {
                    if (!commitQueue.isEmpty()) {
                        request = commitQueue.poll();
                    } else if (!compactQueue.isEmpty()) {
                        request = compactQueue.poll();
                    }
                }
            }

            if (request != null) {
                IOException ioex = null;

                try {
                    request.execute();
                } catch (IOException e) {
                    ioex = e;
                } catch (Throwable t) {
                    ioex = new IOException("Execute failed : " + t.getMessage(), t);
                    LOG.error(
                            "I/O writing thread encountered an error"
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

    public interface ExecuteRequest {

        /**
         * Called by the target I/O thread to perform the actual operation.
         */
        void execute() throws IOException;

        /**
         * Method that is called by the target I/O thread after the request has been processed.
         */
        void requestDone(IOException ioex);


        ExecuteType getExecuteType();

        enum ExecuteType {
            COMMIT,
            COMPACT
        }
    }
}
