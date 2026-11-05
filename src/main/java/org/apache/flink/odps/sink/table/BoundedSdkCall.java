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

package org.apache.flink.odps.sink.table;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Bounds interruptible SDK waits even when a closed connection loses its callback. */
@org.apache.flink.annotation.Internal
public final class BoundedSdkCall {
    private static final ScheduledThreadPoolExecutor DEADLINES = deadlines();

    private static ScheduledThreadPoolExecutor deadlines() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread = new Thread(task, "maxcompute-sdk-deadlines");
                            thread.setDaemon(true);
                            return thread;
                        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public interface Action {
        void run() throws Exception;
    }

    public static void run(long timeoutMillis, Action action) throws IOException {
        Thread caller = Thread.currentThread();
        // Synchronize completion and interruption so a cancelled timer cannot interrupt
        // the caller after it has returned to unrelated Flink work.
        boolean[] state = {true, false};
        ScheduledFuture<?> deadline =
                DEADLINES.schedule(
                        () -> {
                            synchronized (state) {
                                if (state[0]) {
                                    state[1] = true;
                                    caller.interrupt();
                                }
                            }
                        },
                        timeoutMillis,
                        TimeUnit.MILLISECONDS);
        Exception failure = null;
        boolean timedOut;
        try {
            action.run();
        } catch (Exception error) {
            failure = error;
        } finally {
            synchronized (state) {
                state[0] = false;
                timedOut = state[1];
                if (timedOut) Thread.interrupted();
            }
            deadline.cancel(false);
        }
        if (timedOut)
            throw new IOException(
                    "Upsert SDK operation exceeded " + timeoutMillis + " ms", failure);
        if (failure != null) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                if (cause instanceof InterruptedException) {
                    caller.interrupt();
                    break;
                }
            }
            if (failure instanceof IOException) throw (IOException) failure;
            throw new IOException(failure);
        }
    }
}
