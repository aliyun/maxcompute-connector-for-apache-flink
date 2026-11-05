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

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BoundedSdkCallTest {
    @Test(timeout = 5000)
    public void missingSdkCallbackFailsWithoutLeavingAnInterruptOnFlinkThread() throws Exception {
        long started = System.nanoTime();
        IOException failure =
                assertThrows(
                        IOException.class,
                        () -> BoundedSdkCall.run(50, () -> new CountDownLatch(1).await()));
        assertTrue(failure.getMessage().contains("exceeded 50 ms"));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2000);
        assertFalse(Thread.currentThread().isInterrupted());
        BoundedSdkCall.run(50, () -> {});
        Thread.sleep(100);
        assertFalse(
                "cancelled timer interrupted unrelated work",
                Thread.currentThread().isInterrupted());
    }

    @Test
    public void ordinaryFailureAndExternalCancellationRemainVisible() throws Exception {
        IOException original = new IOException("service error");
        assertSame(
                original,
                assertThrows(
                        IOException.class,
                        () ->
                                BoundedSdkCall.run(
                                        1000,
                                        () -> {
                                            throw original;
                                        })));
        try {
            assertThrows(
                    IOException.class,
                    () ->
                            BoundedSdkCall.run(
                                    1000,
                                    () -> {
                                        throw new InterruptedException("cancel");
                                    }));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
