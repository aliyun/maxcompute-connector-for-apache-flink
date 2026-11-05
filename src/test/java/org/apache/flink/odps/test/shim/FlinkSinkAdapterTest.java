/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.flink.odps.test.shim;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.odps.shim.FlinkSinkAdapter;
import org.junit.Test;
import static org.junit.Assert.*;

/** Exercises the callbacks invoked by each supported Flink Sink runtime. */
public class FlinkSinkAdapterTest {
    @Test
    public void legacySink2ContextReachesTheSharedWriter() throws Exception {
        AtomicInteger subtask = new AtomicInteger(-1);
        FlinkSinkAdapter<Object> adapter = (index, timers) -> { subtask.set(index); return null; };
        adapter.createWriter((Sink.InitContext) context(Sink.InitContext.class));
        assertEquals(7, subtask.get());
    }

    @Test
    public void modernWriterContextReachesTheSharedWriterWhenAvailable() throws Exception {
        Class<?> context;
        try {
            context = Class.forName("org.apache.flink.api.connector.sink2.WriterInitContext");
        } catch (ClassNotFoundException legacyRuntime) {
            // These runtimes expose only Sink.InitContext, covered by the test above.
            assertTrue(org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.16.")
                    || org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.17."));
            return;
        }
        AtomicInteger subtask = new AtomicInteger(-1);
        FlinkSinkAdapter<Object> adapter = (index, timers) -> { subtask.set(index); return null; };
        Sink.class.getMethod("createWriter", context).invoke(adapter, context(context));
        assertEquals(7, subtask.get());
    }

    private static Object context(Class<?> type) {
        return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (p, method, args) -> {
            if (method.getName().equals("getSubtaskId")) { return 7; }
            if (method.getName().equals("getProcessingTimeService")) { return null; }
            throw new AssertionError("Unexpected runtime dependency: " + method);
        });
    }
}
