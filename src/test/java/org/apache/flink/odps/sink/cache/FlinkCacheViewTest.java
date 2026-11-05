/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.flink.odps.sink.cache;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.odps.shim.FlinkSubpartitionView;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

/** Real local-cache data and end events through each runtime's availability contract. */
public class FlinkCacheViewTest {
    @Rule public TemporaryFolder files = new TemporaryFolder();

    @Test
    public void bufferedViewReadsDataWithCreditAndEndEventWithoutCredit() throws Exception {
        File file = new File(files.getRoot(), "partition.bin");
        SubPartition partition = SubPartition.createWithFileChannel(file, 4096, true);
        ResultSubpartitionView view = null;
        try {
            try (BufferBuilder builder = new BufferBuilder(
                    MemorySegmentFactory.allocateUnpooledSegment(1024), FreeingBufferRecycler.INSTANCE)) {
                org.apache.flink.runtime.io.network.buffer.BufferConsumer consumer = builder.createBufferConsumer();
                builder.appendAndCommit(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
                builder.finish();
                partition.add(consumer, 0);
            }
            partition.finish();
            view = partition.createReadView(FlinkSubpartitionView.noOpListener());
            assertTrue(view instanceof PartitionBufferReader);
            assertFalse(available(view, false));
            assertTrue(available(view, true));
            Buffer data = view.getNextBuffer().buffer();
            try {
                byte[] actual = new byte[data.getSize()];
                data.getNioBufferReadable().get(actual);
                assertArrayEquals(new byte[]{1, 2, 3, 4}, actual);
            } finally {
                data.recycleBuffer();
            }
            assertTrue("control events must not wait for a data credit", available(view, false));
            Buffer end = view.getNextBuffer().buffer();
            try { assertFalse(end.isBuffer()); } finally { end.recycleBuffer(); }
            assertNull(view.getNextBuffer());
        } finally {
            if (view != null) { view.releaseAllResources(); }
            partition.release();
        }
        assertFalse("cache ownership must close and remove the partition file", file.exists());
    }

    private static boolean available(ResultSubpartitionView view, boolean credit) throws Exception {
        Class<?> parameter = org.apache.flink.runtime.util.EnvironmentInformation.getVersion().startsWith("1.20.")
                ? boolean.class : int.class;
        Object value = parameter == boolean.class ? credit : (Object) (credit ? 1 : 0);
        Object availability = ResultSubpartitionView.class.getMethod("getAvailabilityAndBacklog", parameter)
                .invoke(view, value);
        return ((ResultSubpartitionView.AvailabilityWithBacklog) availability).isAvailable();
    }
}
