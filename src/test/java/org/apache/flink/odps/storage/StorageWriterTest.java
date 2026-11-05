/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.flink.odps.storage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

/** Exercises the same upload lifecycle under every target runtime without a cloud identity. */
public class StorageWriterTest {
    private static final class Harness implements StorageWriter.Encoder<Integer>, StorageWriter.Upload {
        final List<String> events = new ArrayList<>();
        int rows;
        boolean pending;
        long cached;
        boolean failFlush;
        boolean failClose;
        boolean committed;
        public void write(Integer row) { rows++; events.add("row:" + row); }
        public VectorSchemaRoot finishBatch() { events.add("finish:" + rows); return null; }
        public void reset() { events.add("reset"); rows = 0; }
        public void writeBatch(VectorSchemaRoot batch) { events.add("upload:" + rows); cached += rows; }
        public void flush() throws IOException {
            events.add("flush");
            if (failFlush) { throw new IOException("upload failed"); }
            cached = 0;
            pending = false;
        }
        public boolean hasPendingFlush() { return pending; }
        public long cachedSize() { return cached; }
        public void flushAsync() { events.add("async"); pending = true; }
        public void closeWriter() { events.add("writer.close"); }
        public void close() { events.add("encoder.close"); }
        public void close(boolean commit) throws Exception {
            committed = commit;
            events.add("upload.close:" + commit);
            if (failClose) { throw new IOException("close failed"); }
        }
        StorageWriter<Integer> writer() { return new StorageWriter<>(this, this, 2); }
    }

    @Test
    public void batchesAndCheckpointFlushPreserveAllRows() throws Exception {
        Harness h = new Harness();
        StorageWriter<Integer> writer = h.writer();
        writer.write(1); writer.write(2); writer.write(3);
        writer.flush();
        assertEquals(Arrays.asList("row:1", "row:2", "finish:2", "upload:2", "reset",
                "row:3", "finish:1", "upload:1", "reset", "flush"), h.events);
        writer.close();
        assertTrue(h.committed);
    }

    @Test
    public void timerDefersWithoutDrainingWhileUploadIsPending() throws Exception {
        Harness h = new Harness();
        StorageWriter<Integer> writer = h.writer();
        writer.write(1); h.pending = true;
        assertFalse(writer.tryFlushAsync());
        assertEquals(Arrays.asList("row:1"), h.events);
        h.pending = false;
        assertTrue(writer.tryFlushAsync());
        assertEquals(Arrays.asList("row:1", "finish:1", "upload:1", "reset", "async"), h.events);
        writer.close();
    }

    @Test
    public void idleTimerDoesNotUploadEmptyBatches() throws Exception {
        Harness h = new Harness();
        StorageWriter<Integer> writer = h.writer();
        assertTrue(writer.tryFlushAsync());
        assertTrue(h.events.isEmpty());
        writer.close();
    }

    @Test
    public void failedFlushAbortsAndStillClosesBothResources() {
        Harness h = new Harness();
        StorageWriter<Integer> writer = h.writer();
        writer.write(1); h.failFlush = true;
        IOException failure = assertThrows(IOException.class, writer::close);
        assertEquals("upload failed", failure.getMessage());
        assertFalse(h.committed);
        assertEquals("encoder.close", h.events.get(h.events.size() - 2));
        assertEquals("upload.close:false", h.events.get(h.events.size() - 1));
    }

    @Test
    public void cleanupFailureDoesNotHideTheUploadFailure() {
        Harness h = new Harness();
        StorageWriter<Integer> writer = h.writer();
        h.failFlush = true; h.failClose = true;
        IOException failure = assertThrows(IOException.class, writer::close);
        assertEquals("upload failed", failure.getMessage());
        assertEquals("close failed", failure.getSuppressed()[0].getMessage());
    }

    @Test
    public void closeIsIdempotentAndRejectsFurtherWrites() throws Exception {
        Harness h = new Harness();
        StorageWriter<Integer> writer = h.writer();
        writer.close();
        List<String> firstClose = new ArrayList<>(h.events);
        writer.close();
        assertEquals(firstClose, h.events);
        assertThrows(IllegalStateException.class, () -> writer.write(1));
    }

    @Test
    public void sdkWriterClosesBeforeArrowMemoryAndSessionCommit() throws Exception {
        Harness h = new Harness();
        h.writer().close();
        assertEquals(Arrays.asList("flush", "writer.close", "encoder.close", "upload.close:true"), h.events);
    }

    @Test
    public void batchSizeMustBePositive() {
        Harness h = new Harness();
        assertThrows(IllegalArgumentException.class, () -> new StorageWriter<>(h, h, 0));
    }
}
