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

package org.apache.flink.odps.sink.insert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.odps.sink.cache.BucketWriter;
import org.apache.flink.odps.sink.cache.Buckets;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;

/** Checkpoint callbacks may run reentrantly while the cached writer yields its mailbox. */
public class CachedCheckpointProgressTest {
    @Test
    public void laterCheckpointAckCannotStrandAnEarlierWait() throws Exception {
        StubWriter writer = new StubWriter();
        int[] yields = {0};
        writer.setMailboxExecutor(
                proxy(
                        MailboxExecutor.class,
                        (object, method, args) -> {
                            if (method.getName().equals("yield")) {
                                assertEquals(
                                        "Already acknowledged checkpoints must not wait again",
                                        0,
                                        yields[0]++);
                                writer.acknowledge(5);
                                // A nested checkpoint finishes before the outer yield returns.
                                writer.acknowledge(6);
                            }
                            return null;
                        }));
        writer.await(5);
        assertEquals(1, yields[0]);
        assertEquals(6, writer.acknowledged());
    }

    @Test
    public void delayedOlderAckCannotMoveTheWriterBackwards() {
        StubWriter writer = new StubWriter();
        writer.acknowledge(6);
        writer.acknowledge(5);
        assertEquals(6, writer.acknowledged());
    }

    @Test
    public void endedEmptyCheckpointDoesNotWaitForGatedAcks() throws Exception {
        StubWriter writer = new StubWriter();
        writer.inputFinished();
        List<SinkTaskEvent> events = new ArrayList<>();
        writer.setOperatorEventGateway(event -> events.add((SinkTaskEvent) event));
        writer.setMailboxExecutor(
                proxy(MailboxExecutor.class, (p, method, args) -> {
                    throw new AssertionError("An ended writer with no uncommitted data must finish without an ACK");
                }));
        writer.await(5);
        assertEquals(1, events.size());
        assertTrue(events.get(0).isEndInput());
        assertTrue(events.get(0).isInputCommitted());
    }

    @Test
    public void endedWriterStillWaitsForUncommittedData() throws Exception {
        StubWriter writer = new StubWriter();
        writer.inputFinished();
        writer.committed = false;
        int[] yields = {0};
        writer.setMailboxExecutor(
                proxy(MailboxExecutor.class, (p, method, args) -> {
                    if (method.getName().equals("yield")) {
                        if (yields[0]++ == 0) {
                            writer.handleOperatorEvent(TaskAckEvent.builder().endInput(true).build());
                        } else {
                            assertEquals(2, yields[0]);
                            writer.acknowledge(5);
                            writer.committed = true;
                        }
                    }
                    return null;
                }));
        writer.await(5);
        assertEquals(2, yields[0]);
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(
                Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static final class StubWriter extends FileCachedInsertFunction {
        private boolean committed = true;

        StubWriter() {
            super(
                    new Configuration(),
                    new OdpsConf("test", "test", "http://127.0.0.1:1", "test_project"),
                    TableIdentifier.of("test_project", "default", "table"),
                    "",
                    DataSchema.newBuilder()
                            .columns(Collections.emptyList())
                            .partitionBy(Collections.emptyList())
                            .build(),
                    false,
                    false,
                    null,
                    null,
                    WriteOperationType.INSERT,
                    RowType.of());
            lastCommittingCheckpointId = -1;
            setOperatorEventGateway(event -> {});
        }

        void acknowledge(long checkpoint) {
            handleOperatorEvent(
                    TaskAckEvent.builder().checkpoint(true).checkpointId(checkpoint).build());
        }

        long acknowledged() {
            return lastCommittingCheckpointId;
        }

        void inputFinished() {
            inputEnded = true;
        }

        @SuppressWarnings("unchecked")
        void await(long checkpoint) throws Exception {
            // Empty buckets need no network, files or background writer threads.
            Configuration config = new Configuration();
            config.set(OdpsOptions.SINK_THREAD_NUM, 0);
            SimpleVersionedSerializer<?> serializer =
                    proxy(SimpleVersionedSerializer.class, (p, m, a) -> null);
            buckets =
                    new Buckets<RowData, String>(
                            config,
                            proxy(BucketWriter.class, (p, m, a) -> serializer),
                            proxy(BucketAssigner.class, (p, m, a) -> serializer),
                            proxy(RollingPolicy.class, (p, m, a) -> false),
                            0,
                            proxy(
                                    ProcessingTimeService.class,
                                    (p, m, a) ->
                                            m.getName().equals("getCurrentProcessingTime") ? 0L : null),
                            Comparator.naturalOrder()) {
                        @Override
                        public boolean isCommittedForCheckpoint(long checkpointId) {
                            return committed;
                        }

                        @Override
                        public boolean hasUncommittedData() {
                            return !committed;
                        }
                    };
            for (String name : new String[] {"lastCommittingCheckpointState", "endInputState"}) {
                Field field = FileCachedInsertFunction.class.getDeclaredField(name);
                field.setAccessible(true);
                field.set(this, proxy(ListState.class, (p, m, a) -> null));
            }
            try {
                checkForCommittingCheckpoint(checkpoint);
            } finally {
                buckets.close();
            }
        }
    }
}
