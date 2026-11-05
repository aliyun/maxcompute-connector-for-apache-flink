/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.flink.odps.shim;

import java.io.IOException;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/** Keeps runtime context types out of the shared Storage writer construction. */
public interface FlinkSinkAdapter<T> extends Sink<T> {
    SinkWriter<T> createStorageWriter(int subtaskId, ProcessingTimeService timers) throws IOException;

    @Override
    default SinkWriter<T> createWriter(Sink.InitContext context) throws IOException {
        return createStorageWriter(context.getSubtaskId(), context.getProcessingTimeService());
    }

    @Override
    default SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        return createStorageWriter(context.getSubtaskId(), context.getProcessingTimeService());
    }
}
