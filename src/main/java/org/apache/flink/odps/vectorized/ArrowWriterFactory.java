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

package org.apache.flink.odps.vectorized;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

/**
 * A serializable factory for creating {@link ArrowWriter}.
 * The factory itself is created on the client side (in the Sink constructor)
 * and serialized to the TaskManagers, where it creates the actual writer instance.
 *
 * @param <T> The input data type for the writer.
 */
@FunctionalInterface
public interface ArrowWriterFactory<T> extends Serializable {

    /**
     * Creates an ArrowWriter instance.
     *
     * @param root The Arrow Batch need to write.
     * @param rowType The logical row type of the data.
     * @return A new instance of ArrowWriter.
     */
    ArrowWriter<T> create(VectorSchemaRoot root, RowType rowType);
}
