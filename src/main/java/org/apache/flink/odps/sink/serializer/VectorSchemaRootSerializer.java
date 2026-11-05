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

package org.apache.flink.odps.sink.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.odps.util.ArrowUtils;

public class VectorSchemaRootSerializer extends TypeSerializer<VectorSchemaRoot> {

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public VectorSchemaRootSerializer duplicate() {
        return new VectorSchemaRootSerializer();
    }

    @Override
    public VectorSchemaRoot createInstance() {
        // This method is used for creating instances during deserialization.
        // Since deserialization logic already handles instance creation, we return null here.
        return null;
    }

    @Override
    public VectorSchemaRoot copy(VectorSchemaRoot from) {
        if (from == null) {
            return null;
        }
        VectorSchemaRoot copy = VectorSchemaRoot.create(from.getSchema(), ArrowUtils.getRootAllocator());
        try {
            copy.allocateNew();
            for (int column = 0; column < from.getFieldVectors().size(); column++) {
                for (int row = 0; row < from.getRowCount(); row++) {
                    copy.getVector(column).copyFromSafe(row, row, from.getVector(column));
                }
            }
            copy.setRowCount(from.getRowCount());
            return copy;
        } catch (RuntimeException failure) {
            copy.close();
            throw failure;
        }
    }

    @Override
    public VectorSchemaRoot copy(VectorSchemaRoot from, VectorSchemaRoot reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(VectorSchemaRoot recordBatch, DataOutputView target) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); ArrowStreamWriter writer = new ArrowStreamWriter(
            recordBatch, new DictionaryProvider.MapDictionaryProvider(), out)) {
            writer.start();
            writer.writeBatch();
            writer.end();

            byte[] arrowBytes = out.toByteArray();
            target.writeInt(arrowBytes.length);
            target.write(arrowBytes);
        }
    }

    @Override
    public VectorSchemaRoot deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] arrowBytes = new byte[length];
        source.readFully(arrowBytes);
        try (ByteArrayInputStream in = new ByteArrayInputStream(arrowBytes);
             ArrowStreamReader reader = new ArrowStreamReader(in, ArrowUtils.getRootAllocator())) {
            if (!reader.loadNextBatch()) {
                throw new IOException("Arrow state contains no record batch");
            }
            return copy(reader.getVectorSchemaRoot());
        }
    }

    @Override
    public VectorSchemaRoot deserialize(VectorSchemaRoot reuse, DataInputView source) throws IOException {
        // Since we cannot reuse VectorSchemaRoot in the same way as other data types, we'll just call the regular
        // deserialize method.
        if (reuse != null) {
            reuse.close();
        }
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        byte[] arrowBytes = new byte[length];
        source.readFully(arrowBytes);
        target.writeInt(length);
        target.write(arrowBytes);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof VectorSchemaRootSerializer;
    }

    @Override
    public int hashCode() {
        return VectorSchemaRoot.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<VectorSchemaRoot> snapshotConfiguration() {
        // This method would return a snapshot of the serializer's configuration.
        // Since VectorSchemaRoot does not have any configuration that affects serialization, we can return a static
        // snapshot.
        return new VectorSchemaRootSerializerSnapshot();
    }
}
