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

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class VectorSchemaRootSerializerSnapshot implements TypeSerializerSnapshot<VectorSchemaRoot> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getCurrentVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        // 由于 VectorSchemaRootSerializer 无状态，此处无需写入任何快照数据
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        // 由于 VectorSchemaRootSerializer 无状态，此处无需读取任何快照数据
    }

    @Override
    public TypeSerializer<VectorSchemaRoot> restoreSerializer() {
        return new VectorSchemaRootSerializer();
    }

    @Override
    public TypeSerializerSchemaCompatibility<VectorSchemaRoot> resolveSchemaCompatibility(
        TypeSerializer<VectorSchemaRoot> newSerializer) {
        // 检查新序列化器是否是 VectorSchemaRootSerializer 的实例
        if (newSerializer instanceof VectorSchemaRootSerializer) {
            // 如果是，就认为是兼容的
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        } else {
            // 如果新序列化器不是 VectorSchemaRootSerializer 的实例，则认为不兼容
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
