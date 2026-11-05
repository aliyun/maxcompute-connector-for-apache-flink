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

/**
 * @author dingxin ()
 */
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class VectorSchemaRootTypeInfo extends TypeInformation<VectorSchemaRoot> {

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        // VectorSchemaRoot 通常包含多个字段
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<VectorSchemaRoot> getTypeClass() {
        return VectorSchemaRoot.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<VectorSchemaRoot> createSerializer(ExecutionConfig config) {
        return new VectorSchemaRootSerializer();
    }

    @Override
    public String toString() {
        return "TypeInformation for VectorSchemaRoot";
    }


    @Override
    public int hashCode() {
        return VectorSchemaRoot.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof VectorSchemaRootTypeInfo;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof VectorSchemaRootTypeInfo;
    }
}
