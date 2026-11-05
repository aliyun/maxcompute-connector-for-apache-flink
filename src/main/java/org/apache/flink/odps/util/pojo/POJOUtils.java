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

package org.apache.flink.odps.util.pojo;

import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.odps.util.pojo.writers.POJOBooleanWriter;
import org.apache.flink.odps.util.pojo.writers.POJOByteWriter;
import org.apache.flink.odps.util.pojo.writers.POJODoubleWriter;
import org.apache.flink.odps.util.pojo.writers.POJOFloatWriter;
import org.apache.flink.odps.util.pojo.writers.POJOIntegerWriter;
import org.apache.flink.odps.util.pojo.writers.POJOListWriter;
import org.apache.flink.odps.util.pojo.writers.POJOLongWriter;
import org.apache.flink.odps.util.pojo.writers.POJOMapWriter;
import org.apache.flink.odps.util.pojo.writers.POJOShortWriter;
import org.apache.flink.odps.util.pojo.writers.POJOStringWriter;
import org.apache.flink.odps.util.pojo.writers.POJOStructWriter;
import org.apache.flink.odps.vectorized.writers.ArrowFieldWriter;

/**
 * @author dingxin ()
 */
public class POJOUtils {
    public static ArrowFieldWriter createPOJOFieldWriter(ValueVector vector) {
        if (vector instanceof TinyIntVector) {
            return new POJOByteWriter(vector);
        } else if (vector instanceof SmallIntVector) {
            return new POJOShortWriter(vector);
        } else if (vector instanceof BigIntVector) {
            return new POJOLongWriter(vector);
        } else if (vector instanceof BitVector) {
            return new POJOBooleanWriter(vector);
        } else if (vector instanceof Float4Vector) {
            return new POJOFloatWriter(vector);
        } else if (vector instanceof Float8Vector) {
            return new POJODoubleWriter(vector);
        } else if (vector instanceof VarCharVector) {
            return new POJOStringWriter(vector);
        } else if (vector instanceof IntVector) {
            return new POJOIntegerWriter(vector);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector)vector;
            StructVector structVector = (StructVector)mapVector.getDataVector();
            return new POJOMapWriter(mapVector, createPOJOFieldWriter(structVector.getChild(MapVector.KEY_NAME)),
                                     createPOJOFieldWriter(structVector.getChild(MapVector.VALUE_NAME)));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector)vector;
            FieldVector elementVector = listVector.getDataVector();
            return new POJOListWriter(listVector, createPOJOFieldWriter(elementVector));
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector)vector;
            List<FieldVector> childrenVectors = structVector.getChildrenFromFields();
            ArrowFieldWriter[] fieldsWriters = new ArrowFieldWriter[childrenVectors.size()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] = createPOJOFieldWriter(((StructVector)vector).getVectorById(i));
            }
            return new POJOStructWriter(structVector, fieldsWriters);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s.", vector.getName()));
        }
    }

    public static POJOArrowWriter<Object> createPOJOArrowWriter(VectorSchemaRoot root) {
        ArrowFieldWriter<Object>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.allocateNew();
            fieldWriters[i] = createPOJOFieldWriter(vector);
        }
        return new POJOArrowWriter<>(root, fieldWriters);
    }
}
