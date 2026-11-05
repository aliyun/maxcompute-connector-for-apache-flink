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

package org.apache.flink.odps.util.pojo.writers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.odps.vectorized.writers.ArrowFieldWriter;
import org.apache.flink.util.Preconditions;

/**
 * @author dingxin ()
 */
public class POJOMapWriter extends ArrowFieldWriter<Map> {
    protected StructVector structVector;
    private final ArrowFieldWriter<Object> keyWriter;
    private final ArrowFieldWriter<Object> valueWriter;

    public POJOMapWriter(MapVector mapVector, ArrowFieldWriter keyWriter, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.structVector = (StructVector)mapVector.getDataVector();
        this.keyWriter = Preconditions.checkNotNull(keyWriter);
        this.valueWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(Map row, int ordinal) {
        if (row != null) {
            ((MapVector)getValueVector()).startNewValue(getCount());
            List<Map.Entry> list = new ArrayList<>(row.entrySet());
            for (int i = 0; i < list.size(); i++) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.write(list.get(i).getKey(), i);
                valueWriter.write(list.get(i).getValue(), i);
            }
            ((MapVector)getValueVector()).endValue(getCount(), list.size());
        }
    }

    @Override
    public void finish() {
        super.finish();
        keyWriter.finish();
        valueWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        keyWriter.reset();
        valueWriter.reset();
    }
}
