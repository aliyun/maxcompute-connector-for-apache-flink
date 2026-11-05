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

import java.util.List;

import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.odps.vectorized.writers.ArrowFieldWriter;
import org.apache.flink.util.Preconditions;

/**
 * @author dingxin ()
 */
public class POJOListWriter extends ArrowFieldWriter<List> {
    private final ArrowFieldWriter<Object> elementWriter;

    public POJOListWriter(ListVector mapVector, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.elementWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(List row, int ordinal) {
        if (row != null) {
            ((ListVector)getValueVector()).startNewValue(getCount());
            for (int i = 0; i < row.size(); i++) {
                elementWriter.write(row.get(i), i);
            }
            ((ListVector)getValueVector()).endValue(getCount(), row.size());
        }
    }

    @Override
    public void finish() {
        super.finish();
        elementWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        elementWriter.reset();
    }
}
