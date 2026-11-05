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

import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.flink.odps.vectorized.writers.ArrowFieldWriter;

/**
 * @author dingxin ()
 */
public class POJOByteWriter extends ArrowFieldWriter<Byte> {

    public POJOByteWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Byte row, int ordinal) {
        if (row == null) {
            ((TinyIntVector)getValueVector()).setNull(getCount());
        } else {
            ((TinyIntVector)getValueVector()).setSafe(getCount(), row);
        }
    }
}
