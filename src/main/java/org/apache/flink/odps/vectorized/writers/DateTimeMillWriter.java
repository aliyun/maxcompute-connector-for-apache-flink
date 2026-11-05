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
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.vectorized.writers;

import org.apache.arrow.vector.DateMilliVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/** {@link ArrowFieldWriter} for Date. */
@Internal
public abstract class DateTimeMillWriter<T> extends ArrowFieldWriter<T> {

    public static DateTimeMillWriter<RowData> forRow(DateMilliVector dateMillVector, int precision) {
        return new DateTimeMillWriterForRow(dateMillVector, precision);
    }

    public static DateTimeMillWriter<ArrayData> forArray(DateMilliVector dateMillVector, int precision) {
        return new DateTimeMillWriterForArray(dateMillVector, precision);
    }

    // ------------------------------------------------------------------------------------------
    protected final int precision;

    private DateTimeMillWriter(DateMilliVector dateMillVector, int precision) {
        super(dateMillVector);
        this.precision = precision;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract long readDateTime(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((DateMilliVector) getValueVector()).setNull(getCount());
        } else {
            ((DateMilliVector) getValueVector()).setSafe(getCount(), readDateTime(in, ordinal));
        }
    }

    // ------------------------------------------------------------------------------------------
    public static final class DateTimeMillWriterForRow extends DateTimeMillWriter<RowData> {

        private DateTimeMillWriterForRow(DateMilliVector dateDayVector, int precision) {
            super(dateDayVector, precision);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        long readDateTime(RowData in, int ordinal) {
            return in.getTimestamp(ordinal, precision).getMillisecond();
        }
    }

    public static final class DateTimeMillWriterForArray extends DateTimeMillWriter<ArrayData> {

        private DateTimeMillWriterForArray(DateMilliVector dateDayVector, int precision) {
            super(dateDayVector, precision);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        long readDateTime(ArrayData in, int ordinal) {
            return in.getTimestamp(ordinal, precision).getMillisecond();
        }
    }
}
