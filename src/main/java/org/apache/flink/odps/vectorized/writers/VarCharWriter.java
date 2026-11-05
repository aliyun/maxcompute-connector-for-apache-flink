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

import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/** {@link ArrowFieldWriter} for VarChar. */
@Internal
public abstract class VarCharWriter<T> extends ArrowFieldWriter<T> {

    public static VarCharWriter<RowData> forRow(VarCharVector varCharVector) {
        return new VarCharWriterForRow(varCharVector);
    }

    public static VarCharWriter<ArrayData> forArray(VarCharVector varCharVector) {
        return new VarCharWriterForArray(varCharVector);
    }

    public static VarCharWriter<RowData> forRow(VarCharVector varCharVector, int maxLength) {
        return new VarCharWriterForRow(varCharVector, maxLength);
    }

    public static VarCharWriter<ArrayData> forArray(VarCharVector varCharVector, int maxLength) {
        return new VarCharWriterForArray(varCharVector, maxLength);
    }

    // ------------------------------------------------------------------------------------------

    private final int maxLength;

    private VarCharWriter(VarCharVector varCharVector) {
        super(varCharVector);
        this.maxLength = -1;
    }

    private VarCharWriter(VarCharVector varCharVector, int maxLength) {
        super(varCharVector);
        this.maxLength = maxLength;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract StringData readString(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((VarCharVector) getValueVector()).setNull(getCount());
        } else {
            String value = readString(in, ordinal).toString();
            if (maxLength > 0 && value.codePointCount(0, value.length()) > maxLength) {
                value = value.substring(0, value.offsetByCodePoints(0, maxLength));
            }
            byte[] data = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int dataLength = data.length;
            ((VarCharVector) getValueVector())
                    .setSafe(getCount(), data, 0, dataLength);
        }
    }

    // ------------------------------------------------------------------------------------------
    public static final class VarCharWriterForRow extends VarCharWriter<RowData> {

        private VarCharWriterForRow(VarCharVector varCharVector) {
            super(varCharVector);
        }

        private VarCharWriterForRow(VarCharVector varCharVector, int maxLength) {
            super(varCharVector, maxLength);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(RowData in, int ordinal) {
            return in.getString(ordinal);
        }
    }

    public static final class VarCharWriterForArray extends VarCharWriter<ArrayData> {

        private VarCharWriterForArray(VarCharVector varCharVector) {
            super(varCharVector);
        }

        private VarCharWriterForArray(VarCharVector varCharVector, int maxLength) {
            super(varCharVector, maxLength);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(ArrayData in, int ordinal) {
            return in.getString(ordinal);
        }
    }
}
