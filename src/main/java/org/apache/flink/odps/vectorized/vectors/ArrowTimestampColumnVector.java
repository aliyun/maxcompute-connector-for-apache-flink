/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.vectorized.vectors;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

import java.time.ZoneId;

/** Arrow column vector for Timestamp. */
@Internal
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    /** Container which is used to store the sequence of timestamp values of a column to read. */
    private final ValueVector valueVector;

    private final TimeUnit unit;

    private final ZoneId zoneId;

    public ArrowTimestampColumnVector(ValueVector valueVector, ZoneId zoneId) {
        this.valueVector = Preconditions.checkNotNull(valueVector);
        Preconditions.checkState(valueVector instanceof TimeStampVector);
        this.unit = ((ArrowType.Timestamp) valueVector.getField().getType()).getUnit();
        this.zoneId = zoneId;
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        if (unit.equals(TimeUnit.SECOND)) {
            return TimestampData.fromEpochMillis(((TimeStampVector) valueVector).get(i) * 1000);
        } else if (unit.equals(TimeUnit.MILLISECOND)) {
            return TimestampData.fromEpochMillis(((TimeStampVector) valueVector).get(i));
        } else if (unit.equals(TimeUnit.MICROSECOND)) {
            long micros = ((TimeStampVector) valueVector).get(i);
            return TimestampData.fromEpochMillis(Math.floorDiv(micros, 1000), (int) Math.floorMod(micros, 1000) * 1000);
        } else {
            long nanos = ((TimeStampVector) valueVector).get(i);
            return TimestampData.fromEpochMillis(Math.floorDiv(nanos, 1_000_000), (int) Math.floorMod(nanos, 1_000_000));
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
