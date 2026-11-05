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
 */

package org.apache.flink.odps.test.util;

import static org.junit.Assert.*;

import org.apache.arrow.vector.*;
import org.apache.flink.odps.util.ArrowUtils;
import org.apache.flink.odps.vectorized.ArrowWriter;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.types.logical.*;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.*;

/** Logical schema generation and precision conversion use independent expected timestamps. */
public class ArrowLogicalSchemaContractTest {
    @Test
    public void restoresPreEpochTimestampsAtEveryPrecision() throws Exception {
        for (boolean ltz : new boolean[] {false, true})
            for (int precision : new int[] {0, 3, 6, 9}) {
                RowType type =
                        RowType.of(
                                new LogicalType[] {
                                    ltz
                                            ? new LocalZonedTimestampType(precision)
                                            : new TimestampType(precision)
                                },
                                new String[] {"t"});
                try (VectorSchemaRoot root =
                        VectorSchemaRoot.create(
                                ArrowUtils.toArrowSchema(type), ArrowUtils.getRootAllocator())) {
                    ArrowWriter<RowData> writer = ArrowUtils.createRowDataArrowWriter(root, type);
                    TimestampData value = TimestampData.fromEpochMillis(-123456789L, 456789);
                    writer.write(GenericRowData.of(value));
                    writer.write(GenericRowData.of((Object) null));
                    writer.finish();
                    ColumnarRowData row =
                            new ColumnarRowData(
                                    ArrowUtils.createFlinkColumnBatch(root, type, ZoneOffset.UTC));
                    row.setRowId(0);
                    long millis = precision == 0 ? -123457000L : -123456789L;
                    int nanos = precision <= 3 ? 0 : precision == 6 ? 456000 : 456789;
                    assertEquals(
                            TimestampData.fromEpochMillis(millis, nanos),
                            row.getTimestamp(0, precision));
                    row.setRowId(1);
                    assertTrue(row.isNullAt(0));
                }
            }
    }

    @Test
    public void mapsScalarAndNestedLogicalTypesAndRejectsUnsupportedTypes() {
        List<LogicalType> types =
                Arrays.asList(
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new BooleanType(),
                        new FloatType(),
                        new DoubleType(),
                        new CharType(4),
                        new VarCharType(3),
                        new VarBinaryType(4),
                        new DecimalType(38, 8),
                        new DateType(),
                        new ArrayType(new BigIntType()),
                        new MapType(new VarCharType(10), new BigIntType()),
                        RowType.of(new BigIntType(), new VarCharType(10)));
        RowType row = RowType.of(types.toArray(new LogicalType[0]));
        org.apache.arrow.vector.types.pojo.Schema schema = ArrowUtils.toArrowSchema(row);
        assertEquals(types.size(), schema.getFields().size());
        assertEquals(
                64,
                ((org.apache.arrow.vector.types.pojo.ArrowType.Int)
                                schema.getFields().get(3).getType())
                        .getBitWidth());
        assertEquals(
                38,
                ((org.apache.arrow.vector.types.pojo.ArrowType.Decimal)
                                schema.getFields().get(10).getType())
                        .getPrecision());
        assertEquals(2, schema.getFields().get(14).getChildren().size());
        assertFalse(
                schema.getFields().get(13).getChildren().get(0).getChildren().get(0).isNullable());
        assertThrows(
                UnsupportedOperationException.class,
                () -> ArrowUtils.toArrowSchema(RowType.of(new TimeType())));
    }
}
