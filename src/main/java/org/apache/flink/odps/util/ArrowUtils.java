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

package org.apache.flink.odps.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.type.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.odps.vectorized.ArrowWriter;
import org.apache.flink.odps.vectorized.vectors.*;
import org.apache.flink.odps.vectorized.writers.*;
import org.apache.flink.odps.vectorized.writers.TinyIntWriter.OperationColumnWriter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static com.aliyun.odps.table.utils.SchemaUtils.ARROW_DECIMAL_DEFAULT_PRECISION;
import static com.aliyun.odps.table.utils.SchemaUtils.ARROW_DECIMAL_DEFAULT_SCALE;

/**
 * Util class for Arrow interface.
 */
public class ArrowUtils {

    private static RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

    public static RootAllocator getRootAllocator() {
        if (rootAllocator == null) {
            rootAllocator = new RootAllocator(Long.MAX_VALUE);
        }
        return rootAllocator;
    }

    public static VectorizedColumnBatch createFlinkColumnBatch(
            VectorSchemaRoot root, RowType rowType, ZoneId zoneId) {
        String[] tableFieldNames = rowType.getFieldNames().toArray(new String[0]);
        LogicalType[] tableFieldTypes = rowType.getChildren().toArray(new LogicalType[0]);
        // create and initialize the row batch
        ColumnVector[] vectors = new ColumnVector[tableFieldNames.length];
        // order
        for (int i = 0; i < vectors.length; i++) {
            String name = tableFieldNames[i];
            LogicalType type = tableFieldTypes[i];
            vectors[i] = createColumnVector(root.getVector(name), type, zoneId);
        }
        VectorizedColumnBatch flinkBatch = new VectorizedColumnBatch(vectors);
        flinkBatch.setNumRows(root.getRowCount());
        return flinkBatch;
    }

    public static ColumnVector[] createFlinkMetaDataColumnBatch(VectorSchemaRoot root) {
        // create and initialize the row batc
        ColumnVector[] vectors = new ColumnVector[1];
        if (root.getVector(Constants.ODPS_META_OP_TYPE) != null) {
            vectors[0] = createColumnVector(root.getVector(Constants.ODPS_META_OP_TYPE),
                    null, null);
        } else if (root.getVector(Constants.ODPS_OP) != null) {
            vectors[0] = createColumnVector(root.getVector(Constants.ODPS_OP),
                    null, null);
        } else {
            throw new UnsupportedOperationException("Unsupported create meta data columns");
        }
        return vectors;
    }

    public static ColumnVector createColumnVector(
            ValueVector vector, LogicalType fieldType, ZoneId zoneId) {
        // todo avoid using instanceof to speed up.
        if (vector instanceof TinyIntVector) {
            return new ArrowTinyIntColumnVector((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowSmallIntColumnVector((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return new ArrowIntColumnVector((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return new ArrowBigIntColumnVector((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return new ArrowBooleanColumnVector((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return new ArrowFloatColumnVector((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return new ArrowDoubleColumnVector((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return new ArrowVarCharColumnVector((VarCharVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            return new ArrowDecimalColumnVector((DecimalVector) vector);
        } else if (vector instanceof DateDayVector) {
            return new ArrowDateColumnVector((DateDayVector) vector);
        } else if (vector instanceof TimeStampVector) {
            return new ArrowTimestampColumnVector(vector, zoneId);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            LogicalType keyType = ((MapType) fieldType).getKeyType();
            LogicalType valueType = ((MapType) fieldType).getValueType();
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return new ArrowMapColumnVector(
                    mapVector,
                    createColumnVector(structVector.getChild(MapVector.KEY_NAME), keyType, zoneId),
                    createColumnVector(
                            structVector.getChild(MapVector.VALUE_NAME), valueType, zoneId));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            return new ArrowArrayColumnVector(
                    listVector,
                    createColumnVector(
                            listVector.getDataVector(),
                            ((ArrayType) fieldType).getElementType(),
                            zoneId));
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector) vector;
            ColumnVector[] fieldColumns = new ColumnVector[structVector.size()];
            for (int i = 0; i < fieldColumns.length; ++i) {
                fieldColumns[i] =
                        createColumnVector(
                                structVector.getVectorById(i),
                                ((RowType) fieldType).getTypeAt(i),
                                zoneId);
            }
            return new ArrowRowColumnVector(structVector, fieldColumns);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", fieldType));
        }
    }

    public static Schema toArrowSchema(List<Column> columns) {
        return toArrowSchema(columns, ArrowOptions.createDefault());
    }

    public static Field columnToArrowField(Column fieldColumn) {
        return columnToArrowField(fieldColumn, ArrowOptions.createDefault());
    }

    public static Schema toArrowSchema(List<Column> columns, ArrowOptions arrowOptions) {
        Collection<Field> fields =
                columns.stream()
                        .map(col -> columnToArrowField(col, arrowOptions))
                        .collect(Collectors.toCollection(ArrayList::new));
        return new Schema(fields);
    }

    public static Field columnToArrowField(Column fieldColumn, ArrowOptions options) {
        String fieldName = fieldColumn.getName();
        TypeInfo typeInfo = fieldColumn.getTypeInfo();
        return convertTypeInfoToArrowField(fieldName, typeInfo, fieldColumn.isNullable(), options);
    }

    private static Field convertTypeInfoToArrowField(String fieldName,
                                                     TypeInfo typeInfo,
                                                     boolean nullable,
                                                     ArrowOptions options) {
        ArrowType arrowType = getArrowType(typeInfo, options);
        return new Field(fieldName, new FieldType(nullable, arrowType, null, null),
                generateSubFields(typeInfo, options));
    }

    private static List<Field> generateSubFields(TypeInfo typeInfo, ArrowOptions options) {
        if (typeInfo instanceof ArrayTypeInfo) {
            ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
            TypeInfo subti = arrayTypeInfo.getElementTypeInfo();
            return Arrays.asList(convertTypeInfoToArrowField("element", subti, true, options));
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            TypeInfo keyti = mapTypeInfo.getKeyTypeInfo(), valti = mapTypeInfo.getValueTypeInfo();
            return Arrays.asList(
                    new Field(
                            "element",
                            new FieldType(false, new ArrowType.Struct(), null, null),
                            Arrays.asList(
                                    convertTypeInfoToArrowField("key", keyti, false, options),
                                    convertTypeInfoToArrowField("value", valti, true, options))));
        } else if (typeInfo instanceof StructTypeInfo) {
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            ArrayList<Field> sfields = new ArrayList<>();
            List<TypeInfo> subTypeInfos = structTypeInfo.getFieldTypeInfos();
            List<String> subNames = structTypeInfo.getFieldNames();
            for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                sfields.add(
                        convertTypeInfoToArrowField(
                                subNames.get(i), subTypeInfos.get(i), true, options));
            }
            return sfields;
        } else {
            return null;
        }
    }

    private static ArrowType getArrowType(TypeInfo typeInfo, ArrowOptions options) {
        ArrowType arrowType = null;
        switch (typeInfo.getOdpsType()) {
            case JSON:
            case CHAR:
            case VARCHAR:
            case STRING:
                arrowType = new ArrowType.Utf8();
                break;
            case BINARY:
                arrowType = new ArrowType.Binary();
                break;
            case TINYINT:
                arrowType = new ArrowType.Int(8, true);
                break;
            case SMALLINT:
                arrowType = new ArrowType.Int(16, true);
                break;
            case INT:
                arrowType = new ArrowType.Int(32, true);
                break;
            case BIGINT:
                arrowType = new ArrowType.Int(64, true);
                break;
            case BOOLEAN:
                arrowType = new ArrowType.Bool();
                break;
            case FLOAT:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                break;
            case DOUBLE:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                break;
            case DECIMAL:
                // TODO: decimal
                if (((DecimalTypeInfo) typeInfo).getPrecision() == 54
                        && ((DecimalTypeInfo) typeInfo).getScale() == 18) {
                    arrowType =
                            new ArrowType.Decimal(
                                    ARROW_DECIMAL_DEFAULT_PRECISION, ARROW_DECIMAL_DEFAULT_SCALE);
                } else {
                    arrowType =
                            new ArrowType.Decimal(
                                    ((DecimalTypeInfo) typeInfo).getPrecision(),
                                    ((DecimalTypeInfo) typeInfo).getScale());
                }
                break;
            case DATE:
                arrowType = new ArrowType.Date(DateUnit.DAY);
                break;
            case DATETIME:
                // for tunnel arrow date time
                arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
                break;
            case TIMESTAMP:
                arrowType = parseTimeStamp(options.getTimestampUnit());
                break;
            case ARRAY:
                arrowType = new ArrowType.List();
                break;
            case STRUCT:
                arrowType = new ArrowType.Struct();
                break;
            case MAP:
                arrowType = new ArrowType.Map(false);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + typeInfo.getOdpsType());
        }
        return arrowType;
    }

    private static ArrowType.Timestamp parseTimeStamp(ArrowOptions.TimestampUnit unit) {
        switch (unit) {
            case SECOND:
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            case MILLI:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case NANO:
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            case MICRO:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + unit);
        }
    }

    /**
     * Returns the Arrow schema of the specified type.
     */
    public static Schema toArrowSchema(RowType rowType) {
        Collection<Field> fields =
                rowType.getFields().stream()
                        .map(
                                f ->
                                        ArrowUtils.toArrowField(
                                                f.getName(), f.getType(), f.getType().isNullable()))
                        .collect(Collectors.toCollection(ArrayList::new));
        return new Schema(fields);
    }

    private static Field toArrowField(
            String fieldName, LogicalType logicalType, boolean isNullable) {
        FieldType fieldType =
                new FieldType(
                        isNullable,
                        logicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE),
                        null);
        List<Field> children = null;
        if (logicalType instanceof ArrayType) {
            children =
                    Collections.singletonList(
                            toArrowField(
                                    "element",
                                    ((ArrayType) logicalType).getElementType(),
                                    isNullable));
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            children = new ArrayList<>(rowType.getFieldCount());
            for (RowType.RowField field : rowType.getFields()) {
                children.add(toArrowField(field.getName(), field.getType(), isNullable));
            }
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();
            children =
                    Collections.singletonList(
                            new Field(
                                    "element",
                                    new FieldType(false, new ArrowType.Struct(), null),
                                    Arrays.asList(
                                            toArrowField("key", keyType, false),
                                            toArrowField(
                                                    "value", valueType, valueType.isNullable()))));
        }
        return new Field(fieldName, fieldType, children);
    }

    /**
     * Creates an {@link ArrowWriter} for the specified {@link VectorSchemaRoot}.
     */
    public static ArrowWriter<RowData> createRowDataArrowWriter(
            VectorSchemaRoot root, RowType rowType) {
        ArrowFieldWriter<RowData>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.allocateNew();

            // when upsert tables, there has a system column in arrow schema
            if (vector.getName().equals(com.aliyun.odps.storage.internal.Constants.OPERATION_COLUMN_NAME)) {
                fieldWriters[i] = new OperationColumnWriter((TinyIntVector) vector);
            } else {
                fieldWriters[i] = createArrowFieldWriterForRow(vector, rowType.getTypeAt(i));
            }
        }
        return new ArrowWriter<>(root, fieldWriters);
    }

    private static ArrowFieldWriter<RowData> createArrowFieldWriterForRow(
            ValueVector vector, LogicalType fieldType) {
        if (vector instanceof TinyIntVector) {
            return TinyIntWriter.forRow((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return SmallIntWriter.forRow((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return IntWriter.forRow((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return BigIntWriter.forRow((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return BooleanWriter.forRow((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return FloatWriter.forRow((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return DoubleWriter.forRow((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            if (fieldType instanceof CharType) {
                return VarCharWriter.forRow((VarCharVector) vector, ((CharType) fieldType).getLength());
            } else if (fieldType instanceof VarCharType) {
                return VarCharWriter.forRow((VarCharVector) vector, ((VarCharType) fieldType).getLength());
            }
            return VarCharWriter.forRow((VarCharVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return VarBinaryWriter.forRow((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return DecimalWriter.forRow(
                    decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return DateWriter.forRow((DateDayVector) vector);
        } else if (vector instanceof DateMilliVector) {
            int precision;
            if (fieldType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) fieldType).getPrecision();
            } else {
                precision = ((TimestampType) fieldType).getPrecision();
            }
            return DateTimeMillWriter.forRow((DateMilliVector) vector, precision);
        } else if (vector instanceof TimeStampVector) {
            int precision;
            if (fieldType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) fieldType).getPrecision();
            } else {
                precision = ((TimestampType) fieldType).getPrecision();
            }
            return TimestampWriter.forRow(vector, precision);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            MapType mapType = (MapType) fieldType;
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return MapWriter.forRow(
                    mapVector,
                    createArrowFieldWriterForArray(
                            structVector.getChild(MapVector.KEY_NAME), keyType),
                    createArrowFieldWriterForArray(
                            structVector.getChild(MapVector.VALUE_NAME), valueType));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            LogicalType elementType = ((ArrayType) fieldType).getElementType();
            return ArrayWriter.forRow(
                    listVector,
                    createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
        } else if (vector instanceof StructVector) {
            RowType rowType = (RowType) fieldType;
            ArrowFieldWriter<RowData>[] fieldsWriters =
                    new ArrowFieldWriter[rowType.getFieldCount()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] =
                        createArrowFieldWriterForRow(
                                ((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
            }
            return RowWriter.forRow((StructVector) vector, fieldsWriters);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", fieldType));
        }
    }

    private static ArrowFieldWriter<ArrayData> createArrowFieldWriterForArray(
            ValueVector vector, LogicalType fieldType) {
        if (vector instanceof TinyIntVector) {
            return TinyIntWriter.forArray((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return SmallIntWriter.forArray((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return IntWriter.forArray((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return BigIntWriter.forArray((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return BooleanWriter.forArray((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return FloatWriter.forArray((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return DoubleWriter.forArray((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            if (fieldType instanceof CharType) {
                return VarCharWriter.forArray(
                        (VarCharVector) vector, ((CharType) fieldType).getLength());
            } else if (fieldType instanceof VarCharType) {
                return VarCharWriter.forArray(
                        (VarCharVector) vector, ((VarCharType) fieldType).getLength());
            }
            return VarCharWriter.forArray((VarCharVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return VarBinaryWriter.forArray((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return DecimalWriter.forArray(
                    decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return DateWriter.forArray((DateDayVector) vector);
        } else if (vector instanceof DateMilliVector) {
            int precision;
            if (fieldType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) fieldType).getPrecision();
            } else {
                precision = ((TimestampType) fieldType).getPrecision();
            }
            return DateTimeMillWriter.forArray((DateMilliVector) vector, precision);
        } else if (vector instanceof TimeStampVector) {
            int precision;
            if (fieldType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) fieldType).getPrecision();
            } else {
                precision = ((TimestampType) fieldType).getPrecision();
            }
            return TimestampWriter.forArray(vector, precision);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            MapType mapType = (MapType) fieldType;
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return MapWriter.forArray(
                    mapVector,
                    createArrowFieldWriterForArray(
                            structVector.getChild(MapVector.KEY_NAME), keyType),
                    createArrowFieldWriterForArray(
                            structVector.getChild(MapVector.VALUE_NAME), valueType));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            LogicalType elementType = ((ArrayType) fieldType).getElementType();
            return ArrayWriter.forArray(
                    listVector,
                    createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
        } else if (vector instanceof StructVector) {
            RowType rowType = (RowType) fieldType;
            ArrowFieldWriter<RowData>[] fieldsWriters =
                    new ArrowFieldWriter[rowType.getFieldCount()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] =
                        createArrowFieldWriterForRow(
                                ((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
            }
            return RowWriter.forArray((StructVector) vector, fieldsWriters);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported type %s.", fieldType));
        }
    }

    private static int getPrecision(DecimalVector decimalVector) {
        int precision = -1;
        try {
            java.lang.reflect.Field precisionField =
                    decimalVector.getClass().getDeclaredField("precision");
            precisionField.setAccessible(true);
            precision = (int) precisionField.get(decimalVector);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // should not happen, ignore
        }
        return precision;
    }

    private static class LogicalTypeToArrowTypeConverter
            extends LogicalTypeDefaultVisitor<ArrowType> {

        private static final LogicalTypeToArrowTypeConverter INSTANCE =
                new LogicalTypeToArrowTypeConverter();

        @Override
        public ArrowType visit(TinyIntType tinyIntType) {
            return new ArrowType.Int(8, true);
        }

        @Override
        public ArrowType visit(SmallIntType smallIntType) {
            return new ArrowType.Int(2 * 8, true);
        }

        @Override
        public ArrowType visit(IntType intType) {
            return new ArrowType.Int(4 * 8, true);
        }

        @Override
        public ArrowType visit(BigIntType bigIntType) {
            return new ArrowType.Int(8 * 8, true);
        }

        @Override
        public ArrowType visit(BooleanType booleanType) {
            return ArrowType.Bool.INSTANCE;
        }

        @Override
        public ArrowType visit(FloatType floatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }

        @Override
        public ArrowType visit(DoubleType doubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }

        @Override
        public ArrowType visit(VarCharType varCharType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(CharType charType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(VarBinaryType varCharType) {
            return ArrowType.Binary.INSTANCE;
        }

        @Override
        public ArrowType visit(DecimalType decimalType) {
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public ArrowType visit(DateType dateType) {
            return new ArrowType.Date(DateUnit.DAY);
        }


        @Override
        public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
            if (localZonedTimestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 1
                    && localZonedTimestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 4
                    && localZonedTimestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(TimestampType timestampType) {
            if (timestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(ArrayType arrayType) {
            return ArrowType.List.INSTANCE;
        }

        @Override
        public ArrowType visit(RowType rowType) {
            return ArrowType.Struct.INSTANCE;
        }

        @Override
        public ArrowType visit(MapType mapType) {
            return new ArrowType.Map(false);
        }

        @Override
        protected ArrowType defaultMethod(LogicalType logicalType) {
            if (logicalType instanceof LegacyTypeInformationType) {
                Class<?> typeClass =
                        ((LegacyTypeInformationType) logicalType)
                                .getTypeInformation()
                                .getTypeClass();
                if (typeClass == BigDecimal.class) {
                    // Because we can't get precision and scale from legacy BIG_DEC_TYPE_INFO,
                    // we set the precision and scale to default value compatible with python.
                    return new ArrowType.Decimal(38, 18);
                }
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "Python vectorized UDF doesn't support logical type %s currently.",
                            logicalType.asSummaryString()));
        }
    }

    public static RowKind toFlinkRowKind(byte metaOp) {
        switch (metaOp) {
            case 2:
                return RowKind.INSERT;
            case 4:
                return RowKind.DELETE;
            case 16:
                return RowKind.UPDATE_BEFORE;
            case 8:
            case 32:
                return RowKind.UPDATE_AFTER;
            default:
                throw new UnsupportedOperationException("Unsupported meta op type " + metaOp);
        }
    }
}
