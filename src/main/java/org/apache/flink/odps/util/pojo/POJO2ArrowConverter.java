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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * @author dingxin ()
 */
public class POJO2ArrowConverter {
    private final Class clazz;
    private final Schema schema;
    private final Map<String, Method> getterCache = new HashMap<>();
    private VectorSchemaRoot arrowBatch;
    private POJOArrowWriter<Object> pojoArrowWriter;
    private BufferAllocator allocator;
    private int cacheCount = 0;

    public POJO2ArrowConverter(Class pojo, BufferAllocator allocator) {
        this.clazz = pojo;
        this.schema = getSchema(pojo.getDeclaredFields());
        if (allocator == null) {
            allocator = new RootAllocator(Long.MAX_VALUE);
        }
        this.allocator = allocator;
        cacheGetterMethods();
    }

    public void newInstance() {
        arrowBatch = VectorSchemaRoot.create(schema, allocator);
        pojoArrowWriter = POJOUtils.createPOJOArrowWriter(arrowBatch);
    }

    public Schema getSchema() {
        return schema;
    }

    public void reset() {
        cacheCount = 0;
        pojoArrowWriter.reset();
    }

    public void finish() {
        arrowBatch.setRowCount(cacheCount);
        pojoArrowWriter.finish();
    }

    // 将POJO数据写入VectorSchemaRoot，（增加一行）
    public void write(Object pojo) {
        if (pojo == null) {
            for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
                String fieldName = field.getName();
                pojoArrowWriter.write(null, fieldName);
            }
        } else {
            if (pojo.getClass() != clazz) {
                throw new IllegalArgumentException("POJO class not match: " + pojo.getClass() + ", " + clazz);
            }
            for (org.apache.arrow.vector.types.pojo.Field field : schema.getFields()) {
                String fieldName = field.getName();
                Method getter = getterCache.get(fieldName);
                Object value;
                try {
                    value = getter.invoke(pojo);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                pojoArrowWriter.write(value, fieldName);
            }
        }
        cacheCount++;
    }

    public VectorSchemaRoot getArrowBatch() {
        return arrowBatch;
    }

    public int getRowCount() {
        return cacheCount;
    }

    private Schema getSchema(Field[] fields) {
        List<org.apache.arrow.vector.types.pojo.Field> fieldList = new ArrayList<>();
        for (Field pojoField : fields) {
            if (pojoField.isSynthetic() || java.lang.reflect.Modifier.isStatic(pojoField.getModifiers())) {
                continue;
            }
            // 获取字段的名称和类型
            String fieldName = pojoField.getName();
            Class<?> fieldType = pojoField.getType();
            Type genericType = pojoField.getGenericType();

            // 根据字段的 Java 类型创建 Arrow 类型和子字段
            org.apache.arrow.vector.types.pojo.Field field = toArrowField(fieldName, fieldType, genericType, true);
            fieldList.add(field);
        }
        return new Schema(fieldList);
    }

    private static org.apache.arrow.vector.types.pojo.Field toArrowField(String fieldName, Class<?> type,
        Type genericType, boolean nullable) {
        // 根据 Java 类型返回对应的 Arrow 类型
        if (type == int.class || type == Integer.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Int(32, true),
                                                                              null, null), null);
        } else if (type == long.class || type == Long.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Int(64, true),
                                                                              null, null), null);
        } else if (type == short.class || type == Short.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Int(16, true),
                                                                              null, null), null);
        } else if (type == byte.class || type == Byte.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Int(8, true),
                                                                              null, null), null);
        } else if (type == float.class || type == Float.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName, new FieldType(nullable,
                                                                                         new ArrowType.FloatingPoint(
                                                                                             FloatingPointPrecision.SINGLE),
                                                                                         null, null), null);
        } else if (type == double.class || type == Double.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName, new FieldType(nullable,
                                                                                         new ArrowType.FloatingPoint(
                                                                                             FloatingPointPrecision.DOUBLE),
                                                                                         null, null), null);
        } else if (type == boolean.class || type == Boolean.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Bool(), null,
                                                                              null), null);
        } else if (type == String.class) {
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Utf8(), null,
                                                                              null), null);
        } else if (List.class.isAssignableFrom(type)) {
            // 处理泛型列表
            Type elementType = ((ParameterizedType)genericType).getActualTypeArguments()[0];
            org.apache.arrow.vector.types.pojo.Field elementField;
            if (elementType instanceof ParameterizedType) {
                // 处理嵌套列表或其他泛型类型
                // 例如：List<List<T>> 或 List<SomeGenericType<T, U>>
                ParameterizedType parameterizedElementType = (ParameterizedType)elementType;
                Class<?> elementRawType = (Class<?>)parameterizedElementType.getRawType();
                elementField = toArrowField("element", elementRawType, elementType, true);
            } else if (elementType instanceof Class) {
                // 处理简单泛型类型的列表
                // 例如：List<T>
                Class<?> elementClass = (Class<?>)elementType;
                elementField = toArrowField("element", elementClass, elementType, true);
            } else {
                throw new UnsupportedOperationException("Unsupported nested list type: " + elementType.getTypeName());
            }
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.List(), null,
                                                                              null),
                                                                Collections.singletonList(elementField));
        } else if (Map.class.isAssignableFrom(type)) {
            Type keyType = ((ParameterizedType)genericType).getActualTypeArguments()[0];
            Type valueType = ((ParameterizedType)genericType).getActualTypeArguments()[1];

            org.apache.arrow.vector.types.pojo.Field keyField;
            if (keyType instanceof ParameterizedType) {
                ParameterizedType parameterizedElementType = (ParameterizedType)keyType;
                Class<?> elementRawType = (Class<?>)parameterizedElementType.getRawType();
                keyField = toArrowField("key", elementRawType, keyType, false);
            } else if (keyType instanceof Class) {
                Class<?> elementClass = (Class<?>)keyType;
                keyField = toArrowField("key", elementClass, keyType, false);
            } else {
                throw new UnsupportedOperationException("Unsupported nested list type: " + keyType.getTypeName());
            }

            org.apache.arrow.vector.types.pojo.Field valueField;
            if (valueType instanceof ParameterizedType) {
                ParameterizedType parameterizedElementType = (ParameterizedType)valueType;
                Class<?> elementRawType = (Class<?>)parameterizedElementType.getRawType();
                valueField = toArrowField("value", elementRawType, valueType, true);
            } else if (valueType instanceof Class) {
                Class<?> elementClass = (Class<?>)valueType;
                valueField = toArrowField("value", elementClass, valueType, true);
            } else {
                throw new UnsupportedOperationException("Unsupported nested list type: " + valueType.getTypeName());
            }
            org.apache.arrow.vector.types.pojo.Field structFiled = new org.apache.arrow.vector.types.pojo.Field(
                "element", new FieldType(false, new Struct(), null, null), Arrays.asList(keyField, valueField));
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Map(false), null,
                                                                              null),
                                                                Collections.singletonList(structFiled));
        } else if (!type.isPrimitive()) {
            // 假设该字段是一个嵌套的 POJO 类
            List<org.apache.arrow.vector.types.pojo.Field> childFields = new ArrayList<>();
            for (Field pojoField : type.getDeclaredFields()) {
                // 跳过编译器引入的外围类引用字段
                if (pojoField.isSynthetic() || java.lang.reflect.Modifier.isStatic(pojoField.getModifiers()) || pojoField.getName().startsWith("this$")) {
                    continue;
                }
                org.apache.arrow.vector.types.pojo.Field childField = toArrowField(pojoField.getName(),
                                                                                   pojoField.getType(),
                                                                                   pojoField.getGenericType(),
                                                                                   nullable);
                childFields.add(childField);
            }
            return new org.apache.arrow.vector.types.pojo.Field(fieldName,
                                                                new FieldType(nullable, new ArrowType.Struct(), null,
                                                                              null), childFields);
        } else {
            // 更多类型的处理...
            throw new UnsupportedOperationException("Type conversion not supported for: " + type);
        }
    }

    private void cacheGetterMethods() {
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isSynthetic() || java.lang.reflect.Modifier.isStatic(field.getModifiers()) || field.getName().startsWith("this$")) {
                continue;
            }
            // 正常字段的 getter 方法
            String getterName = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
            try {
                Method getter = clazz.getMethod(getterName);
                getterCache.put(field.getName(), getter);
            } catch (NoSuchMethodException e) {
                // 如果没有找到 get 方法，且字段是 boolean 类型，尝试 is 方法
                if (field.getType().equals(boolean.class) || field.getType().equals(Boolean.class)) {
                    String booleanGetterName = "is" + field.getName().substring(0, 1).toUpperCase() + field.getName()
                        .substring(1);
                    try {
                        Method booleanGetter = clazz.getMethod(booleanGetterName);
                        getterCache.put(field.getName(), booleanGetter);
                    } catch (NoSuchMethodException ignored) {
                        throw new IllegalArgumentException("No getter method found for field: " + field);
                    }
                } else {
                    throw new IllegalArgumentException("No getter method found for field: " + field);
                }
            }
        }
    }
}
