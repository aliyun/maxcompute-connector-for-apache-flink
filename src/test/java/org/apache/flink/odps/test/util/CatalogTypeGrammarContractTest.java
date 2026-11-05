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
package org.apache.flink.odps.test.util;

import com.aliyun.odps.type.TypeInfoFactory;

import org.apache.flink.odps.util.OdpsTableUtil;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Catalog type strings must preserve nested shapes and reject incomplete schemas. */
public class CatalogTypeGrammarContractTest {
    @Test
    public void preservesPrimitiveAndBoundedStringTypes() {
        assertEquals(TypeInfoFactory.BIGINT, OdpsTableUtil.getTypeInfoFromString("bigint"));
        assertEquals(TypeInfoFactory.getCharTypeInfo(12), OdpsTableUtil.getTypeInfoFromString("char(12)"));
        assertEquals(TypeInfoFactory.getVarcharTypeInfo(4096), OdpsTableUtil.getTypeInfoFromString("varchar(4096)"));
    }

    @Test
    public void preservesNestedFieldNamesAndTypeParameters() {
        assertEquals(
                TypeInfoFactory.getStructTypeInfo(
                        Arrays.asList("ORDER_ID", "ITEMS"),
                        Arrays.asList(
                                TypeInfoFactory.BIGINT,
                                TypeInfoFactory.getArrayTypeInfo(
                                        TypeInfoFactory.getMapTypeInfo(
                                                TypeInfoFactory.STRING,
                                                TypeInfoFactory.getDecimalTypeInfo(18, 4))))),
                OdpsTableUtil.getTypeInfoFromString(
                        "struct<ORDER_ID:bigint,ITEMS:array<map<string,decimal(18,4)>>>"));
    }

    @Test
    public void distinguishesDefaultAndParameterizedDecimal() {
        assertEquals(TypeInfoFactory.DECIMAL, OdpsTableUtil.getTypeInfoFromString("decimal"));
        assertEquals(TypeInfoFactory.getDecimalTypeInfo(30, 8), OdpsTableUtil.getTypeInfoFromString("decimal(30,8)"));
    }

    @Test
    public void acceptsJsonCatalogSignatures() {
        assertEquals(TypeInfoFactory.JSON, OdpsTableUtil.getTypeInfoFromString("json"));
        assertEquals(TypeInfoFactory.JSON, OdpsTableUtil.getTypeInfoFromString("json<>"));
    }

    @Test
    public void rejectsIncompleteOrTrailingGrammar() {
        for (String type : Arrays.asList("", "array<bigint", "map<string>", "struct<A int>", "int,boolean")) {
            assertThrows(type, IllegalArgumentException.class, () -> OdpsTableUtil.getTypeInfoFromString(type));
        }
    }

    @Test
    public void rejectsNonNumericParameters() {
        for (String type : Arrays.asList("varchar(nope)", "decimal(nope,2)", "decimal(12,nope)")) {
            assertThrows(type, IllegalArgumentException.class, () -> OdpsTableUtil.getTypeInfoFromString(type));
        }
    }
}
