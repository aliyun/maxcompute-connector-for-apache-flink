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

import org.apache.flink.odps.util.OdpsTypeUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class OdpsTypeUtilTest {
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> types() {
        return Arrays.asList(new Object[][] {
                {DataTypes.BOOLEAN()}, {DataTypes.TINYINT()}, {DataTypes.SMALLINT()},
                {DataTypes.INT()}, {DataTypes.BIGINT()}, {DataTypes.FLOAT()}, {DataTypes.DOUBLE()},
                {DataTypes.STRING()}, {DataTypes.CHAR(255)}, {DataTypes.VARCHAR(65535)},
                {DataTypes.BYTES()}, {DataTypes.DATE()}, {DataTypes.TIMESTAMP(3)},
                {DataTypes.TIMESTAMP(9)}, {DataTypes.DECIMAL(38, 18)},
                {DataTypes.ARRAY(DataTypes.INT())},
                {DataTypes.MAP(DataTypes.STRING(), DataTypes.DECIMAL(18, 2))},
                {DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("nested", DataTypes.ARRAY(DataTypes.STRING())))}
        });
    }

    private final DataType type;
    public OdpsTypeUtilTest(DataType type) { this.type = type; }

    @Test
    public void roundTripsPrimitiveAndNestedSchemas() {
        assertEquals(type, OdpsTypeUtil.toFlinkType(OdpsTypeUtil.toOdpsTypeInfo(type)));
    }
}
