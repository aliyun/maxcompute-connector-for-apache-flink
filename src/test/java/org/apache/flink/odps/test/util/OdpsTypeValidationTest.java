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
import org.apache.flink.odps.util.OdpsTypeUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.Test;

import static org.junit.Assert.*;

public class OdpsTypeValidationTest {
    @Test public void rejectsOversizedChar() {
        assertThrows(CatalogException.class, () -> OdpsTypeUtil.toOdpsTypeInfo(DataTypes.CHAR(256)));
    }
    @Test public void rejectsOversizedVarchar() {
        assertThrows(CatalogException.class, () -> OdpsTypeUtil.toOdpsTypeInfo(DataTypes.VARCHAR(65536)));
    }
    @Test public void rejectsUnsupportedDecimalScale() {
        assertThrows(CatalogException.class, () -> OdpsTypeUtil.toOdpsTypeInfo(DataTypes.DECIMAL(38, 19)));
    }
    @Test public void rejectsUnsupportedTime() {
        assertThrows(UnsupportedOperationException.class, () -> OdpsTypeUtil.toOdpsTypeInfo(DataTypes.TIME(3)));
    }
    @Test public void keepsTimestampLtzPrecisionContract() {
        assertEquals(TypeInfoFactory.TIMESTAMP, OdpsTypeUtil.toOdpsTypeInfo(DataTypes.TIMESTAMP_LTZ(9)));
        assertEquals(TypeInfoFactory.DATETIME, OdpsTypeUtil.toOdpsTypeInfo(DataTypes.TIMESTAMP_LTZ(3)));
    }
    @Test public void rejectsNullTypes() {
        assertThrows(NullPointerException.class, () -> OdpsTypeUtil.toOdpsTypeInfo(null));
        assertThrows(NullPointerException.class, () -> OdpsTypeUtil.toFlinkType(null));
    }
}
