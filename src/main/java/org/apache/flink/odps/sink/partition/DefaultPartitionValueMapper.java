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

package org.apache.flink.odps.sink.partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import java.util.List;

/** Applies the connector's STRING partition default before serializing Storage writes. */
public final class DefaultPartitionValueMapper implements MapFunction<RowData,RowData> {
    private final RowData.FieldGetter[] getters;
    private final int[] partitionIndices;
    private final String defaultValue;
    public DefaultPartitionValueMapper(RowType rowType,List<String> partitionNames,String defaultValue) {
        this.defaultValue=defaultValue;
        getters=new RowData.FieldGetter[rowType.getFieldCount()];
        for(int i=0;i<getters.length;i++)getters[i]=RowData.createFieldGetter(rowType.getTypeAt(i),i);
        partitionIndices=partitionNames.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
        for(int index:partitionIndices)if(index<0)throw new IllegalArgumentException("Partition field missing from SQL row");
    }
    @Override public RowData map(RowData input) {
        GenericRowData row=new GenericRowData(input.getRowKind(),getters.length);
        for(int i=0;i<getters.length;i++)row.setField(i,getters[i].getFieldOrNull(input));
        for(int index:partitionIndices) {
            Object value=row.getField(index);
            if(value==null || (value instanceof StringData && value.toString().isEmpty()))row.setField(index,StringData.fromString(defaultValue));
        }
        return row;
    }
}
