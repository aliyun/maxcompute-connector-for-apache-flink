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

package org.apache.flink.odps.source.reader;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.odps.util.ArrowUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ByteColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.time.ZoneId;

/** Iterator of ColumnarRow. */
public class ColumnarRowIterator implements RecordIterator<RowData> {

    private final ColumnarRowData rowData;

    private final int num;
    private int pos;

    private final VectorSchemaRoot root;

    private final ColumnVector[] columns;

    private final boolean hasMetaDataColumns;

    public ColumnarRowIterator(VectorSchemaRoot root, RowType rowType,
                               ZoneId zoneId, boolean hasMetaDataColumns, int startPos) {
        this.root = root;
        this.rowData =
                new ColumnarRowData(ArrowUtils.createFlinkColumnBatch(this.root, rowType, zoneId));
        this.num = root.getRowCount();
        this.pos = startPos;
        this.hasMetaDataColumns = hasMetaDataColumns;
        if (hasMetaDataColumns) {
            this.columns = ArrowUtils.createFlinkMetaDataColumnBatch(this.root);
        } else {
            this.columns = new ColumnVector[0];
        }
    }

    @Nullable
    @Override
    public RowData next() {
        if (pos < num) {
            rowData.setRowId(pos);
            if (hasMetaDataColumns) {
                rowData.setRowKind(
                        ArrowUtils.toFlinkRowKind(((ByteColumnVector)columns[0]).getByte(pos)));
            }
            pos++;
            return rowData;
        } else {
            return null;
        }
    }

    @Override
    public void releaseBatch() {
        if (this.root != null) {
            this.root.close();
        }
    }
}