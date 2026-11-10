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
package org.apache.flink.odps.table;

import org.apache.flink.odps.sink.partition.DefaultPartitionValueMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class DynamicPartitionChangelogOrderTest {
    @Test
    public void partitionNormalizationDoesNotRebalanceAnOrderedInput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        RowType schema = RowType.of(new LogicalType[] {new BigIntType(), new VarCharType()},
                new String[] {"id", "ds"});
        DataStream<RowData> input = env.fromCollection(
                Collections.singletonList(GenericRowData.of(1L, StringData.fromString("a"))),
                InternalTypeInfo.of(schema)).setParallelism(1);
        DataStream<RowData> normalized = OdpsDynamicTableSink.normalizePartitionValues(
                input, schema, Collections.singletonList("ds"), "DEFAULT");
        normalized.addSink(new DiscardingSink<>()).setParallelism(1);
        StreamGraph graph = env.getStreamGraph();
        StreamNode mapper = graph.getStreamNode(normalized.getId());
        assertEquals("An extra channel can reorder same-key update/delete events", 1, mapper.getParallelism());
        assertTrue("Partition defaulting must keep the input's FIFO channel",
                mapper.getInEdges().get(0).getPartitioner() instanceof ForwardPartitioner);
    }

    @Test
    public void normalizationPreservesKeyOnlyDeletesAndUpdateKinds() throws Exception {
        RowType schema = RowType.of(new LogicalType[] {new BigIntType(), new VarCharType()},
                new String[] {"id", "ds"});
        DefaultPartitionValueMapper mapper = new DefaultPartitionValueMapper(
                schema, Collections.singletonList("ds"), "DEFAULT");
        for (RowKind kind : new RowKind[] {RowKind.UPDATE_AFTER, RowKind.DELETE}) {
            GenericRowData original = GenericRowData.of(1L, null);
            original.setRowKind(kind);
            RowData normalized = mapper.map(original);
            assertEquals(kind, normalized.getRowKind());
            assertEquals(1L, normalized.getLong(0));
            assertEquals("DEFAULT", normalized.getString(1).toString());
        }
    }
}
