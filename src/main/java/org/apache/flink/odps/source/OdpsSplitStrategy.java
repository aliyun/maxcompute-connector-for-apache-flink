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
 * Modified for the MaxCompute Flink connector; see NOTICE for upstream attribution.
 */

package org.apache.flink.odps.source;

import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.odps.source.enumerator.BatchOdpsSplitEnumerator;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Split strategy for {@link BatchOdpsSplitEnumerator}.
 */
public enum OdpsSplitStrategy {
    BYTE_SIZE {
        @Override
        public SplitOptions.Builder init(long splitByteSize) {
            return SplitOptions.newBuilder().SplitByByteSize(splitByteSize);
        }

        @Override
        public InputSplit[] getSplits(InputSplitAssigner assigner) {
            return assigner.getAllSplits();
        }
    },
    ROW_OFFSET {
        @Override
        public SplitOptions.Builder init(long splitByteSize) {
            return SplitOptions.newBuilder().SplitByRowOffset();
        }

        @Override
        public InputSplit[] getSplits(InputSplitAssigner assigner) {
            long rows = assigner.getTotalRowCount();
            return rows == 0 ? new InputSplit[0]
                    : new InputSplit[]{assigner.getSplitByRowOffset(0, rows)};
        }

        @Override
        public List<InputSplit> postProcessSplits(
                InputSplit[] splits, SplitEnumeratorContext<?> context) {
            if (splits.length == 0) {
                return java.util.Collections.emptyList();
            }
            Preconditions.checkArgument(splits.length == 1);
            Preconditions.checkArgument(splits[0] instanceof RowRangeInputSplit);
            RowRangeInputSplit split = (RowRangeInputSplit) splits[0];
            Preconditions.checkArgument(split.getRowRange().getStartIndex() == 0);

            long numRecords = split.getRowRange().getNumRecord();
            String sessionId = split.getSessionId();
            long recordsPerSplit = Math.max(numRecords / ((long) context.currentParallelism()), 1);
            List<InputSplit> result = new ArrayList<>();
            for (long offset = 0; offset < numRecords; offset += recordsPerSplit) {
                result.add(
                        new RowRangeInputSplit(
                                sessionId, offset, Math.min(recordsPerSplit, numRecords - offset)));
            }
            return result;
        }
    },
    BUCKET {
        @Override
        public SplitOptions.Builder init(long splitByteSize) {
            return SplitOptions.newBuilder().SplitByBucket();
        }

        @Override
        public InputSplit[] getSplits(InputSplitAssigner assigner) {
            return assigner.getAllSplits();
        }
    },
    ;

    public abstract SplitOptions.Builder init(long splitByteSize);

    public abstract InputSplit[] getSplits(InputSplitAssigner assigner);

    public List<InputSplit> postProcessSplits(
            InputSplit[] splits, SplitEnumeratorContext<?> context) {
        return Arrays.asList(splits);
    }
}
