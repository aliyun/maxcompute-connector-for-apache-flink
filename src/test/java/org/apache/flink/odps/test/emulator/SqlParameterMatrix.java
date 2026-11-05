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

package org.apache.flink.odps.test.emulator;

import java.util.*;

/** Deterministic pair coverage plus the critical split/codec/cache interaction. */
final class SqlParameterMatrix {
    static List<String[]> scenarios() {
        String[][] dimensions = {
            {"ROW_OFFSET", "BYTE_SIZE"},
            {"NO_COMPRESSION", "ZSTD", "LZ4_FRAME"},
            {"1", "3"},
            {"1", "2"},
            {"false", "true"},
            {"all", "filter"},
            {"rows", "empty"}
        };
        List<String[]> candidates = new ArrayList<>();
        expand(dimensions, 0, new String[dimensions.length], candidates);
        Set<String> remaining = new HashSet<>();
        for (String[] row : candidates) remaining.addAll(interactions(row));
        List<String[]> selected = new ArrayList<>();
        while (!remaining.isEmpty()) {
            String[] best = null;
            int score = 0;
            for (String[] row : candidates) {
                int covered = 0;
                for (String key : interactions(row)) if (remaining.contains(key)) covered++;
                if (covered > score) {
                    best = row;
                    score = covered;
                }
            }
            if (best == null) throw new AssertionError("Uncovered SQL parameter interactions");
            selected.add(best);
            remaining.removeAll(interactions(best));
            candidates.remove(best);
        }
        return selected;
    }

    private static Set<String> interactions(String[] row) {
        Set<String> result = new HashSet<>();
        for (int a = 0; a < row.length; a++)
            for (int b = a + 1; b < row.length; b++)
                result.add(a + "=" + row[a] + "/" + b + "=" + row[b]);
        if (row[6].equals("rows")) {
            result.add("critical=" + row[0] + "/" + row[1] + "/" + row[4]);
            for (int a = 0; a < 6; a++)
                for (int b = a + 1; b < 6; b++)
                    result.add("active=" + a + "=" + row[a] + "/" + b + "=" + row[b]);
        }
        return result;
    }

    private static void expand(String[][] dims, int index, String[] row, List<String[]> out) {
        if (index == dims.length) {
            out.add(row.clone());
            return;
        }
        for (String value : dims[index]) {
            row[index] = value;
            expand(dims, index + 1, row, out);
        }
    }

    private SqlParameterMatrix() {}
}
