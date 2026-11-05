/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work by the regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

package org.apache.flink.odps.test.shuffle;

import org.apache.flink.odps.sink.shuffle.AssignmentBreakdownFormatter;
import org.apache.flink.odps.sink.shuffle.PartitionBucketKey;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AssignmentBreakdownFormatter}.
 */
public class AssignmentBreakdownFormatterTest {

    @Test
    public void smokeTest_singlePartitionSingleSubtask() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        stats.put(new PartitionBucketKey("ds=20260512", 0), 100L);
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(new PartitionBucketKey("ds=20260512", 0), 0);

        String out = AssignmentBreakdownFormatter.format(7L, stats, assignment, 1);

        assertTrue("missing header: " + out, out.contains("Assignment v7 weight breakdown"));
    }

    @Test
    public void header_includesTotalsAndCounts() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        stats.put(new PartitionBucketKey("ds=20260512", 0), 1_000_000L);
        stats.put(new PartitionBucketKey("ds=20260512", 1), 234_567L);
        stats.put(new PartitionBucketKey("ds=20260511", 0), 100L);

        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        for (PartitionBucketKey k : stats.keySet()) {
            assignment.put(k, 0);
        }

        String out = AssignmentBreakdownFormatter.format(3L, stats, assignment, 4);

        String firstLine = out.split("\n", 2)[0];
        // header: "Assignment v3 weight breakdown (totalRecords=1,234,667, partitions=2, sinkParallelism=4):"
        assertEquals(
            "Assignment v3 weight breakdown (totalRecords=1,234,667, partitions=2, sinkParallelism=4):",
            firstLine);
    }

    @Test
    public void partitions_underTopN_listsAllNoOthers() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        stats.put(new PartitionBucketKey("p1", 0), 700L);
        stats.put(new PartitionBucketKey("p2", 0), 300L);

        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(new PartitionBucketKey("p1", 0), 0);
        assignment.put(new PartitionBucketKey("p2", 0), 0);

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 1);

        assertTrue(out.contains("top partitions by records:"));
        assertTrue("p1 row missing: " + out, out.contains("    p1 : 700 (70.0%)"));
        assertTrue("p2 row missing: " + out, out.contains("    p2 : 300 (30.0%)"));
        assertFalse("should not have others line for 2 partitions: " + out,
                out.contains("others:"));
        // Ordering: p1 (heavier) appears before p2.
        assertTrue(out.indexOf("p1 :") < out.indexOf("p2 :"));
    }

    @Test
    public void partitions_overTopN_collapsesIntoOthers() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        // 12 partitions: p00 (heaviest) ... p11 (lightest)
        for (int i = 0; i < 12; i++) {
            stats.put(new PartitionBucketKey(String.format("p%02d", i), 0), (long) (1000 - i * 50));
        }
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        for (PartitionBucketKey k : stats.keySet()) {
            assignment.put(k, 0);
        }

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 1);

        // Top 10 must be present.
        for (int i = 0; i < 10; i++) {
            String prefix = String.format("    p%02d :", i);
            assertTrue("missing top-10 row " + prefix + " in: " + out, out.contains(prefix));
        }
        // p10 and p11 must NOT have their own rows.
        assertFalse(out.contains("    p10 :"));
        assertFalse(out.contains("    p11 :"));
        // others line summarizes the remaining 2 partitions.
        // p10 = 500, p11 = 450 → 950, total = 1000+950+...+450 = 8700, 950/8700 ≈ 10.9%
        assertTrue("missing others summary: " + out,
                out.contains("    others: 2 partitions, 950 (10.9%)"));
    }

    @Test
    public void partitions_emptySpec_rendersAsRoot() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        stats.put(new PartitionBucketKey("", 0), 50L);
        stats.put(new PartitionBucketKey("", 1), 50L);

        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(new PartitionBucketKey("", 0), 0);
        assignment.put(new PartitionBucketKey("", 1), 0);

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 1);

        assertTrue("missing <root> row: " + out, out.contains("    <root> : 100 (100.0%)"));
    }

    @Test
    public void subtasks_smallParallelism_listsAllSorted() {
        // sinkParallelism = 4. Loads: subtask 0=400, 1=100, 2=300, 3=200.
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        long[] loads = {400L, 100L, 300L, 200L};
        for (int sub = 0; sub < loads.length; sub++) {
            PartitionBucketKey k = new PartitionBucketKey("p" + sub, 0);
            stats.put(k, loads[sub]);
            assignment.put(k, sub);
        }

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 4);

        // total = 1000, avg = 250, max = 400 → skew = 1.60x
        assertTrue("header missing avg/skew: " + out,
                out.contains("subtask load (avg=250 skew(max/avg)=1.60x):"));
        assertTrue(out.contains("    all subtasks by load:"));
        assertFalse("must not have top/bottom split for sinkParallelism=4: " + out,
                out.contains("top 5 by load:"));
        assertFalse(out.contains("bottom 5 by load:"));
        assertFalse(out.contains("more ..."));

        // Order: #0=400, #2=300, #3=200, #1=100. Index width is 1 (max index = 3).
        int i0 = out.indexOf("#0 : 400");
        int i2 = out.indexOf("#2 : 300");
        int i3 = out.indexOf("#3 : 200");
        int i1 = out.indexOf("#1 : 100");
        assertTrue("#0 row missing: " + out, i0 >= 0);
        assertTrue("#2 row missing: " + out, i2 >= 0);
        assertTrue("#3 row missing: " + out, i3 >= 0);
        assertTrue("#1 row missing: " + out, i1 >= 0);
        assertTrue("rows not in load-desc order", i0 < i2 && i2 < i3 && i3 < i1);
    }

    @Test
    public void subtasks_atBoundaryTen_takesSmallParallelismPath() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        for (int sub = 0; sub < 10; sub++) {
            PartitionBucketKey k = new PartitionBucketKey("p" + sub, 0);
            stats.put(k, 10L);
            assignment.put(k, sub);
        }

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 10);

        assertTrue(out.contains("    all subtasks by load:"));
        assertFalse(out.contains("top 5 by load:"));
    }

    @Test
    public void subtasks_largeParallelism_topAndBottomWithGap() {
        // sinkParallelism = 11 (smallest > 10). Loads: subtask i gets (i+1)*10.
        // totalRecords = 10+20+...+110 = 660. Index width is 2.
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        for (int sub = 0; sub < 11; sub++) {
            PartitionBucketKey k = new PartitionBucketKey("p" + sub, 0);
            stats.put(k, (long) ((sub + 1) * 10));
            assignment.put(k, sub);
        }

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 11);

        assertTrue(out.contains("    top 5 by load:"));
        assertTrue("expected gap line for 11-10=1 more: " + out,
                out.contains("    ... 1 more ..."));
        assertTrue(out.contains("    bottom 5 by load:"));

        // Top 5 are subtasks 10, 9, 8, 7, 6 (loads 110, 100, 90, 80, 70).
        int top10 = out.indexOf("#10 : 110");
        int top9  = out.indexOf("#9  : 100");
        assertTrue("#10 row missing: " + out, top10 >= 0);
        assertTrue("#9 row missing: " + out, top9 >= 0);
        assertTrue("top order wrong", top10 < top9);

        // Bottom 5 are subtasks 0, 1, 2, 3, 4 (loads 10, 20, 30, 40, 50).
        int bot0 = out.indexOf("#0  : 10");
        int bot1 = out.indexOf("#1  : 20");
        assertTrue("#0 row missing: " + out, bot0 >= 0);
        assertTrue("bottom order wrong", bot0 < bot1);

        // Subtask 5 (load 60) appears in neither section (the "1 more").
        assertFalse("middle subtask should be hidden: " + out,
                out.contains("#5  : 60"));
    }

    @Test
    public void subtasks_largeParallelism_fullySkewedAssignment() {
        // sinkParallelism = 32, all weight on subtask 7. Index width = 2.
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        PartitionBucketKey k = new PartitionBucketKey("hot", 0);
        stats.put(k, 1_000_000L);
        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(k, 7);

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 32);

        // Top-5 first row is the hot subtask with 100.0%.
        assertTrue("hot subtask row missing: " + out,
                out.contains("#7  : 1,000,000 (100.0%)"));
        // The next four top-5 rows are 0-load with the lowest indices that aren't 7
        // (0, 1, 2, 3) by tiebreak.
        assertTrue(out.contains("#0  : 0 (0.0%)"));
        assertTrue(out.contains("#1  : 0 (0.0%)"));
        assertTrue(out.contains("#2  : 0 (0.0%)"));
        assertTrue(out.contains("#3  : 0 (0.0%)"));
        // Gap: 32 - 10 = 22 more.
        assertTrue(out.contains("    ... 22 more ..."));
        // skew matches formula: max=1_000_000, avg=1_000_000/32=31250, skew=1_000_000/31250=32.00x.
        assertTrue("skew header missing: " + out, out.contains("skew(max/avg)=32.00x"));
    }

    @Test
    public void zeroTotal_rendersNaForPercentAndSkew() {
        Map<PartitionBucketKey, Long> stats = new HashMap<>();
        PartitionBucketKey k = new PartitionBucketKey("p", 0);
        stats.put(k, 0L);

        Map<PartitionBucketKey, Integer> assignment = new HashMap<>();
        assignment.put(k, 0);

        String out = AssignmentBreakdownFormatter.format(1L, stats, assignment, 4);

        assertTrue("partition row should render n/a percent: " + out,
                out.contains("    p : 0 (n/a)"));
        assertTrue("subtask header should render n/a skew: " + out,
                out.contains("subtask load (avg=0 skew(max/avg)=n/a):"));
        // sinkParallelism=4 → idxWidth=1 → row is "#0 : 0 (n/a)"
        assertTrue("subtask row should render n/a percent: " + out,
                out.contains("#0 : 0 (n/a)"));
    }
}
