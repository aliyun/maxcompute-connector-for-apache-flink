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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/** Real SDK/connector retries at every reachable write protocol stage, including lost ACKs. */
@RunWith(Parameterized.class)
public class EmulatorSqlWriteFaultITCase extends EmulatorWriteHarness {
    private final String writer, stage, effect;
    private final boolean upsert;

    @Parameterized.Parameters(name = "{0}/upsert={1}/{2}/{3}")
    public static Collection<Object[]> cases() {
        List<Object[]> cases = new ArrayList<>();
        for (boolean upsert : new boolean[] {false, true})
            for (String writer :
                    upsert
                            ? Arrays.asList("record", "cached", "modern")
                            : Arrays.asList(
                                    "record",
                                    "cached",
                                    "cached-dynamic",
                                    "grouped-cached-dynamic",
                                    "arrow",
                                    "stream-record",
                                    "modern"))
                for (String stage :
                        writer.equals("modern")
                                ? Arrays.asList("create_stream", "write", "write_late")
                                : writer.equals("stream-record")
                                        ? Arrays.asList("create", "write", "write_late")
                                        : Arrays.asList("create", "write", "write_late", "commit"))
                    for (String effect :
                            writer.contains("dynamic")
                                    ? Arrays.asList("http_error", "lost_ack", "http_throttle")
                                    : Arrays.asList("http_error", "lost_ack"))
                        if ((writer + "/" + upsert + "/" + stage + "/" + effect)
                                .matches(System.getProperty("emulator.fault.filter", ".*")))
                            cases.add(new Object[] {writer, upsert, stage, effect});
        for (String writer : Arrays.asList("cached-dynamic", "grouped-cached-dynamic")) {
            String scenario = writer + "/false/write/http_error_exhausted";
            if (scenario.matches(System.getProperty("emulator.fault.filter", ".*")))
                cases.add(new Object[] {writer, false, "write", "http_error_exhausted"});
        }
        if (cases.isEmpty()) throw new IllegalArgumentException("No fault scenario matched");
        return cases;
    }

    public EmulatorSqlWriteFaultITCase(String writer, boolean upsert, String stage, String effect) {
        this.writer = writer;
        this.upsert = upsert;
        this.stage = stage;
        this.effect = effect;
    }

    @Test(timeout = 300000)
    public void sqlWriteRecoversInjectedProtocolFailure() throws Exception {
        writeScenario(
                "fault_"
                        + writer
                        + "_"
                        + (upsert ? "upsert" : "append")
                        + "_"
                        + stage
                        + "_"
                        + effect,
                writer,
                upsert,
                writer.contains("dynamic") ? 8 : 4,
                Integer.getInteger("emulator.fault.rows", 2048),
                stage,
                effect,
                1);
    }
}
