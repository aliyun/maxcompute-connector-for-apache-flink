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

/** Checkpoint timeout must trigger restart, restore progress and preserve every input key. */
public class EmulatorYieldRecoveryITCase extends EmulatorInsertMailboxReproducer {
    @Override
    protected long checkpointIntervalMillis() {
        return 2000;
    }

    @Override
    protected long checkpointMinPauseMillis() {
        return 1000;
    }

    @Override
    protected boolean dynamic() {
        return true;
    }

    @Override
    protected int parallelism() {
        return 3;
    }

    @Override
    protected int restartAttempts() {
        return 1;
    }

    @Override
    protected boolean expectRecovery() {
        return true;
    }

    @Override
    protected int completionTimeoutSeconds() {
        return 30;
    }

    @Override
    protected String reportLabel() {
        return "recovery-";
    }
}
