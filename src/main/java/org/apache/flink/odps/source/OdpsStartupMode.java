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

public enum OdpsStartupMode {

    /**
     * Bounded read mode. Even in streaming execution mode, performs a bounded read
     * using BatchOdpsSource. Reads the latest snapshot and then finishes.
     * This is useful when users want to use streaming execution mode for its resource
     * management benefits but only need to read a finite dataset.
     */
    BOUNDED("bounded",
            "Performs a bounded read even in streaming mode. "
                    + "Reads the latest snapshot using BatchOdpsSource and then finishes. "
                    + "Useful for one-time data processing in streaming execution mode."),

    INCREMENTAL(
            "incremental",
            "Read incremental changes between start and end snapshot or timestamp."),

    LATEST("latest",
            "For streaming sources, continuously reads latest changes "
                    + "without producing a snapshot at the beginning. "
                    + "For batch sources, behaves the same as the \"latest-full\" startup mode."),

    LATEST_FULL("latest-full",
            "For streaming sources, produces the latest snapshot on the table upon first startup, "
                    + "and continue to read the latest changes. "
                    + "For batch sources, just produce the latest snapshot but does not read new changes."),

    FROM_TIMESTAMP("from-timestamp",
            "For streaming sources, continuously reads changes "
                    + "starting from timestamp specified by \"scan.timestamp-millis\", "
                    + "without producing a snapshot at the beginning. "
                    + "For batch sources, produces a snapshot at timestamp specified by \"scan.timestamp-millis\" "
                    + "but does not read new changes."),

    FROM_SNAPSHOT(
            "from-snapshot",
            "For streaming sources, continuously reads changes starting from snapshot "
                    + "specified by \"scan.snapshot-id\", without producing a snapshot at the beginning. "
                    + "For batch sources, produces a snapshot specified by \"scan.snapshot-id\" "
                    + "or \"scan.tag-name\" but does not read new changes."),

    // Not support now
    FROM_SNAPSHOT_FULL(
            "from-snapshot-full",
            "For streaming sources, produces from snapshot specified by \"scan.snapshot-id\" "
                    + "on the table upon first startup, and continuously reads changes. " +
                    "For batch sources, produces a snapshot specified by \"scan.snapshot-id\" but does not read new changes.");


    private final String value;
    private final String description;

    OdpsStartupMode(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value;
    }
}