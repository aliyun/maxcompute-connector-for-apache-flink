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

package org.apache.flink.odps.source.partition;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.odps.source.split.OdpsSourceSplit;

/** Ordered coordinator/reader handshake, for stopping the old generation. */
public final class PartitionEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;
    public enum Kind { SWITCH, READY }
    public final Kind kind;
    public final long epoch;

    public PartitionEvent(Kind kind, long epoch) {
        this.kind = kind;
        this.epoch = epoch;
    }

    public static PartitionEvent switchTo(long epoch) {
        return new PartitionEvent(Kind.SWITCH, epoch);
    }

    public static String prefix(long epoch) {
        return "latest-partition/" + epoch + "/";
    }

    public static OdpsSourceSplit marker(long epoch) {
        return new OdpsSourceSplit("latest-partition-epoch/" + epoch, -1, null, null);
    }

    public static boolean isMarker(OdpsSourceSplit split) {
        return split.splitId().startsWith("latest-partition-epoch/");
    }

    public static long markerEpoch(OdpsSourceSplit split) {
        return Long.parseLong(split.splitId().substring("latest-partition-epoch/".length()));
    }
}
