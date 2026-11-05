/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
package org.apache.flink.odps.shim;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

/** Compile-time adaptation of the local file-cache view for Flink 1.16 and 1.17. */
public interface FlinkSubpartitionView extends ResultSubpartitionView {
    AvailabilityWithBacklog getAvailabilityAndBacklog(boolean creditAvailable);
    int peekNextBufferSubpartitionId();

    @Override
    default AvailabilityWithBacklog getAvailabilityAndBacklog(int credits) {
        return getAvailabilityAndBacklog(credits > 0);
    }

    static void notifyAvailable(BufferAvailabilityListener listener, ResultSubpartitionView view) {
        listener.notifyDataAvailable();
    }

    static BufferAvailabilityListener noOpListener() {
        return () -> {};
    }
}
