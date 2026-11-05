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

package org.apache.flink.odps.sink.datastream;

import java.io.Serializable;

import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;

/**
 * @author dingxin ()
 */
public interface SinkInterceptor extends Serializable {
    default void beforeInvoke(Object value, Context context) throws Exception {}

    default boolean invokeFilter(Object value, Context context) throws Exception {
        return true;
    }

    default void afterInvoke(Object value, Context context) throws Exception {}

    default void beforeCheckpoint(FunctionSnapshotContext context) throws Exception {}

    default boolean checkpointFilter(FunctionSnapshotContext context) throws Exception {
        return true;
    }

    default void afterCheckpoint(FunctionSnapshotContext context) throws Exception {}
}
