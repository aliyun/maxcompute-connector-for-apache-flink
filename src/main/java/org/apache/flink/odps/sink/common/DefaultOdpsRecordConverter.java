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

package org.apache.flink.odps.sink.common;

import com.aliyun.odps.data.ArrayRecord;

/**
 * @author dingxin ()
 */
public class DefaultOdpsRecordConverter<T> implements OdpsRecordConverter<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public void convert(Object in, ArrayRecord out) {
        if (!(in instanceof ArrayRecord)) {
            throw new IllegalArgumentException(
                "Record converter cannot be null when input type is not 'odps.data.Record'");
        }
    }
}
