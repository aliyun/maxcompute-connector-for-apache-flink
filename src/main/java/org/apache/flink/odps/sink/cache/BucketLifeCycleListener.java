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

package org.apache.flink.odps.sink.cache;

import org.apache.flink.odps.sink.writer.WriterInfo;

import java.util.List;

public interface BucketLifeCycleListener<IN, BucketID> {

    /**
     * Notifies a new bucket has been created.
     *
     * @param bucket The newly created bucket.
     */
    void bucketCreated(Bucket<IN, BucketID> bucket);

    /**
     * Notifies a new bucket has been bootstrap.
     *
     * @param bucket The newly created bucket.
     */
    void bucketBootstrap(Bucket<IN, BucketID> bucket);

    /**
     * Notifies a new bucket has been committing.
     *
     * @param bucket The newly created bucket.
     */
    void bucketCommitting(Bucket<IN, BucketID> bucket);

    /**
     * Notifies a bucket become inactive. A bucket becomes inactive after all the records received
     * so far have been committed.
     *
     * @param bucket The bucket getting inactive.
     */
    void bucketInactive(Bucket<IN, BucketID> bucket);


    boolean bucketsWait() throws Exception;

}