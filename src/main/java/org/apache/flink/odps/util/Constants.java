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

package org.apache.flink.odps.util;

public class Constants {
    public static final String DEFAULT_TABLE_API_PROVIDER = "cupid-native";
    public static final String TUNNEL_TABLE_API_PROVIDER = "tunnel";
    public static final String MAX_STORAGE_TABLE_API_PROVIDER = "v1";

    public static final int MAX_FLINK_TUPLE_SIZE = 25;

    public static final int MAX_CHAR_LENGTH = 255;
    public static final int MAX_VARCHAR_LENGTH = 65535;

    public static final int MAX_DECIMAL_PRECISION = 38;
    public static final int MAX_DECIMAL_SCALE = 18;

    public static final String ODPS_META_CACHE_SIZE = "odps.meta.cache.size";
    public static final String ODPS_META_CACHE_EXPIRE_TIME = "odps.meta.cache.expire.time";
    public static final int DEFAULT_ODPS_META_CACHE_SIZE = 4000;
    public static final int DEFAULT_ODPS_META_CACHE_EXPIRE_TIME = 1200;

    public static final String ODPS_CONF_DIR = "ODPS_CONF_DIR";
    public static final String ODPS_TABLE = "table.name";

    public static final String ODPS_PROJECT_NAME = "odps.project.name";
    public static final String ODPS_ACCESS_ID = "odps.access.id";
    public static final String ODPS_ACCESS_KEY = "odps.access.key";
    public static final String ODPS_END_POINT = "odps.end.point";
    public static final String ODPS_CREDENTIALS_PROVIDER = "odps.credentials.provider";
    public static final String ODPS_CREDENTIALS_PROVIDER_PARAMS = "odps.credentials.provider.params";
    public static final String ODPS_HTTP_PROXY = "odps.http.proxy";
    public static final String ODPS_HTTPS_PROXY = "odps.https.proxy";
    public static final String ODPS_PROXY_DISABLE_RESOLVER = "odps.proxy.disable.resolver";
    public static final String ODPS_TUNNEL_END_POINT = "odps.tunnel.end.point";
    public static final String ODPS_TUNNEL_QUOTA_NAME = "odps.tunnel.quota.name";
    public static final String ODPS_VECTORIZED_WRITE_ENABLE = "odps.vectorized.writer.enable";
    public static final String ODPS_VECTORIZED_READ_ENABLE = "odps.vectorized.reader.enable";
    public static final String ODPS_VECTORIZED_BATCH_SIZE = "odps.vectorized.batch.size";
    public static final int DEFAULT_ODPS_VECTORIZED_BATCH_SIZE = 4096;

    public static final String ODPS_INPUT_SPLIT_SIZE = "odps.input.split.size";
    public static final int DEFAULT_SPLIT_SIZE = 256;

    public static final int DEFAULT_CHECK_COMMIT_INTERVAL = 15 * 60 * 1000;
    public static final long DEFAULT_COMMIT_POLICY_INTERVAL = 60 * 60 * 1000;
    public static final long DEFAULT_COMMIT_POLICY_BLOCK_COUNT = 15000L;
    public static final boolean DEFAULT_ROLL_ON_CHECKPOINT = false;

    public static final String ODPS_META_OP_TYPE = "__meta_op_type";
    public static final String ODPS_META_TIMESTAMP = "__meta_timestamp";

    public static final String ODPS_OP = "__operation";
}
