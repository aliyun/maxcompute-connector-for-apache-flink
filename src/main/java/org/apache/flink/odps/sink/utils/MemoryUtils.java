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

package org.apache.flink.odps.sink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.ShutdownHookUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.runtime.util.ConfigurationParserUtils.checkConfigParameter;
import static org.apache.flink.util.MathUtils.checkedDownCast;

public class MemoryUtils {

    private static NetworkBufferPool networkBufferPool;;
    private static final Logger LOG = LoggerFactory.getLogger(MemoryUtils.class);
    private static Thread shutdownHook;

    public static synchronized NetworkBufferPool getGlobalBufferPool(Configuration configuration) {
        if (networkBufferPool == null) {
            int pageSize = MemoryUtils.getPageSize(configuration);
            int numberOfBuffers = MemoryUtils.calculateNumberOfBuffers(configuration, pageSize);
            networkBufferPool = new NetworkBufferPool(
                    numberOfBuffers,
                    pageSize,
                    Duration.ofMillis(30000L));

            shutdownHook = new Thread(() -> {
                try {
                    destroy();
                } catch (Throwable t) {
                    LOG.error("Error during shutdown of memory utils via JVM shutdown hook.", t);
                }
            },   "MemoryUtils shutdown hook");

            ShutdownHookUtil.addShutdownHookThread(shutdownHook, "MemoryUtils", LOG);
        }
        return networkBufferPool;
    }

    public static int getPageSize(Configuration configuration) {
        final int pageSize =
                checkedDownCast(
                        configuration.get(OdpsOptions.SINK_FILE_MEMORY_SEGMENT_SIZE).getBytes());

        // check page size of for minimum size
        checkConfigParameter(
                pageSize >= MemoryManager.MIN_PAGE_SIZE,
                pageSize,
                TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
                "Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE);
        // check page size for power of two
        checkConfigParameter(
                MathUtils.isPowerOf2(pageSize),
                pageSize,
                TaskManagerOptions.MEMORY_SEGMENT_SIZE.key(),
                "Memory segment size must be a power of 2.");

        return pageSize;
    }

    public static int calculateNumberOfBuffers(Configuration configuration, int pageSize) {
        MemorySize networkMemorySize = configuration.get(OdpsOptions.SINK_FILE_CACHE_MEMORY);
        long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
        if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The given number of memory bytes ("
                            + networkMemorySize.getBytes()
                            + ") corresponds to more than MAX_INT pages.");
        }

        return (int) numberOfNetworkBuffersLong;
    }

    public static synchronized void destroy() throws Exception {
        if (networkBufferPool != null) {
            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
            ShutdownHookUtil.removeShutdownHook(shutdownHook, "MemoryUtils", LOG);
        }
    }
}
