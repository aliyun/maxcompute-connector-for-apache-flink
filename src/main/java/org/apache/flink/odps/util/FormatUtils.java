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

package org.apache.flink.odps.util;

import java.text.DecimalFormat;

/**
 * @author dingxin ()
 */
public class FormatUtils {

    /**
     * 计算并格式化传输速度为人类可读的字符串 (例如 "12.34 MB/s").
     *
     * @param timeInMillis 消耗的时间，单位为毫秒 (ms)
     * @param bytes        传输的字节数
     * @return 格式化后的速度字符串，如 "1.25 MB/s" 或 "800.00 KB/s"
     */
    public static String getFormattedSpeed(long timeInMillis, long bytes) {
        // 防止除以零或无效输入
        if (timeInMillis <= 0 || bytes <= 0) {
            return "0 B/s";
        }

        // 1. 计算速度，单位：字节/秒 (B/s)
        // (bytes / (timeInMillis / 1000.0)) 等价于 (bytes * 1000.0 / timeInMillis)
        // 使用 1000.0 进行浮点数运算，保证精度
        double speedBps = (double)bytes * 1000.0 / timeInMillis;

        // 2. 动态选择合适的单位
        String[] units = {"B/s", "KB/s", "MB/s", "GB/s", "TB/s"};
        int unitIndex = 0;
        double speed = speedBps;

        // 当速度大于等于1024且还有更大的单位时，进行单位转换
        while (speed >= 1024 && unitIndex < units.length - 1) {
            speed /= 1024;
            unitIndex++;
        }

        // 3. 格式化为两位小数的字符串
        // 使用 DecimalFormat 也是一个很好的选择，它能更好地处理非常小的数字
        DecimalFormat df = new DecimalFormat("#.##");
        return df.format(speed) + " " + units[unitIndex];
    }
}
