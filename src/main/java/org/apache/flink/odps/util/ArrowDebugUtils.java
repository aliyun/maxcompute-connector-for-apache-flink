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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Base64;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dingxin ()
 */
public class ArrowDebugUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowDebugUtils.class);

    public static String saveVectorSchemaRootToBase64(VectorSchemaRoot root) throws IOException {
        if (root == null) {
            LOG.error("[arrowdebug] get null vector");
            return "";
        }
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, outStream)) {
            writer.start();
            writer.writeBatch();
            writer.end();
            outStream.flush();
        }
        byte[] arrowBytes = outStream.toByteArray();
        return Base64.getEncoder().encodeToString(arrowBytes);
    }

    public static VectorSchemaRoot cloneVectorSchemaRoot(VectorSchemaRoot original, RootAllocator allocator) {
        // 从原始 VectorSchemaRoot 获取 Schema
        Schema originalSchema = original.getSchema();
        // 根据 Schema 创建新的 VectorSchemaRoot
        VectorSchemaRoot copy = VectorSchemaRoot.create(originalSchema, allocator);
        // 逐个克隆原始 SchemaRoot 中的 Vector
        for (int i = 0; i < original.getFieldVectors().size(); i++) {
            FieldVector originalVector = original.getVector(i);
            FieldVector copyVector = copy.getVector(i);
            // 将原始 Vector 的内容复制到新 Vector 中
            originalVector.makeTransferPair(copyVector).splitAndTransfer(0, original.getRowCount());
        }
        // 设置新 VectorSchemaRoot 的行数
        copy.setRowCount(original.getRowCount());
        return copy;
    }

    public static VectorSchemaRoot loadVectorSchemaRootFromBase64(String base64, BufferAllocator allocator)
        throws IOException {
        if (base64 == null || base64.isEmpty()) {
            throw new IllegalArgumentException("Base64 string is null or empty!");
        }

        // Decode the Base64 encoded string to get the byte array
        byte[] decodedBytes = Base64.getDecoder().decode(base64);
        // Create a ByteArrayInputStream to read from the byte array
        ByteArrayInputStream inputStream = new ByteArrayInputStream(decodedBytes);

        // Use ArrowStreamReader to read Arrow data from the ByteArrayInputStream
        try (ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator)) {
            // Ensure the data is loaded into the vectors
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            // Make a deep copy of the VectorSchemaRoot to return a cloned instance
            VectorSchemaRoot clonedRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
            for (FieldVector originalVector : root.getFieldVectors()) {
                FieldVector clonedVector = clonedRoot.getVector(originalVector.getField().getName());
                originalVector.makeTransferPair(clonedVector).transfer();
            }
            clonedRoot.setRowCount(root.getRowCount());

            return clonedRoot;
        }
        // InputStream and reader are automatically closed at the end of the try block
    }

    public static void logStackTrace(Throwable throwable) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        throwable.printStackTrace(ps);
        String stackTrace = baos.toString();
        LOG.error(stackTrace);
    }

    public static void writeVectorSchemaRootToFile(VectorSchemaRoot root, String filePath) throws Exception {
        File file = new File(filePath);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
             ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    public static VectorSchemaRoot readVectorSchemaRootFromFile(String filePath) throws Exception {
        File file = new File(filePath);
        VectorSchemaRoot root;
        try (FileInputStream fileInputStream = new FileInputStream(file);
             ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), ArrowUtils.getRootAllocator())) {
            if (!reader.loadNextBatch()) {
                throw new IOException("Arrow diagnostic file contains no record batch");
            }
            root = new org.apache.flink.odps.sink.serializer.VectorSchemaRootSerializer()
                    .copy(reader.getVectorSchemaRoot());
        }
        return root;
    }

}
