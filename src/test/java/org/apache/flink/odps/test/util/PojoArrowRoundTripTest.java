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
package org.apache.flink.odps.test.util;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.flink.odps.util.pojo.POJO2ArrowConverter;
import org.junit.Test;
import java.io.*;
import java.nio.channels.Channels;
import java.util.*;
import static org.junit.Assert.*;

/** Public POJO API is not reached by SQL; verify its emitted IPC and reuse separately. */
public class PojoArrowRoundTripTest {
    @Test public void scalarsNestedContainersNullsAndResetSurviveIpc() throws Exception {
        try(RootAllocator allocator=new RootAllocator(16*1024*1024)) {
            POJO2ArrowConverter converter=new POJO2ArrowConverter(AllTypes.class,allocator);
            converter.newInstance();
            try {
                converter.write(new AllTypes());converter.write(null);converter.finish();
                assertEquals(2,converter.getRowCount());
                byte[] ipc=encode(converter.getArrowBatch());
                try(ArrowStreamReader reader=new ArrowStreamReader(new ByteArrayInputStream(ipc),allocator)) {
                    assertTrue(reader.loadNextBatch());VectorSchemaRoot root=reader.getVectorSchemaRoot();
                    assertEquals(2,root.getRowCount());
                    assertEquals((byte)-128,root.getVector("tiny").getObject(0));
                    assertEquals((short)32767,root.getVector("small").getObject(0));
                    assertEquals(Integer.MIN_VALUE,root.getVector("integer").getObject(0));
                    assertEquals(Long.MAX_VALUE,root.getVector("big").getObject(0));
                    assertEquals(1.25f,root.getVector("single").getObject(0));
                    assertEquals(-2.5d,root.getVector("doubleValue").getObject(0));
                    assertEquals(true,root.getVector("active").getObject(0));
                    assertEquals("中文",root.getVector("text").getObject(0).toString());
                    assertEquals(Arrays.asList(Arrays.asList(1L,null),Collections.emptyList()),root.getVector("nested").getObject(0));
                    Map<?,?> entry=(Map<?,?>)((List<?>)root.getVector("mapping").getObject(0)).get(0);
                    assertEquals("a",entry.get("key").toString());assertEquals(Arrays.asList(9L,null),entry.get("value"));
                    StructVector child=(StructVector)root.getVector("child");
                    assertEquals(7L,child.getChild("id").getObject(0));assertEquals(false,child.getChild("enabled").getObject(0));
                    for(org.apache.arrow.vector.FieldVector vector:root.getFieldVectors())assertTrue(vector.getName(),vector.isNull(1));
                    assertFalse(reader.loadNextBatch());
                }
                converter.reset();assertEquals(0,converter.getRowCount());converter.write(new AllTypes());converter.finish();
                assertEquals(1,converter.getArrowBatch().getRowCount());
                assertThrows(IllegalArgumentException.class,()->converter.write("wrong class"));
            } finally {converter.getArrowBatch().close();}
        }
    }
    @Test public void missingGettersAndUnsupportedTypesFailBeforeAllocatingVectors() {
        try(RootAllocator allocator=new RootAllocator(1024)) {
            assertThrows(IllegalArgumentException.class,()->new POJO2ArrowConverter(MissingGetter.class,allocator));
            assertThrows(UnsupportedOperationException.class,()->new POJO2ArrowConverter(UnsupportedField.class,allocator));
            assertThrows(UnsupportedOperationException.class,()->new POJO2ArrowConverter(WildcardField.class,allocator));
            assertEquals(0,allocator.getAllocatedMemory());
        }
    }
    private static byte[] encode(VectorSchemaRoot root)throws Exception {
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        try(ArrowStreamWriter writer=new ArrowStreamWriter(root,null,Channels.newChannel(bytes))){writer.start();writer.writeBatch();writer.end();}
        return bytes.toByteArray();
    }
    public static class AllTypes {
        private Byte tiny=(byte)-128;
        public Byte getTiny(){return tiny;}
        private Short small=(short)32767;
        public Short getSmall(){return small;}
        private Integer integer=Integer.MIN_VALUE;
        public Integer getInteger(){return integer;}
        private Long big=Long.MAX_VALUE;
        public Long getBig(){return big;}
        private Float single=1.25f;
        public Float getSingle(){return single;}
        private Double doubleValue=-2.5d;
        public Double getDoubleValue(){return doubleValue;}
        private Boolean active=true;
        public Boolean isActive(){return active;}
        private String text="中文";
        public String getText(){return text;}
        private List<List<Long>> nested=Arrays.asList(Arrays.asList(1L,null),Collections.emptyList());
        public List<List<Long>> getNested(){return nested;}
        private Map<String,List<Long>> mapping=Collections.singletonMap("a",Arrays.asList(9L,null));
        public Map<String,List<Long>> getMapping(){return mapping;}
        private Child child=new Child();
        public Child getChild(){return child;}
    }
    public static class Child {
        private long id=7;
        private boolean enabled=false;
        public long getId(){return id;}
        public boolean isEnabled(){return enabled;}
    }
    public static class MissingGetter {private long id;}
    public static class UnsupportedField {private char value;public char getValue(){return value;}}
    public static class WildcardField {private List<?> value;public List<?> getValue(){return value;}}
}
