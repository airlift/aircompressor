/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v3.zstdFFM;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.List;

import static io.airlift.compress.v3.zstdFFM.Util.get24BitLittleEndian;
import static io.airlift.compress.v3.zstdFFM.Util.put24BitLittleEndian;
import static org.assertj.core.api.Assertions.assertThat;

class TestUtil
{
    private final List<TestData> test24bitIntegers = ImmutableList.<TestData>builder()
            .add(new TestData(new byte[] {1, 0, 0, 0}, 0, 1))
            .add(new TestData(new byte[] {12, -83, 0, 0}, 0, 44300))
            .add(new TestData(new byte[] {0, 0, -128}, 0, 8388608))
            .add(new TestData(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, 0, 16777215))
            .add(new TestData(new byte[] {63, 25, 72, 0}, 0, 4725055))
            .add(new TestData(new byte[] {0, 0, 0, 0, 0, 0, 1, 0, 0}, 6, 1))
            .build();

    @Test
    void testGet24BitLittleEndian()
    {
        for (TestData testData : test24bitIntegers) {
            testGet24BitLittleEndian(testData);
        }
    }

    private static void testGet24BitLittleEndian(TestData testData)
    {
        MemorySegment input = MemorySegment.ofArray(testData.bytes);
        assertThat(get24BitLittleEndian(input, testData.offset)).isEqualTo(testData.value);
    }

    @Test
    void testPut24BitLittleEndian()
    {
        for (TestData testData : test24bitIntegers) {
            testPut24BitLittleEndian(testData);
        }
    }

    private static void testPut24BitLittleEndian(TestData testData)
    {
        byte[] outputBuffer = new byte[testData.bytes.length];
        MemorySegment output = MemorySegment.ofArray(outputBuffer);
        put24BitLittleEndian(output, testData.offset, testData.value);
        assertThat(outputBuffer).isEqualTo(testData.bytes);
    }

    private record TestData(byte[] bytes, int offset, int value) {}
}
