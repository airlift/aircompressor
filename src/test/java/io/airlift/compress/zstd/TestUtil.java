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
package io.airlift.compress.zstd;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.airlift.compress.zstd.Util.get24BitLittleEndian;
import static io.airlift.compress.zstd.Util.put24BitLittleEndian;
import static org.testng.Assert.assertEquals;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class TestUtil
{
    @DataProvider(name = "test24bitIntegers")
    public static Object[][] test24bitIntegers()
    {
        return new Object[][] {
                {new byte[]{1, 0, 0, 0}, 0, 1},
                {new byte[]{12, -83, 0, 0}, 0, 44300},
                {new byte[]{0, 0, -128}, 0, 8388608},
                {new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, 0, 16777215},
                {new byte[]{63, 25, 72, 0}, 0, 4725055},
                {new byte[]{0, 0, 0, 0, 0, 0, 1, 0, 0}, 6, 1}
        };
    }

    @Test(dataProvider = "test24bitIntegers")
    public void testGet24BitLittleEndian(byte[] bytes, int offset, int value)
    {
        long inputAddress = ARRAY_BYTE_BASE_OFFSET + offset;
        assertEquals(get24BitLittleEndian(bytes, inputAddress), value);
    }

    @Test(dataProvider = "test24bitIntegers")
    public void testPut24BitLittleEndian(byte[] bytes, int offset, int value)
    {
        Object outputBase = new byte[bytes.length];
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + offset;
        put24BitLittleEndian(outputBase, outputAddress, value);
        assertEquals(outputBase, bytes);
    }
}
