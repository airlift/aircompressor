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
package io.airlift.compress;

import io.airlift.compress.benchmark.DataSet;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.testng.Assert.assertEquals;

@Guice(modules = TestingModule.class)
public abstract class AbstractTestCompression
{
    private List<TestCase> testCases;

    protected abstract byte[] prepareCompressedData(byte[] uncompressed);

    protected abstract Compressor getCompressor();
    protected abstract Decompressor getDecompressor();

    @Inject
    public void setup(List<DataSet> dataSets)
    {
        testCases = new ArrayList<>();

        testCases.add(createTestCase("short literal", "hello world!".getBytes(UTF_8)));
        testCases.add(createTestCase("small copy", "XXXXabcdabcdABCDABCDwxyzwzyz123".getBytes(UTF_8)));
        testCases.add(createTestCase("long copy", "XXXXabcdefgh abcdefgh abcdefgh abcdefgh abcdefgh abcdefgh ABC".getBytes(UTF_8)));

        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        testCases.add(createTestCase("long literal", data));

        for (DataSet dataSet : dataSets) {
            byte[] uncompressed = dataSet.getUncompressed();
            testCases.add(createTestCase(dataSet.getName(), uncompressed));
        }
    }

    @Test(dataProvider = "data")
    public void testDecompress(TestCase testCase)
            throws Exception
    {
        byte[] uncompressed = new byte[testCase.uncompressed.length];

        Decompressor decompressor = getDecompressor();
        int written = decompressor.decompress(
                testCase.compressed,
                0,
                testCase.compressed.length,
                uncompressed,
                0,
                uncompressed.length);

        assertByteArraysEqual(testCase.uncompressed, 0, testCase.uncompressed.length, uncompressed, 0, written);
    }

    @Test(dataProvider = "data")
    public void testCompress(TestCase testCase)
            throws Exception
    {
        Compressor compressor = getCompressor();

        byte[] compressed = new byte[compressor.maxCompressedLength(testCase.uncompressed.length)];

        int written = compressor.compress(
                testCase.uncompressed,
                0,
                testCase.uncompressed.length,
                compressed,
                0,
                compressed.length);

        byte[] uncompressed = new byte[testCase.uncompressed.length];
        // TODO: validate with "control" decompressor
        int decompressedSize = getDecompressor().decompress(compressed, 0, written, uncompressed, 0, uncompressed.length);

        assertByteArraysEqual(testCase.uncompressed, 0, testCase.uncompressed.length, uncompressed, 0, decompressedSize);
    }

    @DataProvider(name = "data")
    public Object[][] getTestCases()
            throws IOException
    {
        Object[][] result = new Object[testCases.size()][];

        for (int i = 0; i < testCases.size(); i++) {
            result[i] = new Object[] {testCases.get(i)};
        }

        return result;
    }

    private TestCase createTestCase(String name, byte[] uncompressed)
    {
        byte[] compressed = prepareCompressedData(uncompressed);
        return new TestCase(name, compressed, uncompressed);
    }

    private static void assertByteArraysEqual(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        checkPositionIndexes(leftOffset, leftOffset + leftLength, left.length);
        checkPositionIndexes(rightOffset, rightOffset + rightLength, right.length);

        for (int i = 0; i < Math.min(leftLength, rightLength); i++) {
            if (left[leftOffset + i] != right[rightOffset + i]) {
                Assert.fail(String.format("Byte arrays differ at position %s: 0x%02X vs 0x%02X", i, left[i], right[i]));
            }
        }

        assertEquals(leftLength, rightLength, String.format("Array lengths differ: %s vs %s", leftLength, rightLength));
    }

    public static class TestCase
    {
        private final String name;
        private final byte[] compressed;
        private final byte[] uncompressed;

        private TestCase(String name, byte[] compressed, byte[] uncompressed)
        {
            this.name = name;
            this.compressed = compressed;
            this.uncompressed = uncompressed;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
