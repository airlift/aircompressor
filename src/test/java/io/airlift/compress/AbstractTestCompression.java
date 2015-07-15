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

import com.google.common.io.Files;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestCompression
{
    private static final File TEST_DATA_DIR = new File("testdata");

    protected abstract byte[] prepareCompressedData(byte[] uncompressed);
    protected abstract Decompressor getDecompressor();

    @Test(dataProvider = "data")
    public void testDecompress(TestCase testCase)
            throws Exception
    {
        byte[] uncompressed = new byte[testCase.getUncompressed().length];

        Decompressor decompressor = getDecompressor();
        int written = decompressor.decompress(
                testCase.compressed,
                0,
                testCase.compressed.length,
                uncompressed,
                0,
                uncompressed.length);

        assertEquals(written, uncompressed.length);
        assertByteArraysEqual(testCase.uncompressed, 0, uncompressed, 0, written);
    }

    @DataProvider(name = "data")
    public Iterator<Object[]> getTestCases()
            throws IOException
    {
        File[] testFiles = TEST_DATA_DIR.listFiles();
        Assert.assertTrue(testFiles != null && testFiles.length > 0, "No test files at " + TEST_DATA_DIR.getAbsolutePath());

        List<Object[]> result = new ArrayList<>();

        for (File file : testFiles) {
            byte[] uncompressed = Files.toByteArray(file);
            byte[] compressed = prepareCompressedData(uncompressed);
            result.add(new Object[] { new TestCase(file.getName(), compressed, uncompressed)});
        }

        return result.iterator();
    }

    private static void assertByteArraysEqual(byte[] left, int leftOffset, byte[] right, int rightOffset, int length)
    {
        checkPositionIndexes(leftOffset, leftOffset + length, left.length);
        checkPositionIndexes(rightOffset, rightOffset + length, right.length);

        for (int i = 0; i < length; i++) {
            if (left[leftOffset + i] != right[rightOffset + i]) {
                Assert.fail(String.format("Byte arrays differ at position %s: 0x%02X vs 0x%02X", i, left[i], right[i]));
            }
        }
    }

    private static class TestCase
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

        public byte[] getCompressed()
        {
            return compressed;
        }

        public byte[] getUncompressed()
        {
            return uncompressed;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
