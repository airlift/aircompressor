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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.testng.Assert.assertEquals;

@Guice(modules = TestingModule.class)
public abstract class AbstractTestCompression
{
    private List<DataSet> testCases;

    protected abstract byte[] prepareCompressedData(byte[] uncompressed);

    protected abstract Compressor getCompressor();
    protected abstract Decompressor getDecompressor();

    protected boolean isByteBufferSupported() {
        return true;
    }

    @Inject
    public void setup(List<DataSet> dataSets)
    {
        testCases = new ArrayList<>();

        testCases.add(new DataSet("short literal", "hello world!".getBytes(UTF_8)));
        testCases.add(new DataSet("small copy", "XXXXabcdabcdABCDABCDwxyzwzyz123".getBytes(UTF_8)));
        testCases.add(new DataSet("long copy", "XXXXabcdefgh abcdefgh abcdefgh abcdefgh abcdefgh abcdefgh ABC".getBytes(UTF_8)));

        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        testCases.add(new DataSet("long literal", data));

        testCases.addAll(dataSets);
    }

    @Test(dataProvider = "data")
    public void testDecompress(DataSet dataSet)
            throws Exception
    {
        byte[] uncompressedOriginal = dataSet.getUncompressed();
        byte[] compressed = prepareCompressedData(uncompressedOriginal);

        byte[] uncompressed = new byte[uncompressedOriginal.length];

        Decompressor decompressor = getDecompressor();
        int uncompressedSize = decompressor.decompress(
                compressed,
                0,
                compressed.length,
                uncompressed,
                0,
                uncompressed.length);

        assertByteArraysEqual(uncompressed, 0, uncompressedSize, uncompressedOriginal, 0, uncompressedOriginal.length);
    }

    @Test(dataProvider = "data")
    public void testDecompressByteBufferHeapToHeap(DataSet dataSet)
            throws Exception
    {
        if (!isByteBufferSupported()) {
            return;
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        ByteBuffer compressed = ByteBuffer.wrap(prepareCompressedData(uncompressedOriginal));
        ByteBuffer uncompressed = ByteBuffer.allocate(uncompressedOriginal.length);

        getDecompressor().decompress(compressed, uncompressed);
        uncompressed.flip();

        assertByteBufferEqual(ByteBuffer.wrap(uncompressedOriginal), uncompressed);
    }

    @Test(dataProvider = "data")
    public void testDecompressByteBufferHeapToDirect(DataSet dataSet)
            throws Exception
    {
        if (!isByteBufferSupported()) {
            return;
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        ByteBuffer compressed = ByteBuffer.wrap(prepareCompressedData(uncompressedOriginal));
        ByteBuffer uncompressed = ByteBuffer.allocateDirect(uncompressedOriginal.length);

        getDecompressor().decompress(compressed, uncompressed);
        uncompressed.flip();

        assertByteBufferEqual(ByteBuffer.wrap(uncompressedOriginal), uncompressed);
    }

    @Test(dataProvider = "data")
    public void testDecompressByteBufferDirectToHeap(DataSet dataSet)
            throws Exception
    {
        if (!isByteBufferSupported()) {
            return;
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        ByteBuffer compressed = toDirectBuffer(prepareCompressedData(uncompressedOriginal));
        ByteBuffer uncompressed = ByteBuffer.allocate(uncompressedOriginal.length);

        getDecompressor().decompress(compressed, uncompressed);
        uncompressed.flip();

        assertByteBufferEqual(ByteBuffer.wrap(uncompressedOriginal), uncompressed);
    }

    @Test(dataProvider = "data")
    public void testDecompressByteBufferDirectToDirect(DataSet dataSet)
            throws Exception
    {
        if (!isByteBufferSupported()) {
            return;
        }

        byte[] uncompressedOriginal = dataSet.getUncompressed();

        ByteBuffer compressed = toDirectBuffer(prepareCompressedData(uncompressedOriginal));
        ByteBuffer uncompressed = ByteBuffer.allocateDirect(uncompressedOriginal.length);

        getDecompressor().decompress(compressed, uncompressed);
        uncompressed.flip();

        assertByteBufferEqual(ByteBuffer.wrap(uncompressedOriginal), uncompressed);
    }

    @Test(dataProvider = "data")
    public void testCompress(DataSet testCase)
            throws Exception
    {
        Compressor compressor = getCompressor();

        byte[] originalUncompressed = testCase.getUncompressed();
        byte[] compressed = new byte[compressor.maxCompressedLength(originalUncompressed.length)];

        int compressedLength = compressor.compress(
                originalUncompressed,
                0,
                originalUncompressed.length,
                compressed,
                0,
                compressed.length);

        // TODO: validate with "control" decompressor
        byte[] uncompressed = new byte[originalUncompressed.length];
        int uncompresseSize = getDecompressor().decompress(compressed, 0, compressedLength, uncompressed, 0, uncompressed.length);

        assertByteArraysEqual(uncompressed, 0, uncompresseSize, originalUncompressed, 0, originalUncompressed.length);
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

    private static void assertByteArraysEqual(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength)
    {
        checkPositionIndexes(leftOffset, leftOffset + leftLength, left.length);
        checkPositionIndexes(rightOffset, rightOffset + rightLength, right.length);

        for (int i = 0; i < Math.min(leftLength, rightLength); i++) {
            if (left[leftOffset + i] != right[rightOffset + i]) {
                Assert.fail(String.format("Byte arrays differ at position %s: 0x%02X vs 0x%02X", i, left[leftOffset + i], right[rightOffset + i]));
            }
        }

        assertEquals(leftLength, rightLength, String.format("Array lengths differ: %s vs %s", leftLength, rightLength));
    }

    private static void assertByteBufferEqual(ByteBuffer left, ByteBuffer right)
    {
        int leftPosition = left.position();
        int rightPosition = right.position();
        for (int i = 0; i < Math.min(left.remaining(), right.remaining()); i++) {
            if (left.get(leftPosition + i) != right.get(rightPosition + i)) {
                Assert.fail(String.format("Byte buffers differ at position %s: 0x%02X vs 0x%02X", i, left.get(leftPosition + i), right.get(rightPosition + i)));
            }
        }

        assertEquals(left.remaining(), right.remaining(), String.format("Buffer lengths differ: %s vs %s", left.remaining(), left.remaining()));
    }

    private static ByteBuffer toDirectBuffer(byte[] data)
    {
        ByteBuffer direct = ByteBuffer.allocateDirect(data.length);
        direct.put(data).flip();
        return direct;
    }
}
