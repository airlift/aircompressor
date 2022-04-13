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
package io.airlift.compress.lz4;

import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.benchmark.DataSet;
import io.airlift.compress.thirdparty.JPountzLz4FrameCompressor;
import io.airlift.compress.thirdparty.JPountzLz4FrameDecompressor;
import net.jpountz.lz4.LZ4Factory;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class TestLz4Frame
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        return new Lz4FrameCompressor();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new Lz4FrameDecompressor();
    }

    @Override
    protected boolean isByteBufferSupported()
    {
        // TODO support byte buffer
        return false;
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new JPountzLz4FrameCompressor(LZ4Factory.fastestInstance());
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new JPountzLz4FrameDecompressor(LZ4Factory.fastestInstance());
    }

    // test over data sets, should the result depend on input size or its compressibility
    @Test(dataProvider = "data")
    public void testGetDecompressedSize(DataSet dataSet)
    {
        Compressor compressor = getCompressor();
        byte[] originalUncompressed = dataSet.getUncompressed();
        byte[] compressed = new byte[compressor.maxCompressedLength(originalUncompressed.length)];

        int compressedLength = compressor.compress(originalUncompressed, 0, originalUncompressed.length, compressed, 0, compressed.length);

        assertEquals(Lz4FrameDecompressor.getDecompressedSize(compressed, 0, compressedLength), originalUncompressed.length);

        int padding = 10;
        byte[] compressedWithPadding = new byte[compressedLength + padding];
        Arrays.fill(compressedWithPadding, (byte) 42);
        System.arraycopy(compressed, 0, compressedWithPadding, padding, compressedLength);
        assertEquals(Lz4FrameDecompressor.getDecompressedSize(compressedWithPadding, padding, compressedLength), originalUncompressed.length);
    }
}
