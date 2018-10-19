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

import com.google.common.io.Resources;
import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;
import io.airlift.compress.thirdparty.ZstdJniCompressor;
import io.airlift.compress.thirdparty.ZstdJniDecompressor;
import org.testng.annotations.Test;

import java.io.IOException;

public class TestZstd
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        // TODO: replace with Java implementation once it's ready
        return new ZstdJniCompressor(6);
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new ZstdDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJniCompressor(6);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJniDecompressor();
    }

    // Ideally, this should be covered by super.testDecompressWithOutputPadding(...), but the data written by the native
    // compressor doesn't include checksums, so it's not a comprehensive test. The dataset for this test has a checksum.
    @Test
    public void testDecompressWithOutputPaddingAndChecksum()
            throws IOException
    {
        int padding = 1021;

        byte[] compressed = Resources.toByteArray(getClass().getClassLoader().getResource("data/zstd/with-checksum.zst"));
        byte[] uncompressed = Resources.toByteArray(getClass().getClassLoader().getResource("data/zstd/with-checksum"));

        byte[] output = new byte[uncompressed.length + padding * 2]; // pre + post padding
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressed.length, output, padding, output.length);

        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, padding, decompressedSize);
    }

    @Test
    public void testConcatenatedFrames()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(getClass().getClassLoader().getResource("data/zstd/multiple-frames.zst"));
        byte[] uncompressed = Resources.toByteArray(getClass().getClassLoader().getResource("data/zstd/multiple-frames"));

        byte[] output = new byte[uncompressed.length];
        getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length);

        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, 0, output.length);
    }

    @Test(expectedExceptions = MalformedInputException.class, expectedExceptionsMessageRegExp = "Input is corrupted: offset=894")
    public void testInvalidSequenceOffset()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(getClass().getClassLoader().getResource("data/zstd/offset-before-start.zst"));
        byte[] output = new byte[compressed.length * 10];

        getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length);
    }
}
