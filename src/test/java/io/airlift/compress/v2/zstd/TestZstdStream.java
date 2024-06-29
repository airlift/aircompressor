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
package io.airlift.compress.v2.zstd;

import com.google.common.io.Resources;
import io.airlift.compress.v2.Compressor;
import io.airlift.compress.v2.Decompressor;
import io.airlift.compress.v2.MalformedInputException;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestZstdStream
        extends TestZstd
{
    @Override
    protected boolean isMemorySegmentSupported()
    {
        return false;
    }

    @Override
    protected ZstdCompressor getCompressor()
    {
        return new ZstdStreamCompressor();
    }

    @Override
    protected ZstdDecompressor getDecompressor()
    {
        return new ZstdStreamDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJavaCompressor();
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJavaDecompressor();
    }

    @Override
    public void testInvalidSequenceOffset()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/offset-before-start.zst"));
        byte[] output = new byte[compressed.length * 10];

        assertThatThrownBy(() -> getDecompressor().decompress(compressed, 0, compressed.length, output, 0, output.length))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Input is corrupted: offset=");
    }

    @Override
    public void testGetDecompressedSize()
    {
        // streaming does not publish the size
    }
}
