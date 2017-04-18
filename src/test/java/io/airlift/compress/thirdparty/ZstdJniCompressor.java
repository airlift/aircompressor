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
package io.airlift.compress.thirdparty;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ZstdJniCompressor
        implements Compressor
{
    private final int level;

    public ZstdJniCompressor(int level)
    {
        this.level = level;
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return (int) Zstd.compressBound(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        byte[] uncompressed = Arrays.copyOfRange(input, inputOffset, inputLength);
        byte[] compressed = Zstd.compress(uncompressed, level);

        System.arraycopy(compressed, 0, output, outputOffset, compressed.length);

        return compressed.length;
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        byte[] uncompressed = new byte[input.remaining()];
        input.get(uncompressed);

        byte[] compressed = new byte[output.remaining()];

        int compressedSize = compress(uncompressed, 0, uncompressed.length, compressed, 0, compressed.length);
        output.put(compressed, 0, compressedSize);
    }
}
