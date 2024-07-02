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
package io.airlift.compress.v2.thirdparty;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.v2.Compressor;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

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
        return (int) Zstd.compressByteArray(output, outputOffset, maxOutputLength, input, inputOffset, inputLength, level);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        ByteBuffer inputBuffer = input.asByteBuffer();
        ByteBuffer outputBuffer = output.asByteBuffer();
        Zstd.compress(inputBuffer, outputBuffer, level);
        return outputBuffer.position();
    }
}
