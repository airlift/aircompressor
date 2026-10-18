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
package io.airlift.compress.v3.zstd;

import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.zstd.ZstdNative.DEFAULT_COMPRESSION_LEVEL;
import static java.lang.Math.toIntExact;

public class ZstdNativeCompressor
        implements ZstdCompressor
{
    private final int compressionLevel;
    private final ZstdNativeContextCache contexts;

    public ZstdNativeCompressor()
    {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public ZstdNativeCompressor(int compressionLevel)
    {
        ZstdNative.verifyEnabled();
        this.compressionLevel = compressionLevel;
        this.contexts = new ZstdNativeContextCache(ZstdNative::createCompressionContext, ZstdNative::freeCompressionContext);
    }

    public static boolean isEnabled()
    {
        return ZstdNative.isEnabled();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return toIntExact(ZstdNative.maxCompressedLength(uncompressedSize));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return compress(inputSegment, inputLength, outputSegment, maxOutputLength);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        return compress(input, input.byteSize(), output, output.byteSize());
    }

    private int compress(MemorySegment input, long inputLength, MemorySegment output, long maxOutputLength)
    {
        MemorySegment context = contexts.acquire();
        try {
            return toIntExact(ZstdNative.compress(context, input, inputLength, output, maxOutputLength, compressionLevel));
        }
        finally {
            contexts.release(context);
        }
    }
}
