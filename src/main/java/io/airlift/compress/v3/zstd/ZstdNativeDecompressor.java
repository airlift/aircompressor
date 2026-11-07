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

import io.airlift.compress.v3.MalformedInputException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static io.airlift.compress.v3.zstd.Constants.MAGIC_NUMBER;
import static io.airlift.compress.v3.zstd.Constants.SIZE_OF_INT;
import static io.airlift.compress.v3.zstd.Constants.SKIPPABLE_MAGIC_NUMBER;
import static io.airlift.compress.v3.zstd.Constants.SKIPPABLE_MAGIC_NUMBER_MASK;
import static java.lang.Math.toIntExact;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class ZstdNativeDecompressor
        implements ZstdDecompressor
{
    private static final ValueLayout.OfInt INT_LE = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private final ZstdNativeContextCache contexts;

    public ZstdNativeDecompressor()
    {
        ZstdNative.verifyEnabled();
        this.contexts = new ZstdNativeContextCache(ZstdNative::createDecompressionContext, ZstdNative::freeDecompressionContext);
    }

    public static boolean isEnabled()
    {
        return ZstdNative.isEnabled();
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return decompress(inputSegment, inputLength, outputSegment, maxOutputLength);
    }

    @Override
    public int decompress(MemorySegment input, MemorySegment output)
    {
        return decompress(input, input.byteSize(), output, output.byteSize());
    }

    private int decompress(MemorySegment input, long inputLength, MemorySegment output, long maxOutputLength)
    {
        verifyFrameMagics(input, inputLength);
        MemorySegment context = contexts.acquire();
        try {
            return toIntExact(ZstdNative.decompress(context, input, inputLength, output, maxOutputLength));
        }
        finally {
            contexts.release(context);
        }
    }

    @Override
    public long getDecompressedSize(byte[] input, int offset, int length)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(offset, length);
        // fewer bytes than a magic number are left to libzstd, which rejects them as truncated input
        if (length >= SIZE_OF_INT) {
            verifyMagic(inputSegment, 0);
        }
        return ZstdNative.decompressedLength(inputSegment, length);
    }

    private static void verifyFrameMagics(MemorySegment input, long inputLength)
    {
        long offset = 0;
        // fewer bytes than a magic number are left to libzstd, which rejects them as truncated input
        while (inputLength - offset >= SIZE_OF_INT) {
            verifyMagic(input, offset);
            offset += ZstdNative.findFrameCompressedSize(input.asSlice(offset), inputLength - offset);
        }
    }

    // libzstd also decodes the legacy v0.5 to v0.7 frame formats, which the Java decompressor rejects, and reports corrupt legacy frames as a generic error
    private static void verifyMagic(MemorySegment input, long offset)
    {
        int magic = input.get(INT_LE, offset);
        if (magic != MAGIC_NUMBER && (magic & SKIPPABLE_MAGIC_NUMBER_MASK) != SKIPPABLE_MAGIC_NUMBER) {
            throw new MalformedInputException(offset, "Invalid magic prefix: " + Integer.toHexString(magic));
        }
    }
}
