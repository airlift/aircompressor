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
package io.airlift.compress.snappy;

import io.airlift.compress.MalformedInputException;

import java.lang.foreign.MemorySegment;

import static java.lang.Math.toIntExact;

public final class SnappyNativeDecompressor
        implements SnappyDecompressor
{
    private final SnappyNative snappyNative = new SnappyNative();

    public SnappyNativeDecompressor()
    {
        SnappyNative.verifyEnabled();
    }

    public static boolean isEnabled()
    {
        return SnappyNative.isEnabled();
    }

    @Override
    public int getUncompressedLength(byte[] compressed, int compressedOffset)
    {
        MemorySegment input = MemorySegment.ofArray(compressed).asSlice(compressedOffset);
        long length = snappyNative.decompressedLength(input, input.byteSize());
        if ((length < 0) || (length > Integer.MAX_VALUE)) {
            throw new MalformedInputException(0, "invalid compressed length");
        }
        return toIntExact(length);
    }

    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        return decompress(inputSegment, outputSegment);
    }

    @Override
    public int decompress(MemorySegment input, MemorySegment output)
    {
        return toIntExact(snappyNative.decompress(input, input.byteSize(), output, output.byteSize()));
    }
}
