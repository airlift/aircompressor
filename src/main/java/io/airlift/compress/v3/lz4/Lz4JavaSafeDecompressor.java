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
package io.airlift.compress.v3.lz4;

import io.airlift.compress.v3.MalformedInputException;

import java.lang.foreign.MemorySegment;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Safe ({@code Unsafe}-free) LZ4 decompressor built on {@link MemorySegment}. Behaves identically to
 * {@link Lz4JavaDecompressor} (the {@code Unsafe} reference) but trades a little throughput for not touching
 * {@code sun.misc.Unsafe}.
 */
public final class Lz4JavaSafeDecompressor
        implements Lz4Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        return Lz4SafeRawDecompressor.decompress(
                MemorySegment.ofArray(input), inputOffset, inputLength,
                MemorySegment.ofArray(output), outputOffset, maxOutputLength);
    }

    @Override
    public int decompress(MemorySegment inputSegment, MemorySegment outputSegment)
    {
        // Zero-copy: the decoder never reads the input past inputLimit nor writes the output past outputLimit
        // (the LAST_LITERAL_SIZE margin in the fast-literal guard bounds the long-at-a-time over-read), so the
        // caller's segments can be decoded in place.
        return Lz4SafeRawDecompressor.decompress(
                inputSegment, 0, toIntExact(inputSegment.byteSize()),
                outputSegment, 0, toIntExact(outputSegment.byteSize()));
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
