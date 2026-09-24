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
package io.airlift.compress.v3.lzo;

import io.airlift.compress.v3.Compressor;

import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.lzo.LzoRawCompressor.MAX_TABLE_SIZE;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is not thread-safe
 */
public class LzoCompressor
        implements Compressor
{
    private final int[] table = new int[MAX_TABLE_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return LzoRawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        return LzoRawCompressor.compress(MemorySegment.ofArray(input), inputOffset, inputLength, MemorySegment.ofArray(output), outputOffset, maxOutputLength, table);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        return LzoRawCompressor.compress(
                input,
                0,
                toIntExact(input.byteSize()),
                output,
                0,
                toIntExact(output.byteSize()),
                table);
    }

    @Override
    public int getRetainedSizeInBytes(int inputLength)
    {
        return MAX_TABLE_SIZE;
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
