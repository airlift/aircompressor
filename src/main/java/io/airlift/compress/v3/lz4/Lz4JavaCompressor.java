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

import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.lz4.Lz4RawCompressor.MAX_TABLE_SIZE;
import static io.airlift.compress.v3.lz4.UnsafeUtil.getAddress;
import static io.airlift.compress.v3.lz4.UnsafeUtil.getBase;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.ref.Reference.reachabilityFence;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * This class is not thread-safe
 */
public final class Lz4JavaCompressor
        implements Lz4Compressor
{
    private final int[] table = new int[MAX_TABLE_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4RawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;

        return Lz4RawCompressor.compress(input, inputAddress, inputLength, output, outputAddress, maxOutputLength, table);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        try {
            return Lz4RawCompressor.compress(
                    getBase(input),
                    getAddress(input),
                    toIntExact(input.byteSize()),
                    getBase(output),
                    getAddress(output),
                    toIntExact(output.byteSize()),
                    table);
        }
        finally {
            reachabilityFence(input);
            reachabilityFence(output);
        }
    }

    @Override
    public int getRetainedSizeInBytes(int inputLength)
    {
        return Lz4RawCompressor.computeTableSize(inputLength);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
