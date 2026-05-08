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
package io.airlift.compress.v3.zstdFFM;

import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.zstdFFM.Constants.MAX_BLOCK_SIZE;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.lang.ref.Reference.reachabilityFence;
import static java.util.Objects.requireNonNull;

public class ZstdJavaCompressor
        implements ZstdCompressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        int result = uncompressedSize + (uncompressedSize >>> 8);

        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 11;
        }

        return result;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        MemorySegment inputSegment = MemorySegment.ofArray(input);
        MemorySegment outputSegment = MemorySegment.ofArray(output);

        return ZstdFrameCompressor.compress(
                inputSegment,
                inputOffset,
                inputOffset + inputLength,
                outputSegment,
                outputOffset,
                outputOffset + maxOutputLength,
                CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        try {
            long inputAddress = 0;
            long inputLimit = addExact(inputAddress, input.byteSize());

            long outputAddress = 0;
            long outputLimit = addExact(outputAddress, output.byteSize());

            return ZstdFrameCompressor.compress(
                    input,
                    inputAddress,
                    inputLimit,
                    output,
                    outputAddress,
                    outputLimit,
                    CompressionParameters.DEFAULT_COMPRESSION_LEVEL);
        }
        finally {
            reachabilityFence(input);
            reachabilityFence(output);
        }
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
