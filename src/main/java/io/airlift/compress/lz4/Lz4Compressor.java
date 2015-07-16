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
package io.airlift.compress.lz4;

import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * This class is not thread-safe
 */
public class Lz4Compressor
    implements Compressor
{
    private final int[] table = new int[Lz4RawCompressor.STREAM_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4RawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Arrays.fill(table, 0);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;

        return Lz4RawCompressor.compress(input, inputAddress, inputLength, output, outputAddress, maxOutputLength, table);
    }

    @Override
    public int compress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset, int maxOutputLength)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
