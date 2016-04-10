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
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.airlift.compress.Fences.reachabilityFence;
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
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        Object inputBase;
        long inputAddress;
        long inputLimit;
        if (input instanceof DirectBuffer) {
            DirectBuffer direct = (DirectBuffer) input;
            inputBase = null;
            inputAddress = direct.address() + input.position();
            inputLimit = direct.address() + input.limit();
        }
        else if (input.hasArray()) {
            inputBase = input.array();
            inputAddress = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.position();
            inputLimit = ARRAY_BYTE_BASE_OFFSET + input.arrayOffset() + input.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported input ByteBuffer implementation " + input.getClass().getName());
        }

        Object outputBase;
        long outputAddress;
        long outputLimit;
        if (output instanceof DirectBuffer) {
            DirectBuffer direct = (DirectBuffer) output;
            outputBase = null;
            outputAddress = direct.address() + output.position();
            outputLimit = direct.address() + output.limit();
        }
        else if (output.hasArray()) {
            outputBase = output.array();
            outputAddress = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.position();
            outputLimit = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported output ByteBuffer implementation " + output.getClass().getName());
        }

        // HACK: Assure JVM does not collect ByteBuffers while decompressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault.
        try {
            int written = Lz4RawCompressor.compress(
                    inputBase,
                    inputAddress,
                    (int) (inputLimit - inputAddress),
                    outputBase,
                    outputAddress,
                    outputLimit - outputAddress,
                    table);
            output.position(output.position() + written);
        }
        finally {
            reachabilityFence(input);
            reachabilityFence(output);
            // it is possible but unlikely that the above reference could be retained, so clear the reference here
            reachabilityFence(null);
        }
    }
}
