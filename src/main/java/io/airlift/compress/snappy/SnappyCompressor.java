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

import io.airlift.compress.Compressor;

import java.lang.foreign.MemorySegment;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import static io.airlift.compress.snappy.UnsafeUtil.getAddress;
import static io.airlift.compress.snappy.UnsafeUtil.getBase;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.lang.ref.Reference.reachabilityFence;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SnappyCompressor
        implements Compressor
{
    private final short[] table = new short[SnappyRawCompressor.MAX_HASH_TABLE_SIZE];

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return SnappyRawCompressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        long inputAddress = ARRAY_BYTE_BASE_OFFSET + inputOffset;
        long inputLimit = inputAddress + inputLength;
        long outputAddress = ARRAY_BYTE_BASE_OFFSET + outputOffset;
        long outputLimit = outputAddress + maxOutputLength;

        return SnappyRawCompressor.compress(input, inputAddress, inputLimit, output, outputAddress, outputLimit, table);
    }

    @Override
    public void compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
    {
        // Java 9+ added an overload of various methods in ByteBuffer. When compiling with Java 11+ and targeting Java 8 bytecode
        // the resulting signatures are invalid for JDK 8, so accesses below result in NoSuchMethodError. Accessing the
        // methods through the interface class works around the problem
        // Sidenote: we can't target "javac --release 8" because Unsafe is not available in the signature data for that profile
        Buffer input = inputBuffer;
        Buffer output = outputBuffer;

        Object inputBase;
        long inputAddress;
        long inputLimit;
        if (input.isDirect()) {
            inputBase = null;
            long address = getAddress(input);
            inputAddress = address + input.position();
            inputLimit = address + input.limit();
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
        if (output.isDirect()) {
            outputBase = null;
            long address = getAddress(output);
            outputAddress = address + output.position();
            outputLimit = address + output.limit();
        }
        else if (output.hasArray()) {
            outputBase = output.array();
            outputAddress = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.position();
            outputLimit = ARRAY_BYTE_BASE_OFFSET + output.arrayOffset() + output.limit();
        }
        else {
            throw new IllegalArgumentException("Unsupported output ByteBuffer implementation " + output.getClass().getName());
        }

        // HACK: Assure JVM does not collect Slice wrappers while compressing, since the
        // collection may trigger freeing of the underlying memory resulting in a segfault
        // There is no other known way to signal to the JVM that an object should not be
        // collected in a block, and technically, the JVM is allowed to eliminate these locks.
        synchronized (input) {
            synchronized (output) {
                int written = SnappyRawCompressor.compress(
                        inputBase,
                        inputAddress,
                        inputLimit,
                        outputBase,
                        outputAddress,
                        outputLimit,
                        table);
                output.position(output.position() + written);
            }
        }
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        try {
            byte[] inputBase = getBase(input);
            long inputAddress = getAddress(input);
            long inputLimit = addExact(inputAddress, input.byteSize());

            byte[] outputBase = getBase(output);
            long outputAddress = getAddress(output);
            long outputLimit = addExact(outputAddress, output.byteSize());

            return SnappyRawCompressor.compress(
                    inputBase,
                    inputAddress,
                    inputLimit,
                    outputBase,
                    outputAddress,
                    outputLimit,
                    table);
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
