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

import io.airlift.compress.v3.internal.MemorySegmentAccess;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static io.airlift.compress.v3.lz4.Lz4Native.DEFAULT_ACCELERATION;
import static io.airlift.compress.v3.lz4.Lz4Native.MAX_ACCELERATION;
import static io.airlift.compress.v3.lz4.Lz4RawCompressor.MAX_INPUT_SIZE;
import static io.airlift.compress.v3.lz4.Lz4RawCompressor.MAX_TABLE_SIZE;
import static io.airlift.compress.v3.lz4.UnsafeUtil.getAddress;
import static java.lang.Math.toIntExact;

/**
 * This class is not thread-safe.
 */
public final class Lz4NativeCompressor
        implements Lz4Compressor
{
    private static final int MIN_SIMPLE_INPUT_SIZE = 128 * 1024;

    private final MemorySegment state = Arena.ofAuto().allocate(Lz4Native.STATE_SIZE);
    private final int acceleration;
    private int[] table;

    public Lz4NativeCompressor()
    {
        this(DEFAULT_ACCELERATION);
    }

    public Lz4NativeCompressor(int acceleration)
    {
        if (acceleration < DEFAULT_ACCELERATION || acceleration > MAX_ACCELERATION) {
            throw new IllegalArgumentException("LZ4 acceleration should be in the [%d, %d] range but got %d".formatted(DEFAULT_ACCELERATION, MAX_ACCELERATION, acceleration));
        }
        Lz4Native.verifyEnabled();
        this.acceleration = acceleration;
    }

    public static boolean isEnabled()
    {
        return Lz4Native.isEnabled();
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return Lz4Native.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        MemorySegment inputSegment = MemorySegment.ofArray(input).asSlice(inputOffset, inputLength);
        MemorySegment outputSegment = MemorySegment.ofArray(output).asSlice(outputOffset, maxOutputLength);
        int result = tryCompressSimple(inputSegment, outputSegment);
        if (result >= 0) {
            return result;
        }
        return Lz4Native.compress(inputSegment, inputLength, outputSegment, maxOutputLength, acceleration, state);
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        MemorySegmentAccess.checkWritable(output);
        int result = tryCompressSimple(input, output);
        if (result >= 0) {
            return result;
        }
        return Lz4Native.compress(input, toIntExact(input.byteSize()), output, toIntExact(output.byteSize()), acceleration, state);
    }

    private int tryCompressSimple(MemorySegment input, MemorySegment output)
    {
        long inputLength = input.byteSize();
        // The native 32-bit hash-table search applies above its 64 KiB small-input range.
        // Heap arrays need no arena protection, and disjoint ranges keep output writes from
        // changing input. Other segment kinds keep the native path and its access checks.
        if (inputLength < MIN_SIMPLE_INPUT_SIZE || inputLength > MAX_INPUT_SIZE ||
                !(input.heapBase().orElse(null) instanceof byte[] inputBase) ||
                !(output.heapBase().orElse(null) instanceof byte[] outputBase) ||
                output.byteSize() < maxCompressedLength((int) inputLength) ||
                input.asOverlappingSlice(output).isPresent()) {
            return -1;
        }
        if (table == null) {
            table = new int[MAX_TABLE_SIZE];
        }
        return Lz4RawCompressor.tryCompressSimple(inputBase, getAddress(input), (int) inputLength, outputBase, getAddress(output), acceleration, table);
    }

    @Override
    public int getRetainedSizeInBytes(int inputLength)
    {
        return Lz4Native.STATE_SIZE + (table == null ? 0 : Integer.BYTES * table.length);
    }
}
