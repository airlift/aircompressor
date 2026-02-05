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
package io.airlift.compress.v3.xxhash;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static java.lang.Long.rotateLeft;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;

public final class XxHash64JavaHasher
        implements XxHash64Hasher
{
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2b2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private final byte[] buffer = new byte[32];
    private int bufferSize;

    private long bodyLength;

    private long v1;
    private long v2;
    private long v3;
    private long v4;

    private long seed;

    public XxHash64JavaHasher(long seed)
    {
        this.seed = seed;
        resetState(seed);
    }

    private void resetState(long seed)
    {
        this.v1 = seed + PRIME64_1 + PRIME64_2;
        this.v2 = seed + PRIME64_2;
        this.v3 = seed;
        this.v4 = seed - PRIME64_1;
        this.bufferSize = 0;
        this.bodyLength = 0;
    }

    public static long hash(long value, long seed)
    {
        long hash = seed + PRIME64_5 + Long.BYTES;
        hash = updateTail(hash, value);
        return finalShuffle(hash);
    }

    public static long hash(byte[] input, int offset, int length, long seed)
    {
        checkFromIndexSize(offset, length, input.length);

        long hash;
        int index = offset;
        int end = offset + length;

        if (length >= 32) {
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            while (index <= end - 32) {
                v1 = mix(v1, (long) LONG_HANDLE.get(input, index));
                v2 = mix(v2, (long) LONG_HANDLE.get(input, index + 8));
                v3 = mix(v3, (long) LONG_HANDLE.get(input, index + 16));
                v4 = mix(v4, (long) LONG_HANDLE.get(input, index + 24));
                index += 32;
            }

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
            hash = update(hash, v1);
            hash = update(hash, v2);
            hash = update(hash, v3);
            hash = update(hash, v4);
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += length;

        // Process remaining bytes
        while (index <= end - 8) {
            hash = updateTail(hash, (long) LONG_HANDLE.get(input, index));
            index += 8;
        }

        if (index <= end - 4) {
            hash = updateTail(hash, (int) INT_HANDLE.get(input, index));
            index += 4;
        }

        while (index < end) {
            hash = updateTail(hash, input[index]);
            index++;
        }

        return finalShuffle(hash);
    }

    public static long hash(MemorySegment input, long seed)
    {
        // For heap segments backed by byte arrays, extract and use direct array access
        if (input.isNative()) {
            return hashSegment(input, seed);
        }

        // Try to get the backing array for better performance
        byte[] array = input.heapBase()
                .filter(base -> base instanceof byte[])
                .map(base -> (byte[]) base)
                .orElse(null);

        if (array != null) {
            // Calculate offset into the array
            // heapBase gives us the array, address gives us offset from array start
            long arrayOffset = input.address();
            return hash(array, (int) arrayOffset, (int) input.byteSize(), seed);
        }

        return hashSegment(input, seed);
    }

    private static long hashSegment(MemorySegment input, long seed)
    {
        long length = input.byteSize();
        long hash;
        long index = 0;

        if (length >= 32) {
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            while (index <= length - 32) {
                v1 = mix(v1, input.get(ValueLayout.JAVA_LONG_UNALIGNED, index));
                v2 = mix(v2, input.get(ValueLayout.JAVA_LONG_UNALIGNED, index + 8));
                v3 = mix(v3, input.get(ValueLayout.JAVA_LONG_UNALIGNED, index + 16));
                v4 = mix(v4, input.get(ValueLayout.JAVA_LONG_UNALIGNED, index + 24));
                index += 32;
            }

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
            hash = update(hash, v1);
            hash = update(hash, v2);
            hash = update(hash, v3);
            hash = update(hash, v4);
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += length;

        while (index <= length - 8) {
            hash = updateTail(hash, input.get(ValueLayout.JAVA_LONG_UNALIGNED, index));
            index += 8;
        }

        if (index <= length - 4) {
            hash = updateTail(hash, input.get(ValueLayout.JAVA_INT_UNALIGNED, index));
            index += 4;
        }

        while (index < length) {
            hash = updateTail(hash, input.get(ValueLayout.JAVA_BYTE, index));
            index++;
        }

        return finalShuffle(hash);
    }

    @Override
    public XxHash64Hasher update(byte[] input)
    {
        return update(input, 0, input.length);
    }

    @Override
    public XxHash64Hasher update(byte[] input, int offset, int length)
    {
        checkFromIndexSize(offset, length, input.length);

        int index = offset;
        int remaining = length;

        // Fill buffer if partially filled
        if (bufferSize > 0) {
            int available = min(32 - bufferSize, remaining);
            System.arraycopy(input, index, buffer, bufferSize, available);

            bufferSize += available;
            index += available;
            remaining -= available;

            if (bufferSize == 32) {
                updateBodyFromBuffer();
                bufferSize = 0;
            }
        }

        // Process full 32-byte blocks directly from input
        while (remaining >= 32) {
            v1 = mix(v1, (long) LONG_HANDLE.get(input, index));
            v2 = mix(v2, (long) LONG_HANDLE.get(input, index + 8));
            v3 = mix(v3, (long) LONG_HANDLE.get(input, index + 16));
            v4 = mix(v4, (long) LONG_HANDLE.get(input, index + 24));

            index += 32;
            remaining -= 32;
            bodyLength += 32;
        }

        // Buffer remaining bytes
        if (remaining > 0) {
            System.arraycopy(input, index, buffer, bufferSize, remaining);
            bufferSize += remaining;
        }

        return this;
    }

    @Override
    public XxHash64Hasher update(MemorySegment input)
    {
        // For heap segments backed by byte arrays, use direct array access
        byte[] array = input.heapBase()
                .filter(base -> base instanceof byte[])
                .map(base -> (byte[]) base)
                .orElse(null);

        if (array != null) {
            long arrayOffset = input.address();
            return update(array, (int) arrayOffset, (int) input.byteSize());
        }

        // Fall back to MemorySegment access for native memory
        return updateSegment(input);
    }

    private XxHash64Hasher updateSegment(MemorySegment input)
    {
        long length = input.byteSize();
        long inputOffset = 0;

        if (bufferSize > 0) {
            int available = (int) min(32 - bufferSize, length);
            for (int i = 0; i < available; i++) {
                buffer[bufferSize + i] = input.get(ValueLayout.JAVA_BYTE, inputOffset + i);
            }

            bufferSize += available;
            inputOffset += available;
            length -= available;

            if (bufferSize == 32) {
                updateBodyFromBuffer();
                bufferSize = 0;
            }
        }

        while (length >= 32) {
            v1 = mix(v1, input.get(ValueLayout.JAVA_LONG_UNALIGNED, inputOffset));
            v2 = mix(v2, input.get(ValueLayout.JAVA_LONG_UNALIGNED, inputOffset + 8));
            v3 = mix(v3, input.get(ValueLayout.JAVA_LONG_UNALIGNED, inputOffset + 16));
            v4 = mix(v4, input.get(ValueLayout.JAVA_LONG_UNALIGNED, inputOffset + 24));

            inputOffset += 32;
            length -= 32;
            bodyLength += 32;
        }

        if (length > 0) {
            for (int i = 0; i < length; i++) {
                buffer[bufferSize + i] = input.get(ValueLayout.JAVA_BYTE, inputOffset + i);
            }
            bufferSize += (int) length;
        }

        return this;
    }

    @Override
    public XxHash64Hasher updateLE(long value)
    {
        byte[] bytes = new byte[8];
        LONG_HANDLE.set(bytes, 0, value);
        return update(bytes);
    }

    @Override
    public XxHash64Hasher updateLE(int value)
    {
        byte[] bytes = new byte[4];
        INT_HANDLE.set(bytes, 0, value);
        return update(bytes);
    }

    private void updateBodyFromBuffer()
    {
        v1 = mix(v1, (long) LONG_HANDLE.get(buffer, 0));
        v2 = mix(v2, (long) LONG_HANDLE.get(buffer, 8));
        v3 = mix(v3, (long) LONG_HANDLE.get(buffer, 16));
        v4 = mix(v4, (long) LONG_HANDLE.get(buffer, 24));
        bodyLength += 32;
    }

    @Override
    public long digest()
    {
        long hash;
        if (bodyLength > 0) {
            hash = computeBody();
        }
        else {
            hash = seed + PRIME64_5;
        }

        hash += bodyLength + bufferSize;

        // Process remaining bytes in buffer
        int index = 0;
        while (index <= bufferSize - 8) {
            hash = updateTail(hash, (long) LONG_HANDLE.get(buffer, index));
            index += 8;
        }

        if (index <= bufferSize - 4) {
            hash = updateTail(hash, (int) INT_HANDLE.get(buffer, index));
            index += 4;
        }

        while (index < bufferSize) {
            hash = updateTail(hash, buffer[index]);
            index++;
        }

        return finalShuffle(hash);
    }

    private long computeBody()
    {
        long hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

        hash = update(hash, v1);
        hash = update(hash, v2);
        hash = update(hash, v3);
        hash = update(hash, v4);

        return hash;
    }

    @Override
    public XxHash64Hasher reset()
    {
        return reset(DEFAULT_SEED);
    }

    @Override
    public XxHash64Hasher reset(long seed)
    {
        this.seed = seed;
        resetState(seed);
        return this;
    }

    @Override
    public void close() {}

    private static long mix(long current, long value)
    {
        return rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1;
    }

    private static long update(long hash, long value)
    {
        long temp = hash ^ mix(0, value);
        return temp * PRIME64_1 + PRIME64_4;
    }

    private static long updateTail(long hash, long value)
    {
        long temp = hash ^ mix(0, value);
        return rotateLeft(temp, 27) * PRIME64_1 + PRIME64_4;
    }

    private static long updateTail(long hash, int value)
    {
        long unsigned = value & 0xFFFF_FFFFL;
        long temp = hash ^ (unsigned * PRIME64_1);
        return rotateLeft(temp, 23) * PRIME64_2 + PRIME64_3;
    }

    private static long updateTail(long hash, byte value)
    {
        int unsigned = value & 0xFF;
        long temp = hash ^ (unsigned * PRIME64_5);
        return rotateLeft(temp, 11) * PRIME64_1;
    }

    private static long finalShuffle(long hash)
    {
        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;
        return hash;
    }
}
