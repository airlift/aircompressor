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

import static java.lang.Integer.rotateLeft;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;

public final class XxHash32JavaHasher
        implements XxHash32Hasher
{
    private static final int PRIME32_1 = 0x9E3779B1;
    private static final int PRIME32_2 = 0x85EBCA77;
    private static final int PRIME32_3 = 0xC2B2AE3D;
    private static final int PRIME32_4 = 0x27D4EB2F;
    private static final int PRIME32_5 = 0x165667B1;

    private static final int BLOCK_SIZE = 16;

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private final byte[] buffer = new byte[BLOCK_SIZE];
    private int bufferSize;

    private long bodyLength;

    private int v1;
    private int v2;
    private int v3;
    private int v4;

    private int seed;

    public XxHash32JavaHasher(int seed)
    {
        this.seed = seed;
        resetState(seed);
    }

    private void resetState(int seed)
    {
        this.v1 = seed + PRIME32_1 + PRIME32_2;
        this.v2 = seed + PRIME32_2;
        this.v3 = seed;
        this.v4 = seed - PRIME32_1;
        this.bufferSize = 0;
        this.bodyLength = 0;
    }

    public static int hash(byte[] input, int offset, int length, int seed)
    {
        checkFromIndexSize(offset, length, input.length);

        int hash;
        int index = offset;
        int end = offset + length;

        if (length >= BLOCK_SIZE) {
            int v1 = seed + PRIME32_1 + PRIME32_2;
            int v2 = seed + PRIME32_2;
            int v3 = seed;
            int v4 = seed - PRIME32_1;

            while (index <= end - BLOCK_SIZE) {
                v1 = mix(v1, (int) INT_HANDLE.get(input, index));
                v2 = mix(v2, (int) INT_HANDLE.get(input, index + 4));
                v3 = mix(v3, (int) INT_HANDLE.get(input, index + 8));
                v4 = mix(v4, (int) INT_HANDLE.get(input, index + 12));
                index += BLOCK_SIZE;
            }

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
        }
        else {
            hash = seed + PRIME32_5;
        }

        hash += length;

        // Process remaining bytes
        while (index <= end - 4) {
            hash = updateTail(hash, (int) INT_HANDLE.get(input, index));
            index += 4;
        }

        while (index < end) {
            hash = updateTail(hash, input[index]);
            index++;
        }

        return finalShuffle(hash);
    }

    public static int hash(MemorySegment input, int seed)
    {
        // For heap segments backed by byte arrays, extract and use direct array access
        if (input.isNative()) {
            return hashSegment(input, seed);
        }

        byte[] array = input.heapBase()
                .filter(base -> base instanceof byte[])
                .map(base -> (byte[]) base)
                .orElse(null);

        if (array != null) {
            // heapBase gives us the array, address gives us the offset from the array start
            return hash(array, toIntExact(input.address()), toIntExact(input.byteSize()), seed);
        }

        return hashSegment(input, seed);
    }

    private static int hashSegment(MemorySegment input, int seed)
    {
        long length = input.byteSize();
        int hash;
        long index = 0;

        if (length >= BLOCK_SIZE) {
            int v1 = seed + PRIME32_1 + PRIME32_2;
            int v2 = seed + PRIME32_2;
            int v3 = seed;
            int v4 = seed - PRIME32_1;

            while (index <= length - BLOCK_SIZE) {
                v1 = mix(v1, input.get(ValueLayout.JAVA_INT_UNALIGNED, index));
                v2 = mix(v2, input.get(ValueLayout.JAVA_INT_UNALIGNED, index + 4));
                v3 = mix(v3, input.get(ValueLayout.JAVA_INT_UNALIGNED, index + 8));
                v4 = mix(v4, input.get(ValueLayout.JAVA_INT_UNALIGNED, index + 12));
                index += BLOCK_SIZE;
            }

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
        }
        else {
            hash = seed + PRIME32_5;
        }

        hash += (int) length;

        while (index <= length - 4) {
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
    public XxHash32Hasher update(byte[] input)
    {
        return update(input, 0, input.length);
    }

    @Override
    public XxHash32Hasher update(byte[] input, int offset, int length)
    {
        checkFromIndexSize(offset, length, input.length);

        int index = offset;
        int remaining = length;

        // Fill buffer if partially filled
        if (bufferSize > 0) {
            int available = min(BLOCK_SIZE - bufferSize, remaining);
            System.arraycopy(input, index, buffer, bufferSize, available);

            bufferSize += available;
            index += available;
            remaining -= available;

            if (bufferSize == BLOCK_SIZE) {
                updateBodyFromBuffer();
                bufferSize = 0;
            }
        }

        // Process full 16-byte blocks directly from input
        while (remaining >= BLOCK_SIZE) {
            v1 = mix(v1, (int) INT_HANDLE.get(input, index));
            v2 = mix(v2, (int) INT_HANDLE.get(input, index + 4));
            v3 = mix(v3, (int) INT_HANDLE.get(input, index + 8));
            v4 = mix(v4, (int) INT_HANDLE.get(input, index + 12));

            index += BLOCK_SIZE;
            remaining -= BLOCK_SIZE;
            bodyLength += BLOCK_SIZE;
        }

        // Buffer remaining bytes
        if (remaining > 0) {
            System.arraycopy(input, index, buffer, bufferSize, remaining);
            bufferSize += remaining;
        }

        return this;
    }

    @Override
    public XxHash32Hasher update(MemorySegment input)
    {
        byte[] array = input.heapBase()
                .filter(base -> base instanceof byte[])
                .map(base -> (byte[]) base)
                .orElse(null);

        if (array != null) {
            return update(array, toIntExact(input.address()), toIntExact(input.byteSize()));
        }

        // Fall back to MemorySegment access for native memory
        return updateSegment(input);
    }

    private XxHash32Hasher updateSegment(MemorySegment input)
    {
        long length = input.byteSize();
        long inputOffset = 0;

        if (bufferSize > 0) {
            int available = (int) min(BLOCK_SIZE - bufferSize, length);
            for (int i = 0; i < available; i++) {
                buffer[bufferSize + i] = input.get(ValueLayout.JAVA_BYTE, inputOffset + i);
            }

            bufferSize += available;
            inputOffset += available;
            length -= available;

            if (bufferSize == BLOCK_SIZE) {
                updateBodyFromBuffer();
                bufferSize = 0;
            }
        }

        while (length >= BLOCK_SIZE) {
            v1 = mix(v1, input.get(ValueLayout.JAVA_INT_UNALIGNED, inputOffset));
            v2 = mix(v2, input.get(ValueLayout.JAVA_INT_UNALIGNED, inputOffset + 4));
            v3 = mix(v3, input.get(ValueLayout.JAVA_INT_UNALIGNED, inputOffset + 8));
            v4 = mix(v4, input.get(ValueLayout.JAVA_INT_UNALIGNED, inputOffset + 12));

            inputOffset += BLOCK_SIZE;
            length -= BLOCK_SIZE;
            bodyLength += BLOCK_SIZE;
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
    public XxHash32Hasher updateLE(int value)
    {
        byte[] bytes = new byte[4];
        INT_HANDLE.set(bytes, 0, value);
        return update(bytes);
    }

    private void updateBodyFromBuffer()
    {
        v1 = mix(v1, (int) INT_HANDLE.get(buffer, 0));
        v2 = mix(v2, (int) INT_HANDLE.get(buffer, 4));
        v3 = mix(v3, (int) INT_HANDLE.get(buffer, 8));
        v4 = mix(v4, (int) INT_HANDLE.get(buffer, 12));
        bodyLength += BLOCK_SIZE;
    }

    @Override
    public int digest()
    {
        int hash;
        if (bodyLength > 0) {
            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
        }
        else {
            hash = seed + PRIME32_5;
        }

        hash += (int) (bodyLength + bufferSize);

        // Process remaining bytes in buffer
        int index = 0;
        while (index <= bufferSize - 4) {
            hash = updateTail(hash, (int) INT_HANDLE.get(buffer, index));
            index += 4;
        }

        while (index < bufferSize) {
            hash = updateTail(hash, buffer[index]);
            index++;
        }

        return finalShuffle(hash);
    }

    @Override
    public XxHash32Hasher reset()
    {
        return reset(DEFAULT_SEED);
    }

    @Override
    public XxHash32Hasher reset(int seed)
    {
        this.seed = seed;
        resetState(seed);
        return this;
    }

    @Override
    public void close() {}

    private static int mix(int current, int value)
    {
        return rotateLeft(current + value * PRIME32_2, 13) * PRIME32_1;
    }

    private static int updateTail(int hash, int value)
    {
        return rotateLeft(hash + value * PRIME32_3, 17) * PRIME32_4;
    }

    private static int updateTail(int hash, byte value)
    {
        int unsigned = value & 0xFF;
        return rotateLeft(hash + unsigned * PRIME32_5, 11) * PRIME32_1;
    }

    private static int finalShuffle(int hash)
    {
        hash ^= hash >>> 15;
        hash *= PRIME32_2;
        hash ^= hash >>> 13;
        hash *= PRIME32_3;
        hash ^= hash >>> 16;
        return hash;
    }
}
