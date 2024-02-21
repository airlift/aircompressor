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
package io.airlift.compress.zlib;

import io.airlift.compress.MalformedInputException;

import java.util.StringJoiner;

import static io.airlift.compress.zlib.UnsafeUtil.UNSAFE;

final class InputReader
{
    private final Object inputBase;
    private final long inputAddress;
    private final long inputLimit;

    private long inputPosition;
    private int bitCount;
    private int bitBuffer;

    public InputReader(Object inputBase, long inputAddress, long inputLimit)
    {
        checkArgument(inputAddress >= 0, "inputAddress is negative");
        checkArgument(inputAddress <= inputLimit, "inputAddress exceeds inputLimit");
        if (inputAddress == inputLimit) {
            throw new MalformedInputException(0, "Input is empty");
        }

        this.inputBase = inputBase;
        this.inputAddress = inputAddress;
        this.inputLimit = inputLimit;
        this.inputPosition = inputAddress;
    }

    public int bits(int need)
    {
        int accumulator = bitBuffer;

        while (bitCount < need) {
            int octet = readByte();
            accumulator |= octet << bitCount;
            bitCount += 8;
        }

        bitBuffer = accumulator >> need;
        bitCount -= need;

        return accumulator & mask(need);
    }

    public int peek(int need)
    {
        return peek(need, mask(need));
    }

    public int peek(int need, int mask)
    {
        while (bitCount < need) {
            if (available() == 0) {
                return bitBuffer & mask(bitCount);
            }
            bitBuffer |= readByte() << bitCount;
            bitCount += 8;
        }

        return bitBuffer & mask;
    }

    public void skip(int need)
    {
        bits(need);
    }

    public int readByte()
    {
        if (inputPosition >= inputLimit) {
            throw new MalformedInputException(offset(), "Input is truncated");
        }

        int octet = UNSAFE.getByte(inputBase, inputPosition) & 0xFF;
        inputPosition++;

        return octet;
    }

    public long offset()
    {
        return inputPosition - inputAddress;
    }

    public long available()
    {
        return inputLimit - inputPosition;
    }

    public void clear()
    {
        if (bitCount >= 8) {
            throw new MalformedInputException(offset(), "Too many partial bits: " + bitCount);
        }

        bitCount = 0;
        bitBuffer = 0;
    }

    public void copyMemory(Object outputBase, long outputPosition, int length)
    {
        if (available() < length) {
            throw new MalformedInputException(offset(), "Input is truncated");
        }

        UNSAFE.copyMemory(inputBase, inputPosition, outputBase, outputPosition, length);
        inputPosition += length;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", getClass().getSimpleName() + "{", "}")
                .add("offset=" + offset())
                .add("available=" + available())
                .toString();
    }

    public static int mask(int bits)
    {
        return (1 << bits) - 1;
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
