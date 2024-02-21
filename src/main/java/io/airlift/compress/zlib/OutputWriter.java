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

final class OutputWriter
{
    private static final int SIZE_OF_INT = 4;
    private static final int SIZE_OF_LONG = 8;

    private static final int[] DEC_32_TABLE = {4, 1, 2, 1, 4, 4, 4, 4};
    private static final int[] DEC_64_TABLE = {0, 0, 0, -1, 0, 1, 2, 3};

    private final Object outputBase;
    private final long outputAddress;
    private final long outputLimit;
    private final long fastOutputLimit;

    private long outputPosition;

    public OutputWriter(Object outputBase, long outputAddress, long outputLimit)
    {
        checkArgument(outputAddress >= 0, "outputAddress is negative");
        checkArgument(outputAddress <= outputLimit, "outputAddress exceeds outputLimit");

        this.outputBase = outputBase;
        this.outputAddress = outputAddress;
        this.outputLimit = outputLimit;
        this.fastOutputLimit = outputLimit - SIZE_OF_LONG;
        this.outputPosition = outputAddress;
    }

    public long offset()
    {
        return outputPosition - outputAddress;
    }

    public long available()
    {
        return outputLimit - outputPosition;
    }

    public void writeByte(InputReader reader, byte value)
    {
        if (available() == 0) {
            throw new MalformedInputException(reader.offset(), "Output buffer is too small");
        }

        UNSAFE.putByte(outputBase, outputPosition, value);
        outputPosition++;
    }

    public void copyInput(InputReader reader, int length)
    {
        if (length > available()) {
            throw new MalformedInputException(reader.offset(), "Output buffer too small");
        }

        reader.copyMemory(outputBase, outputPosition, length);
        outputPosition += length;
    }

    public void copyOutput(InputReader reader, int distance, int length)
    {
        if (length > available()) {
            throw new MalformedInputException(reader.offset(), "Output buffer too small");
        }
        if (distance > offset()) {
            throw new MalformedInputException(reader.offset(), "Distance is too far back");
        }

        long matchAddress = outputPosition - distance;

        if (distance >= length) {
            UNSAFE.copyMemory(outputBase, matchAddress, outputBase, outputPosition, length);
            outputPosition += length;
            return;
        }

        long matchOutputLimit = outputPosition + length;

        if (outputPosition > fastOutputLimit) {
            // slow match copy
            while (outputPosition < matchOutputLimit) {
                UNSAFE.putByte(outputBase, outputPosition, UNSAFE.getByte(outputBase, matchAddress));
                matchAddress++;
                outputPosition++;
            }
            return;
        }

        // copy repeated sequence
        if (distance < SIZE_OF_LONG) {
            // 8 bytes apart so that we can copy long-at-a-time below
            int increment32 = DEC_32_TABLE[distance];
            int decrement64 = DEC_64_TABLE[distance];

            UNSAFE.putByte(outputBase, outputPosition, UNSAFE.getByte(outputBase, matchAddress));
            UNSAFE.putByte(outputBase, outputPosition + 1, UNSAFE.getByte(outputBase, matchAddress + 1));
            UNSAFE.putByte(outputBase, outputPosition + 2, UNSAFE.getByte(outputBase, matchAddress + 2));
            UNSAFE.putByte(outputBase, outputPosition + 3, UNSAFE.getByte(outputBase, matchAddress + 3));
            outputPosition += SIZE_OF_INT;
            matchAddress += increment32;

            UNSAFE.putInt(outputBase, outputPosition, UNSAFE.getInt(outputBase, matchAddress));
            outputPosition += SIZE_OF_INT;
            matchAddress -= decrement64;
        }
        else {
            UNSAFE.putLong(outputBase, outputPosition, UNSAFE.getLong(outputBase, matchAddress));
            matchAddress += SIZE_OF_LONG;
            outputPosition += SIZE_OF_LONG;
        }

        if (matchOutputLimit > fastOutputLimit) {
            while (outputPosition < fastOutputLimit) {
                UNSAFE.putLong(outputBase, outputPosition, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                outputPosition += SIZE_OF_LONG;
            }

            while (outputPosition < matchOutputLimit) {
                UNSAFE.putByte(outputBase, outputPosition, UNSAFE.getByte(outputBase, matchAddress));
                matchAddress++;
                outputPosition++;
            }
        }
        else {
            while (outputPosition < matchOutputLimit) {
                UNSAFE.putLong(outputBase, outputPosition, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                outputPosition += SIZE_OF_LONG;
            }
        }

        // correction in case we over-copied
        outputPosition = matchOutputLimit;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", getClass().getSimpleName() + "{", "}")
                .add("offset=" + offset())
                .add("available=" + available())
                .toString();
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
