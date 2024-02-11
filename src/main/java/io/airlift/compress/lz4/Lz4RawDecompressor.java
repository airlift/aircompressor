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

import io.airlift.compress.MalformedInputException;

import static io.airlift.compress.lz4.Lz4Constants.LAST_LITERAL_SIZE;
import static io.airlift.compress.lz4.Lz4Constants.MIN_MATCH;
import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_INT;
import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_LONG;
import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_SHORT;
import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;

public final class Lz4RawDecompressor
{
    private static final int[] DEC_32_TABLE = {4, 1, 2, 1, 4, 4, 4, 4};
    private static final int[] DEC_64_TABLE = {0, 0, 0, -1, 0, 1, 2, 3};

    private static final int OFFSET_SIZE = 2;
    private static final int TOKEN_SIZE = 1;

    private Lz4RawDecompressor() {}

    public static int decompress(
            final Object inputBase,
            final long inputAddress,
            final long inputLimit,
            final Object outputBase,
            final long outputAddress,
            final long outputLimit)
    {
        final long fastOutputLimit = outputLimit - SIZE_OF_LONG; // maximum offset in output buffer to which it's safe to write long-at-a-time

        long input = inputAddress;
        long output = outputAddress;

        if (inputAddress == inputLimit) {
            throw new MalformedInputException(0, "input is empty");
        }

        if (outputAddress == outputLimit) {
            if (inputLimit - inputAddress == 1 && UNSAFE.getByte(inputBase, inputAddress) == 0) {
                return 0;
            }
            return -1;
        }

        while (input < inputLimit) {
            final int token = UNSAFE.getByte(inputBase, input++) & 0xFF;

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token
            if (literalLength == 0xF) {
                if (input >= inputLimit) {
                    throw new MalformedInputException(input - inputAddress);
                }
                int value;
                do {
                    value = UNSAFE.getByte(inputBase, input++) & 0xFF;
                    literalLength += value;
                }
                while (value == 255 && input < inputLimit - 15);
            }
            if (literalLength < 0) {
                throw new MalformedInputException(input - inputAddress);
            }

            // copy literal
            long literalEnd = input + literalLength;
            long literalOutputLimit = output + literalLength;
            if (literalOutputLimit > (fastOutputLimit - MIN_MATCH) || literalEnd > inputLimit - (OFFSET_SIZE + TOKEN_SIZE + LAST_LITERAL_SIZE)) {
                // copy the last literal and finish
                if (literalOutputLimit > outputLimit) {
                    throw new MalformedInputException(input - inputAddress, "attempt to write last literal outside of destination buffer");
                }

                if (literalEnd != inputLimit) {
                    throw new MalformedInputException(input - inputAddress, "all input must be consumed");
                }

                // slow, precise copy
                UNSAFE.copyMemory(inputBase, input, outputBase, output, literalLength);
                output += literalLength;
                break;
            }

            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
            int index = 0;
            do {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
                output += SIZE_OF_LONG;
                input += SIZE_OF_LONG;
                index += SIZE_OF_LONG;
            }
            while (index < literalLength);
            output = literalOutputLimit;

            input = literalEnd;

            // get offset
            // we know we can read two bytes because of the bounds check performed before copying the literal above
            int offset = UNSAFE.getShort(inputBase, input) & 0xFFFF;
            input += SIZE_OF_SHORT;

            long matchAddress = output - offset;
            if (matchAddress < outputAddress) {
                throw new MalformedInputException(input - inputAddress, "offset outside destination buffer");
            }

            // compute match length
            int matchLength = token & 0xF; // bottom-most 4 bits of token
            if (matchLength == 0xF) {
                int value;
                do {
                    if (input > inputLimit - LAST_LITERAL_SIZE) {
                        throw new MalformedInputException(input - inputAddress);
                    }

                    value = UNSAFE.getByte(inputBase, input++) & 0xFF;
                    matchLength += value;
                }
                while (value == 255);
            }
            matchLength += MIN_MATCH; // implicit length from initial 4-byte match in encoder
            if (matchLength < 0) {
                throw new MalformedInputException(input - inputAddress);
            }

            long matchOutputLimit = output + matchLength;

            // at this point we have at least 12 bytes of space in the output buffer
            // due to the fastLimit check before copying a literal, so no need to check again

            // copy repeated sequence
            if (offset < SIZE_OF_LONG) {
                // 8 bytes apart so that we can copy long-at-a-time below
                int increment32 = DEC_32_TABLE[offset];
                int decrement64 = DEC_64_TABLE[offset];

                UNSAFE.putByte(outputBase, output, UNSAFE.getByte(outputBase, matchAddress));
                UNSAFE.putByte(outputBase, output + 1, UNSAFE.getByte(outputBase, matchAddress + 1));
                UNSAFE.putByte(outputBase, output + 2, UNSAFE.getByte(outputBase, matchAddress + 2));
                UNSAFE.putByte(outputBase, output + 3, UNSAFE.getByte(outputBase, matchAddress + 3));
                output += SIZE_OF_INT;
                matchAddress += increment32;

                UNSAFE.putInt(outputBase, output, UNSAFE.getInt(outputBase, matchAddress));
                output += SIZE_OF_INT;
                matchAddress -= decrement64;
            }
            else {
                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }

            if (matchOutputLimit > fastOutputLimit - MIN_MATCH) {
                if (matchOutputLimit > outputLimit - LAST_LITERAL_SIZE) {
                    throw new MalformedInputException(input - inputAddress, String.format("last %s bytes must be literals", LAST_LITERAL_SIZE));
                }

                while (output < fastOutputLimit) {
                    UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                    matchAddress += SIZE_OF_LONG;
                    output += SIZE_OF_LONG;
                }

                while (output < matchOutputLimit) {
                    UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                }
            }
            else {
                int i = 0;
                do {
                    UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                    output += SIZE_OF_LONG;
                    matchAddress += SIZE_OF_LONG;
                    i += SIZE_OF_LONG;
                }
                while (i < matchLength - SIZE_OF_LONG); // first long copied previously
            }

            output = matchOutputLimit; // correction in case we overcopied
        }

        return (int) (output - outputAddress);
    }
}
