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

import static io.airlift.compress.lz4.Lz4Constants.LAST_LITERAL_SIZE;
import static io.airlift.compress.lz4.Lz4Constants.MIN_MATCH;
import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_INT;
import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_LONG;
import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_SHORT;
import static io.airlift.compress.lz4.UnsafeUtil.UNSAFE;

public final class Lz4RawCompressor
{
    private static final int MAX_INPUT_SIZE = 0x7E000000;   /* 2 113 929 216 bytes */

    private static final int MEMORY_USAGE = 14;
    public static final int STREAM_SIZE = 2 * ((1 << (MEMORY_USAGE - 3)) + 4);

    private static final int HASHLOG = MEMORY_USAGE - 2;
    private static final int HASH_SHIFT = 40 - HASHLOG;
    private static final int HASH_MASK = (1 << HASHLOG) - 1;

    private static final int COPY_LENGTH = 8;
    private static final int MATCH_FIND_LIMIT = COPY_LENGTH + MIN_MATCH;

    private static final int MIN_LENGTH = MATCH_FIND_LIMIT + 1;

    private static final int ML_BITS = 4;
    private static final int ML_MASK = (1 << ML_BITS) - 1;
    private static final int RUN_BITS = 8 - ML_BITS;
    private static final int RUN_MASK = (1 << RUN_BITS) - 1;

    private static final int MAX_DISTANCE = ((1 << 16) - 1);

    private static final int SKIP_TRIGGER = 6;  /* Increase this value ==> compression run slower on incompressible data */

    private Lz4RawCompressor() {}

    private static int hash(long value)
    {
        return (int) ((value * 889523592379L >>> HASH_SHIFT) & HASH_MASK);
    }

    public static int maxCompressedLength(int sourceLength)
    {
        return sourceLength + sourceLength / 255 + 16;
    }

    public static int compress(
            final Object inputBase,
            final long inputAddress,
            final int inputLength,
            final Object outputBase,
            final long outputAddress,
            final long maxOutputLength,
            final int[] table)
    {
        if (inputLength > MAX_INPUT_SIZE) {
            throw new IllegalArgumentException("Max input length exceeded");
        }

        if (maxOutputLength < maxCompressedLength(inputLength)) {
            throw new IllegalArgumentException("Max output length must be larger than " + maxCompressedLength(inputLength));
        }

        long input = inputAddress;
        long output = outputAddress;

        final long inputLimit = inputAddress + inputLength;
        final long matchFindLimit = inputLimit - MATCH_FIND_LIMIT;
        final long matchLimit = inputLimit - LAST_LITERAL_SIZE;

        if (inputLength < MIN_LENGTH) {
            output = emitLastLiteral(outputBase, output, inputBase, input, inputLimit - input);
            return (int) (output - outputAddress);
        }

        // record first position in hash
        table[hash(UNSAFE.getLong(inputBase, input))] = (int) (input - inputAddress);

        long anchor = input;
        while (true) {
            // find 4-byte match
            long matchIndex;

            int findMatchAttempts = 1 << SKIP_TRIGGER;
            int step = 1;
            input++;
            while (true) {
                long nextInput = input + step;
                if (nextInput > matchFindLimit) {
                    return (int) (emitLastLiteral(outputBase, output, inputBase, anchor, inputLimit - anchor) - outputAddress);
                }

                matchIndex = computeMatchAndUpdateTable(inputBase, inputAddress, input, table);

                if (UNSAFE.getInt(inputBase, matchIndex) == UNSAFE.getInt(inputBase, input) && matchIndex >= input - MAX_DISTANCE) {
                    break;
                }

                step = (findMatchAttempts++) >>> SKIP_TRIGGER;
                input = nextInput;
            }

            // see if we can find a longer match
            while ((input > anchor) && (matchIndex > inputAddress) && (UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, matchIndex - 1))) {
                --input;
                --matchIndex;
            }

            long tokenAddress = output;
            output = emitLiteral(inputBase, outputBase, anchor, (int) (input - anchor), tokenAddress);

            while (true) {
                int matchLength = count(inputBase, input + MIN_MATCH, matchIndex + MIN_MATCH, matchLimit);
                output = emitMatch(outputBase, output, tokenAddress, (short) (input - matchIndex), matchLength);
                input += matchLength + MIN_MATCH;

                if (input > matchFindLimit) {
                    output = emitLastLiteral(outputBase, output, inputBase, input, inputLimit - input);
                    return (int) (output - outputAddress);
                }

                anchor = input;

                // update table to improve chances of matching
                long position = input - 2;
                table[hash(UNSAFE.getLong(inputBase, position))] = (int) (position - inputAddress);

                // try another match at current position. This is, effectively, one iteration of the matching
                // loop at the top, with a literal of length 0
                matchIndex = computeMatchAndUpdateTable(inputBase, inputAddress, input, table);

                if (UNSAFE.getInt(inputBase, matchIndex) != UNSAFE.getInt(inputBase, input) || matchIndex < input - MAX_DISTANCE) {
                    // we couldn't find one, so continue matching at the next position (followed by literal, etc)
                    break;
                }

                // special case of a token for a 0-length literal. The match length will be set in the next iteration of this loop
                tokenAddress = output++;
                UNSAFE.putByte(outputBase, tokenAddress, (byte) 0);
            }
        }
    }

    private static long computeMatchAndUpdateTable(Object inputBase, long inputAddress, long input, int[] table)
    {
        int hash = hash(UNSAFE.getLong(inputBase, input));
        long matchIndex = inputAddress + table[hash];
        table[hash] = (int) (input - inputAddress);
        return matchIndex;
    }

    private static long emitLiteral(Object inputBase, Object outputBase, long input, int literalLength, long output)
    {
        output = encodeRunLength(outputBase, output, literalLength);

        final long outputLimit = output + literalLength;
        do {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
        }
        while (output < outputLimit);

        return outputLimit;
    }

    private static long emitMatch(Object outputBase, long output, long tokenAddress, short offset, long matchLength)
    {
        // write offset
        UNSAFE.putShort(outputBase, output, offset);
        output += SIZE_OF_SHORT;

        // write match length
        if (matchLength >= ML_MASK) {
            UNSAFE.putByte(outputBase, tokenAddress, (byte) (UNSAFE.getByte(outputBase, tokenAddress) | ML_MASK));
            long remaining = matchLength - ML_MASK;
            while (remaining >= 510) {
                UNSAFE.putShort(outputBase, output, (short) 0xFFFF);
                output += SIZE_OF_SHORT;
                remaining -= 510;
            }
            if (remaining >= 255) {
                UNSAFE.putByte(outputBase, output++, (byte) 255);
                remaining -= 255;
            }
            UNSAFE.putByte(outputBase, output++, (byte) remaining);
        }
        else {
            UNSAFE.putByte(outputBase, tokenAddress, (byte) (UNSAFE.getByte(outputBase, tokenAddress) | matchLength));
        }

        return output;
    }

    private static int count(Object inputBase, final long start, long matchStart, long matchLimit)
    {
        long current = start;

        // first, compare long at a time
        while (current < matchLimit - (SIZE_OF_LONG - 1)) {
            long diff = UNSAFE.getLong(inputBase, matchStart) ^ UNSAFE.getLong(inputBase, current);
            if (diff != 0) {
                current += Long.numberOfTrailingZeros(diff) >> 3;
                return (int) (current - start);
            }

            current += SIZE_OF_LONG;
            matchStart += SIZE_OF_LONG;
        }

        if (current < matchLimit - (SIZE_OF_INT - 1) && UNSAFE.getInt(inputBase, matchStart) == UNSAFE.getInt(inputBase, current)) {
            current += SIZE_OF_INT;
            matchStart += SIZE_OF_INT;
        }

        if (current < matchLimit - (SIZE_OF_SHORT - 1) && UNSAFE.getShort(inputBase, matchStart) == UNSAFE.getShort(inputBase, current)) {
            current += SIZE_OF_SHORT;
            matchStart += SIZE_OF_SHORT;
        }

        if (current < matchLimit && UNSAFE.getByte(inputBase, matchStart) == UNSAFE.getByte(inputBase, current)) {
            ++current;
        }

        return (int) (current - start);
    }

    private static long emitLastLiteral(
            final Object outputBase,
            final long outputAddress,
            final Object inputBase,
            final long inputAddress,
            final long length)
    {
        long output = encodeRunLength(outputBase, outputAddress, length);
        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, output, length);

        return output + length;
    }

    private static long encodeRunLength(
            final Object base,
            long output,
            final long length)
    {
        if (length >= RUN_MASK) {
            UNSAFE.putByte(base, output++, (byte) (RUN_MASK << ML_BITS));

            long remaining = length - RUN_MASK;
            while (remaining >= 255) {
                UNSAFE.putByte(base, output++, (byte) 255);
                remaining -= 255;
            }
            UNSAFE.putByte(base, output++, (byte) remaining);
        }
        else {
            UNSAFE.putByte(base, output++, (byte) (length << ML_BITS));
        }

        return output;
    }
}
