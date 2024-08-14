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

import java.util.Arrays;

import static io.airlift.compress.v3.lz4.Lz4Constants.LAST_LITERAL_SIZE;
import static io.airlift.compress.v3.lz4.Lz4Constants.MIN_MATCH;
import static io.airlift.compress.v3.lz4.Lz4Constants.SIZE_OF_LONG;
import static io.airlift.compress.v3.lz4.Lz4Constants.SIZE_OF_SHORT;
import static io.airlift.compress.v3.lz4.UnsafeUtil.UNSAFE;

final class Lz4RawCompressor
{
    private static final int MAX_INPUT_SIZE = 0x7E000000;   /* 2 113 929 216 bytes */

    private static final int HASH_LOG = 12;

    private static final int MIN_TABLE_SIZE = 16;
    public static final int MAX_TABLE_SIZE = (1 << HASH_LOG);

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

    private static int hash(long value, int mask)
    {
        // Multiplicative hash. It performs the equivalent to
        // this computation:
        //
        //  value * frac(a)
        //
        // for some real number 'a' with a good & random mix
        // of 1s and 0s in its binary representation
        //
        // For performance, it does it using fixed point math
        return (int) ((value * 889523592379L >>> 28) & mask);
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
        int tableSize = computeTableSize(inputLength);
        Arrays.fill(table, 0, tableSize, 0);

        int mask = tableSize - 1;

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

        long anchor = input;

        // First Byte
        // put position in hash
        table[hash(UNSAFE.getLong(inputBase, input), mask)] = (int) (input - inputAddress);

        input++;
        int nextHash = hash(UNSAFE.getLong(inputBase, input), mask);

        boolean done = false;
        do {
            long nextInputIndex = input;
            int findMatchAttempts = 1 << SKIP_TRIGGER;
            int step = 1;

            // find 4-byte match
            long matchIndex;
            do {
                int hash = nextHash;
                input = nextInputIndex;
                nextInputIndex += step;

                step = (findMatchAttempts++) >>> SKIP_TRIGGER;

                if (nextInputIndex > matchFindLimit) {
                    return (int) (emitLastLiteral(outputBase, output, inputBase, anchor, inputLimit - anchor) - outputAddress);
                }

                // get position on hash
                matchIndex = inputAddress + table[hash];
                nextHash = hash(UNSAFE.getLong(inputBase, nextInputIndex), mask);

                // put position on hash
                table[hash] = (int) (input - inputAddress);
            }
            while (UNSAFE.getInt(inputBase, matchIndex) != UNSAFE.getInt(inputBase, input) || matchIndex + MAX_DISTANCE < input);

            // catch up
            while ((input > anchor) && (matchIndex > inputAddress) && (UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, matchIndex - 1))) {
                --input;
                --matchIndex;
            }

            int literalLength = (int) (input - anchor);
            long tokenAddress = output;

            output = emitLiteral(inputBase, outputBase, anchor, literalLength, tokenAddress);

            // next match
            while (true) {
                // find match length
                int matchLength = count(inputBase, input + MIN_MATCH, matchLimit, matchIndex + MIN_MATCH);
                output = emitMatch(outputBase, output, tokenAddress, (short) (input - matchIndex), matchLength);

                input += matchLength + MIN_MATCH;

                anchor = input;

                // are we done?
                if (input > matchFindLimit) {
                    done = true;
                    break;
                }

                long position = input - 2;
                table[hash(UNSAFE.getLong(inputBase, position), mask)] = (int) (position - inputAddress);

                // Test next position
                int hash = hash(UNSAFE.getLong(inputBase, input), mask);
                matchIndex = inputAddress + table[hash];
                table[hash] = (int) (input - inputAddress);

                if (matchIndex + MAX_DISTANCE < input || UNSAFE.getInt(inputBase, matchIndex) != UNSAFE.getInt(inputBase, input)) {
                    input++;
                    nextHash = hash(UNSAFE.getLong(inputBase, input), mask);
                    break;
                }

                // go for another match
                tokenAddress = output++;
                UNSAFE.putByte(outputBase, tokenAddress, (byte) 0);
            }
        }
        while (!done);

        // Encode Last Literals
        output = emitLastLiteral(outputBase, output, inputBase, anchor, inputLimit - anchor);

        return (int) (output - outputAddress);
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

    /**
     * matchAddress must be < inputAddress
     */
    static int count(Object inputBase, final long inputAddress, final long inputLimit, final long matchAddress)
    {
        long input = inputAddress;
        long match = matchAddress;

        int remaining = (int) (inputLimit - inputAddress);

        // first, compare long at a time
        int count = 0;
        while (count < remaining - (SIZE_OF_LONG - 1)) {
            long diff = UNSAFE.getLong(inputBase, match) ^ UNSAFE.getLong(inputBase, input);
            if (diff != 0) {
                return count + (Long.numberOfTrailingZeros(diff) >> 3);
            }

            count += SIZE_OF_LONG;
            input += SIZE_OF_LONG;
            match += SIZE_OF_LONG;
        }

        while (count < remaining && UNSAFE.getByte(inputBase, match) == UNSAFE.getByte(inputBase, input)) {
            count++;
            match++;
            input++;
        }

        return count;
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

    private static int computeTableSize(int inputSize)
    {
        // smallest power of 2 larger than inputSize
        int target = Integer.highestOneBit(inputSize - 1) << 1;

        // keep it between MIN_TABLE_SIZE and MAX_TABLE_SIZE
        return Math.max(Math.min(target, MAX_TABLE_SIZE), MIN_TABLE_SIZE);
    }
}
