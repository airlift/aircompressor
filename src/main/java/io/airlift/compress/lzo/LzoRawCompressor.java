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
package io.airlift.compress.lzo;

import java.util.Arrays;

import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_INT;
import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_LONG;
import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_SHORT;
import static io.airlift.compress.lzo.UnsafeUtil.UNSAFE;

public final class LzoRawCompressor
{
    public static final int LAST_LITERAL_SIZE = 5;
    public static final int MIN_MATCH = 4;

    private static final int MAX_INPUT_SIZE = 0x7E000000;   /* 2 113 929 216 bytes */

    private static final int HASH_LOG = 12;
    private static final int HASH_SHIFT = 40 - HASH_LOG;

    private static final int MIN_TABLE_SIZE = 16;
    public static final int MAX_TABLE_SIZE = (1 << HASH_LOG);

    private static final int COPY_LENGTH = 8;
    private static final int MATCH_FIND_LIMIT = COPY_LENGTH + MIN_MATCH;

    private static final int MIN_LENGTH = MATCH_FIND_LIMIT + 1;

    private static final int ML_BITS = 4;
    private static final int RUN_BITS = 8 - ML_BITS;
    private static final int RUN_MASK = (1 << RUN_BITS) - 1;

    private static final int MAX_DISTANCE = 0b1100_0000_0000_0000 - 1;

    private static final int SKIP_TRIGGER = 6;  /* Increase this value ==> compression run slower on incompressible data */

    private LzoRawCompressor() {}

    private static int hash(long value, int mask)
    {
        return (int) ((value * 889523592379L >>> HASH_SHIFT) & mask);
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

        // nothing compresses to nothing
        if (inputLength == 0) {
            return 0;
        }

        long input = inputAddress;
        long output = outputAddress;

        final long inputLimit = inputAddress + inputLength;
        final long matchFindLimit = inputLimit - MATCH_FIND_LIMIT;
        final long matchLimit = inputLimit - LAST_LITERAL_SIZE;

        if (inputLength < MIN_LENGTH) {
            output = emitLastLiteral(true, outputBase, output, inputBase, input, inputLimit - input);
            return (int) (output - outputAddress);
        }

        long anchor = input;

        // First Byte
        // put position in hash
        table[hash(UNSAFE.getLong(inputBase, input), mask)] = (int) (input - inputAddress);

        input++;
        int nextHash = hash(UNSAFE.getLong(inputBase, input), mask);

        boolean done = false;
        boolean firstLiteral = true;
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
                    output = emitLastLiteral(firstLiteral, outputBase, output, inputBase, anchor, inputLimit - anchor);
                    return (int) (output - outputAddress);
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

            output = emitLiteral(firstLiteral, inputBase, anchor, outputBase, output, literalLength);
            firstLiteral = false;

            // next match
            while (true) {
                int offset = (int) (input - matchIndex);

                // find match length
                input += MIN_MATCH;
                int matchLength = count(inputBase, input, matchIndex + MIN_MATCH, matchLimit);
                input += matchLength;

                // write copy command
                output = emitCopy(outputBase, output, offset, matchLength + MIN_MATCH);
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
            }
        }
        while (!done);

        // Encode Last Literals
        output = emitLastLiteral(false, outputBase, output, inputBase, anchor, inputLimit - anchor);

        return (int) (output - outputAddress);
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
            boolean firstLiteral,
            final Object outputBase,
            long output,
            final Object inputBase,
            final long inputAddress,
            final long literalLength)
    {
        output = encodeLiteralLength(firstLiteral, outputBase, output, literalLength);
        UNSAFE.copyMemory(inputBase, inputAddress, outputBase, output, literalLength);
        output += literalLength;

        // write stop command
        UNSAFE.putByte(outputBase, output++, (byte) 0b0001_0001);

        // write 2 zeros
        UNSAFE.putShort(outputBase, output, (byte) 0);
        output += SIZE_OF_SHORT;
        return output;
    }

    private static long emitLiteral(
            boolean firstLiteral,
            Object inputBase,
            long input,
            Object outputBase,
            long output,
            int literalLength)
    {
        output = encodeLiteralLength(firstLiteral, outputBase, output, literalLength);

        final long outputLimit = output + literalLength;
        do {
            UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
        }
        while (output < outputLimit);

        return outputLimit;
    }

    private static long encodeLiteralLength(
            boolean firstLiteral,
            final Object outBase,
            long output,
            long length)
    {
        if (firstLiteral && length < (0xFF - 17)) {
            UNSAFE.putByte(outBase, output++, (byte) (length + 17));
        }
        else if (length < 4) {
            // Small literals are encoded in the low two bits trailer of the previous command.  The
            // trailer is a little endian short, so we need to adjust the byte 2 back in the output.
            UNSAFE.putByte(outBase, output - 2, (byte) (UNSAFE.getByte(outBase, output - 2) | length));
        }
        else {
            length -= 3;
            if (length > RUN_MASK) {
                UNSAFE.putByte(outBase, output++, (byte) 0);

                long remaining = length - RUN_MASK;
                while (remaining > 255) {
                    UNSAFE.putByte(outBase, output++, (byte) 0);
                    remaining -= 255;
                }
                UNSAFE.putByte(outBase, output++, (byte) remaining);
            }
            else {
                UNSAFE.putByte(outBase, output++, (byte) length);
            }
        }
        return output;
    }

    private static long emitCopy(Object outputBase, long output, int matchOffset, int matchLength)
    {
        if (matchOffset > MAX_DISTANCE || matchOffset < 1) {
            throw new IllegalArgumentException("Unsupported copy offset: " + matchOffset);
        }

        // use short command for small copy with small offset
        if (matchLength <= 8 && matchOffset <= 2048) {
            // 0bMMMP_PPLL 0bPPPP_PPPP

            // encodes matchLength and matchOffset - 1
            matchLength--;
            matchOffset--;

            UNSAFE.putByte(outputBase, output++, (byte) (((matchLength) << 5) | ((matchOffset & 0b111) << 2)));
            UNSAFE.putByte(outputBase, output++, (byte) (matchOffset >>> 3));

            return output;
        }

        // lzo encodes matchLength - 2
        matchLength -= 2;

        if (matchOffset >= (1 << 15)) {
            // 0b0001_1MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL
            output = encodeMatchLength(outputBase, output, matchLength, 0b0000_0111, 0b0001_1000);
        }
        else if (matchOffset > (1 << 14)) {
            // 0b0001_0MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL
            output = encodeMatchLength(outputBase, output, matchLength, 0b0000_0111, 0b0001_0000);
        }
        else {
            // 0b001M_MMMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL
            output = encodeMatchLength(outputBase, output, matchLength, 0b0001_1111, 0b0010_0000);

            // this command encodes matchOffset - 1
            matchOffset--;
        }

        output = encodeOffset(outputBase, output, matchOffset);
        return output;
    }

    private static long encodeOffset(final Object outputBase, final long outputAddress, final int offset)
    {
        UNSAFE.putShort(outputBase, outputAddress, (short) (offset << 2));
        return outputAddress + 2;
    }

    private static long encodeMatchLength(Object outputBase, long output, int matchLength, int baseMatchLength, int command)
    {
        if (matchLength <= baseMatchLength) {
            UNSAFE.putByte(outputBase, output++, (byte) (command | matchLength));
        }
        else {
            UNSAFE.putByte(outputBase, output++, (byte) command);
            long remaining = matchLength - baseMatchLength;
            while (remaining > 510) {
                UNSAFE.putShort(outputBase, output, (short) 0);
                output += SIZE_OF_SHORT;
                remaining -= 510;
            }
            if (remaining > 255) {
                UNSAFE.putByte(outputBase, output++, (byte) 0);
                remaining -= 255;
            }
            UNSAFE.putByte(outputBase, output++, (byte) remaining);
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
