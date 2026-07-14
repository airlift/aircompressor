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

import io.airlift.compress.v3.MalformedInputException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static io.airlift.compress.v3.lz4.Lz4Constants.LAST_LITERAL_SIZE;
import static io.airlift.compress.v3.lz4.Lz4Constants.MIN_MATCH;
import static io.airlift.compress.v3.lz4.Lz4Constants.SIZE_OF_INT;
import static io.airlift.compress.v3.lz4.Lz4Constants.SIZE_OF_LONG;
import static io.airlift.compress.v3.lz4.Lz4Constants.SIZE_OF_SHORT;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Safe ({@code Unsafe}-free) LZ4 block decompressor built on {@link MemorySegment}. Bounds-checked equivalent of
 * {@link Lz4RawDecompressor}, which is kept as the {@code Unsafe} reference/oracle.
 *
 * <p>Every offset and limit that addresses a {@code MemorySegment} is a {@code long}, and the copy loops are driven
 * by comparing the 64-bit output pointer directly (rather than an {@code int} counter). This keeps all pointer math
 * in 64-bit registers with no {@code int}->{@code long} (I2L) widening, so C2 can anchor its bounds-check elimination
 * on long induction variables -- which measurably closes the gap to the {@code Unsafe} implementation.
 */
final class Lz4SafeRawDecompressor
{
    private static final long[] DEC_32_TABLE = {4L, 1L, 2L, 1L, 4L, 4L, 4L, 4L};
    private static final long[] DEC_64_TABLE = {0L, 0L, 0L, -1L, 0L, 1L, 2L, 3L};

    private static final int OFFSET_SIZE = 2;
    private static final int TOKEN_SIZE = 1;

    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final ValueLayout.OfShort SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private Lz4SafeRawDecompressor() {}

    /**
     * Decompresses an LZ4 block. The {@code input} segment must contain valid data in
     * {@code [inputOffset, inputOffset + inputLength)} plus at least {@code SIZE_OF_LONG} bytes of slack beyond it to
     * allow long-at-a-time over-read. The {@code output} segment must be at least {@code outputOffset + maxOutputLength}
     * bytes.
     */
    public static int decompress(
            final MemorySegment inputBase,
            final int inputOffset,
            final int inputLength,
            final MemorySegment outputBase,
            final int outputOffset,
            final int maxOutputLength)
    {
        final long inputAddress = inputOffset;
        final long inputLimit = (long) inputOffset + inputLength;
        final long outputAddress = outputOffset;
        final long outputLimit = (long) outputOffset + maxOutputLength;

        final long fastOutputLimit = outputLimit - SIZE_OF_LONG; // maximum offset in output buffer to which it's safe to write long-at-a-time

        // Fast-loop shortcut bounds (mirrors LZ4_decompress_generic): inside the shortcut we overcopy 16 literal
        // bytes and 24 match bytes, so the cursors must stay this far from the end to keep every wide access in bounds.
        final long shortInputLimit = inputLimit - 16;
        final long shortOutputLimit = outputLimit - 40;

        long input = inputAddress;
        long output = outputAddress;

        if (inputAddress == inputLimit) {
            throw malformed(0, "input is empty");
        }

        if (outputAddress == outputLimit) {
            if (inputLimit - inputAddress == 1 && inputBase.get(BYTE, inputAddress) == 0) {
                return 0;
            }
            return -1;
        }

        while (input < inputLimit) {
            final int token = inputBase.get(BYTE, input++) & 0xFF;

            // decode literal length
            int literalLength = token >>> 4; // top-most 4 bits of token

            // Fast-loop shortcut: the common case is a short literal (< 15) followed by a short match (< 15) at a
            // wide-enough offset. Both nibbles come from the token and the offset can be peeked before copying, so
            // every condition is known up front -- handle it with two wide overcopies and no length-decode branches.
            // Ported from the reference lz4.c (LZ4_decompress_generic shortcut; BSD-2-Clause).
            if (literalLength != 0xF && (token & 0xF) != 0xF && input <= shortInputLimit && output <= shortOutputLimit) {
                long offset = inputBase.get(SHORT, input + literalLength) & 0xFFFFL;
                long matchAddress = output + literalLength - offset;
                if (offset >= SIZE_OF_LONG && matchAddress >= outputAddress) {
                    int matchLength = token & 0xF; // bottom-most 4 bits of token
                    // copy 16 literal bytes (overcopy; the upcoming match overwrites any slack)
                    outputBase.set(LONG, output, inputBase.get(LONG, input));
                    outputBase.set(LONG, output + 8, inputBase.get(LONG, input + 8));
                    long matchOutput = output + literalLength;
                    // copy 24 match bytes (overcopy) 8 at a time; offset >= 8 keeps each long read >= 8 behind the
                    // write that consumes it, so the in-order copy reproduces the (possibly repeating) match
                    outputBase.set(LONG, matchOutput, outputBase.get(LONG, matchAddress));
                    outputBase.set(LONG, matchOutput + 8, outputBase.get(LONG, matchAddress + 8));
                    outputBase.set(LONG, matchOutput + 16, outputBase.get(LONG, matchAddress + 16));
                    output = matchOutput + matchLength + MIN_MATCH;
                    input += literalLength + SIZE_OF_SHORT;
                    continue;
                }
            }
            if (literalLength == 0xF) {
                if (input >= inputLimit) {
                    throw malformed(input - inputAddress);
                }
                int value;
                do {
                    value = inputBase.get(BYTE, input++) & 0xFF;
                    literalLength += value;
                }
                while (value == 255 && input < inputLimit - 15);
            }
            if (literalLength < 0) {
                throw malformed(input - inputAddress);
            }

            // copy literal
            long literalEnd = input + literalLength;
            long literalOutputLimit = output + literalLength;
            if (literalOutputLimit > (fastOutputLimit - MIN_MATCH) || literalEnd > inputLimit - (OFFSET_SIZE + TOKEN_SIZE + LAST_LITERAL_SIZE)) {
                // copy the last literal and finish
                if (literalOutputLimit > outputLimit) {
                    throw malformed(input - inputAddress, "attempt to write last literal outside of destination buffer");
                }

                if (literalEnd != inputLimit) {
                    throw malformed(input - inputAddress, "all input must be consumed");
                }

                // slow, precise copy
                MemorySegment.copy(inputBase, input, outputBase, output, literalLength);
                output += literalLength;
                break;
            }

            // fast copy. We may overcopy but there's enough room in input and output to not overrun them
            // long-driven loop (compare the 64-bit output pointer directly) so C2 anchors ABCE on a long induction var
            do {
                outputBase.set(LONG, output, inputBase.get(LONG, input));
                output += SIZE_OF_LONG;
                input += SIZE_OF_LONG;
            }
            while (output < literalOutputLimit);
            output = literalOutputLimit;

            input = literalEnd;

            // get offset
            // we know we can read two bytes because of the bounds check performed before copying the literal above
            // widen to long immediately so all subsequent pointer math stays 64-bit (no I2L casts)
            long offset = inputBase.get(SHORT, input) & 0xFFFFL;
            input += SIZE_OF_SHORT;

            long matchAddress = output - offset;
            if (matchAddress < outputAddress || matchAddress >= output) {
                throw malformed(input - inputAddress, "offset outside destination buffer");
            }

            // compute match length
            int matchLength = token & 0xF; // bottom-most 4 bits of token
            if (matchLength == 0xF) {
                int value;
                do {
                    if (input > inputLimit - LAST_LITERAL_SIZE) {
                        throw malformed(input - inputAddress);
                    }

                    value = inputBase.get(BYTE, input++) & 0xFF;
                    matchLength += value;
                }
                while (value == 255);
            }
            matchLength += MIN_MATCH; // implicit length from initial 4-byte match in encoder
            if (matchLength < 0) {
                throw malformed(input - inputAddress);
            }

            long matchOutputLimit = output + matchLength;

            // at this point we have at least 12 bytes of space in the output buffer
            // due to the fastLimit check before copying a literal, so no need to check again

            // copy repeated sequence
            if (offset < SIZE_OF_LONG) {
                // 8 bytes apart so that we can copy long-at-a-time below
                long increment32 = DEC_32_TABLE[(int) offset];
                long decrement64 = DEC_64_TABLE[(int) offset];

                outputBase.set(BYTE, output, outputBase.get(BYTE, matchAddress));
                outputBase.set(BYTE, output + 1, outputBase.get(BYTE, matchAddress + 1));
                outputBase.set(BYTE, output + 2, outputBase.get(BYTE, matchAddress + 2));
                outputBase.set(BYTE, output + 3, outputBase.get(BYTE, matchAddress + 3));
                output += SIZE_OF_INT;
                matchAddress += increment32;

                outputBase.set(INT, output, outputBase.get(INT, matchAddress));
                output += SIZE_OF_INT;
                matchAddress -= decrement64;
            }
            else {
                outputBase.set(LONG, output, outputBase.get(LONG, matchAddress));
                matchAddress += SIZE_OF_LONG;
                output += SIZE_OF_LONG;
            }

            if (matchOutputLimit > fastOutputLimit - MIN_MATCH) {
                if (matchOutputLimit > outputLimit - LAST_LITERAL_SIZE) {
                    throw malformed(input - inputAddress, String.format("last %s bytes must be literals", LAST_LITERAL_SIZE));
                }

                while (output < fastOutputLimit) {
                    outputBase.set(LONG, output, outputBase.get(LONG, matchAddress));
                    matchAddress += SIZE_OF_LONG;
                    output += SIZE_OF_LONG;
                }

                while (output < matchOutputLimit) {
                    outputBase.set(BYTE, output++, outputBase.get(BYTE, matchAddress++));
                }
            }
            else {
                // long-driven loop (compare the 64-bit output pointer directly); first long copied previously
                do {
                    outputBase.set(LONG, output, outputBase.get(LONG, matchAddress));
                    output += SIZE_OF_LONG;
                    matchAddress += SIZE_OF_LONG;
                }
                while (output < matchOutputLimit);
            }

            output = matchOutputLimit; // correction in case we overcopied
        }

        return (int) (output - outputAddress);
    }

    // Error construction is pulled out of decompress() so the never-taken throw sites do not inflate the hot
    // method's bytecode. Keeping decompress() small lets C2 stay within its inlining budget for the MemorySegment
    // accessors (their bounds/session checks then hoist out of the copy loops), measurably improving throughput.
    private static MalformedInputException malformed(long offset)
    {
        return new MalformedInputException(offset);
    }

    private static MalformedInputException malformed(long offset, String reason)
    {
        return new MalformedInputException(offset, reason);
    }
}
