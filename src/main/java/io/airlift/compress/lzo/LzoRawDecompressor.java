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

import io.airlift.compress.MalformedInputException;

import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_INT;
import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_LONG;
import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_SHORT;
import static io.airlift.compress.lzo.UnsafeUtil.UNSAFE;
import static java.lang.Integer.toBinaryString;

public final class LzoRawDecompressor
{
    private static final int[] DEC_32_TABLE = {4, 1, 2, 1, 4, 4, 4, 4};
    private static final int[] DEC_64_TABLE = {0, 0, 0, -1, 0, 1, 2, 3};

    private LzoRawDecompressor() {}

    public static int decompress(
            final Object inputBase,
            final long inputAddress,
            final long inputLimit,
            final Object outputBase,
            final long outputAddress,
            final long outputLimit)
            throws MalformedInputException
    {
        // nothing compresses to nothing
        if (inputAddress == inputLimit) {
            return 0;
        }

        // maximum offset in buffers to which it's safe to write long-at-a-time
        final long fastOutputLimit = outputLimit - SIZE_OF_LONG;

        // LZO can concat two blocks together so, decode until the input data is consumed
        long input = inputAddress;
        long output = outputAddress;
        while (input < inputLimit) {
            //
            // Note: For safety some of the code below may stop decoding early or skip decoding,
            // because input is not available.  This makes the code safe, and since LZO requires
            // an explicit "stop" command, the decoder will still throw a exception.
            //

            boolean firstCommand = true;
            int lastLiteralLength = 0;
            while (true) {
                if (input >= inputLimit) {
                    throw new MalformedInputException(input - inputAddress);
                }
                int command = UNSAFE.getByte(inputBase, input++) & 0xFF;
                if (command == 0b0001_0001) {
                    break;
                }

                // Commands are described using a bit pattern notation:
                // 0: bit is not set
                // 1: bit is set
                // L: part of literal length
                // P: part of match offset position
                // M: part of match length
                // ?: see documentation in command decoder

                int matchLength;
                int matchOffset;
                int literalLength;
                if ((command & 0b1111_0000) == 0b0000_0000) {
                    if (lastLiteralLength == 0) {
                        // 0b0000_LLLL (0bLLLL_LLLL)*

                        // copy length :: fixed
                        //   0
                        matchOffset = 0;

                        // copy offset :: fixed
                        //   0
                        matchLength = 0;

                        // literal length - 3 :: variable bits :: valid range [4..]
                        //   3 + variableLength(command bits [0..3], 4)
                        literalLength = command & 0b1111;
                        if (literalLength == 0) {
                            literalLength = 0b1111;

                            int nextByte = 0;
                            while (input < inputLimit && (nextByte = UNSAFE.getByte(inputBase, input++) & 0xFF) == 0) {
                                literalLength += 0b1111_1111;
                            }
                            literalLength += nextByte;
                        }
                        literalLength += 3;
                    }
                    else if (lastLiteralLength <= 3) {
                        // 0b0000_PPLL 0bPPPP_PPPP

                        // copy length: fixed
                        //   3
                        matchLength = 3;

                        // copy offset :: 12 bits :: valid range [2048..3071]
                        //   [0..1] from command [2..3]
                        //   [2..9] from trailer [0..7]
                        //   [10] unset
                        //   [11] set
                        if (input >= inputLimit) {
                            throw new MalformedInputException(input - inputAddress);
                        }
                        matchOffset = (command & 0b1100) >>> 2;
                        matchOffset |= (UNSAFE.getByte(inputBase, input++) & 0xFF) << 2;
                        matchOffset |= 0b1000_0000_0000;

                        // literal length :: 2 bits :: valid range [0..3]
                        //   [0..1] from command [0..1]
                        literalLength = (command & 0b0000_0011);
                    }
                    else {
                        // 0b0000_PPLL 0bPPPP_PPPP

                        // copy length :: fixed
                        //   2
                        matchLength = 2;

                        // copy offset :: 10 bits :: valid range [0..1023]
                        //   [0..1] from command [2..3]
                        //   [2..9] from trailer [0..7]
                        if (input >= inputLimit) {
                            throw new MalformedInputException(input - inputAddress);
                        }
                        matchOffset = (command & 0b1100) >>> 2;
                        matchOffset |= (UNSAFE.getByte(inputBase, input++) & 0xFF) << 2;

                        // literal length :: 2 bits :: valid range [0..3]
                        //   [0..1] from command [0..1]
                        literalLength = (command & 0b0000_0011);
                    }
                }
                else if (firstCommand) {
                    // first command has special handling when high nibble is set
                    matchLength = 0;
                    matchOffset = 0;
                    literalLength = command - 17;
                }
                else if ((command & 0b1111_0000) == 0b0001_0000) {
                    // 0b0001_?MMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL

                    // copy length - 2 :: variable bits :: valid range [3..]
                    //   2 + variableLength(command bits [0..2], 3)
                    matchLength = command & 0b111;
                    if (matchLength == 0) {
                        matchLength = 0b111;

                        int nextByte = 0;
                        while (input < inputLimit && (nextByte = UNSAFE.getByte(inputBase, input++) & 0xFF) == 0) {
                            matchLength += 0b1111_1111;
                        }
                        matchLength += nextByte;
                    }
                    matchLength += 2;

                    // read trailer
                    if (input + SIZE_OF_SHORT > inputLimit) {
                        throw new MalformedInputException(input - inputAddress);
                    }
                    int trailer = UNSAFE.getShort(inputBase, input) & 0xFFFF;
                    input += SIZE_OF_SHORT;

                    // copy offset :: 16 bits :: valid range [32767..49151]
                    //   [0..13] from trailer [2..15]
                    //   [14] if command bit [3] unset
                    //   [15] if command bit [3] set
                    matchOffset = trailer >> 2;
                    if ((command & 0b1000) == 0) {
                        matchOffset |= 0b0100_0000_0000_0000;
                    }
                    else {
                        matchOffset |= 0b1000_0000_0000_0000;
                    }
                    matchOffset--;

                    // literal length :: 2 bits :: valid range [0..3]
                    //   [0..1] from trailer [0..1]
                    literalLength = trailer & 0b11;
                }
                else if ((command & 0b1110_0000) == 0b0010_0000) {
                    // 0b001M_MMMM (0bMMMM_MMMM)* 0bPPPP_PPPP_PPPP_PPLL

                    // copy length - 2 :: variable bits :: valid range [3..]
                    //   2 + variableLength(command bits [0..4], 5)
                    matchLength = command & 0b1_1111;
                    if (matchLength == 0) {
                        matchLength = 0b1_1111;

                        int nextByte = 0;
                        while (input < inputLimit && (nextByte = UNSAFE.getByte(inputBase, input++) & 0xFF) == 0) {
                            matchLength += 0b1111_1111;
                        }
                        matchLength += nextByte;
                    }
                    matchLength += 2;

                    // read trailer
                    if (input + SIZE_OF_SHORT > inputLimit) {
                        throw new MalformedInputException(input - inputAddress);
                    }
                    int trailer = UNSAFE.getShort(inputBase, input) & 0xFFFF;
                    input += SIZE_OF_SHORT;

                    // copy offset :: 14 bits :: valid range [0..16383]
                    //  [0..13] from trailer [2..15]
                    matchOffset = trailer >>> 2;

                    // literal length :: 2 bits :: valid range [0..3]
                    //   [0..1] from trailer [0..1]
                    literalLength = trailer & 0b11;
                }
                else if ((command & 0b1100_0000) != 0) {
                    // 0bMMMP_PPLL 0bPPPP_PPPP

                    // copy length - 1 :: 3 bits :: valid range [1..8]
                    //   [0..2] from command [5..7]
                    //   add 1
                    matchLength = (command & 0b1110_0000) >>> 5;
                    matchLength += 1;

                    // copy offset :: 11 bits :: valid range [0..4095]
                    //   [0..2] from command [2..4]
                    //   [3..10] from trailer [0..7]
                    if (input >= inputLimit) {
                        throw new MalformedInputException(input - inputAddress);
                    }
                    matchOffset = (command & 0b0001_1100) >>> 2;
                    matchOffset |= (UNSAFE.getByte(inputBase, input++) & 0xFF) << 3;

                    // literal length :: 2 bits :: valid range [0..3]
                    //   [0..1] from command [0..1]
                    literalLength = (command & 0b0000_0011);
                }
                else {
                    String binary = toBinary(command);
                    throw new MalformedInputException(input - 1, "Invalid LZO command " + binary);
                }
                firstCommand = false;

                // copy match
                if (matchLength != 0) {
                    // lzo encodes match offset minus one
                    matchOffset++;

                    long matchAddress = output - matchOffset;
                    if (matchAddress < outputAddress || output + matchLength > outputLimit) {
                        throw new MalformedInputException(input - inputAddress);
                    }
                    long matchOutputLimit = output + matchLength;

                    if (output > fastOutputLimit) {
                        // slow match copy
                        while (output < matchOutputLimit) {
                            UNSAFE.putByte(outputBase, output++, UNSAFE.getByte(outputBase, matchAddress++));
                        }
                    }
                    else {
                        // copy repeated sequence
                        if (matchOffset < SIZE_OF_LONG) {
                            // 8 bytes apart so that we can copy long-at-a-time below
                            int increment32 = DEC_32_TABLE[matchOffset];
                            int decrement64 = DEC_64_TABLE[matchOffset];

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

                        if (matchOutputLimit >= fastOutputLimit) {
                            if (matchOutputLimit > outputLimit) {
                                throw new MalformedInputException(input - inputAddress);
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
                            while (output < matchOutputLimit) {
                                UNSAFE.putLong(outputBase, output, UNSAFE.getLong(outputBase, matchAddress));
                                matchAddress += SIZE_OF_LONG;
                                output += SIZE_OF_LONG;
                            }
                        }
                    }
                    output = matchOutputLimit; // correction in case we over-copied
                }

                // copy literal
                long literalOutputLimit = output + literalLength;
                if (literalOutputLimit > fastOutputLimit || input + literalLength > inputLimit - SIZE_OF_LONG) {
                    if (literalOutputLimit > outputLimit) {
                        throw new MalformedInputException(input - inputAddress);
                    }

                    // slow, precise copy
                    UNSAFE.copyMemory(inputBase, input, outputBase, output, literalLength);
                    input += literalLength;
                    output += literalLength;
                }
                else {
                    // fast copy. We may over-copy but there's enough room in input and output to not overrun them
                    do {
                        UNSAFE.putLong(outputBase, output, UNSAFE.getLong(inputBase, input));
                        input += SIZE_OF_LONG;
                        output += SIZE_OF_LONG;
                    }
                    while (output < literalOutputLimit);
                    input -= (output - literalOutputLimit); // adjust index if we over-copied
                    output = literalOutputLimit;
                }
                lastLiteralLength = literalLength;
            }

            if (input + SIZE_OF_SHORT > inputLimit && UNSAFE.getShort(inputBase, input) != 0) {
                throw new MalformedInputException(input - inputAddress);
            }
            input += SIZE_OF_SHORT;
        }

        return (int) (output - outputAddress);
    }

    private static String toBinary(int command)
    {
        String binaryString = String.format("%8s", toBinaryString(command)).replace(' ', '0');
        return "0b" + binaryString.substring(0, 4) + "_" + binaryString.substring(4);
    }
}
