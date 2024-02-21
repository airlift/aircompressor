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
import io.airlift.compress.zlib.InflateTables.CodeType;
import io.airlift.compress.zlib.InflateTables.InflateTable;

import static io.airlift.compress.zlib.InflateTables.END_OF_BLOCK;
import static io.airlift.compress.zlib.InflateTables.ENOUGH_DISTANCES;
import static io.airlift.compress.zlib.InflateTables.ENOUGH_LENGTHS;
import static io.airlift.compress.zlib.InflateTables.INVALID_CODE;
import static io.airlift.compress.zlib.InflateTables.buildCodeTable;
import static io.airlift.compress.zlib.InflateTables.extractBits;
import static io.airlift.compress.zlib.InflateTables.extractOp;
import static io.airlift.compress.zlib.InflateTables.extractValue;
import static java.lang.Math.toIntExact;

// This implementation is based on zlib by Jean-loup Gailly and Mark Adler
public final class InflateRawDecompressor
{
    private static final int NON_COMPRESSED = 0;
    private static final int FIXED_HUFFMAN = 1;
    private static final int DYNAMIC_HUFFMAN = 2;

    private static final int MAX_LENGTH_CODES = 286;    // max number of literal/length codes
    private static final int MAX_DISTANCE_CODES = 30;   // max number of distance codes

    private InflateRawDecompressor() {}

    public static int decompress(Object inputBase, long inputAddress, long inputLimit, Object outputBase, long outputAddress, long outputLimit)
            throws MalformedInputException
    {
        InputReader reader = new InputReader(inputBase, inputAddress, inputLimit);
        OutputWriter writer = new OutputWriter(outputBase, outputAddress, outputLimit);

        boolean last;
        do {
            last = reader.bits(1) == 1;
            int type = reader.bits(2);

            switch (type) {
                case NON_COMPRESSED:
                    nonCompressed(reader, writer);
                    break;
                case FIXED_HUFFMAN:
                    fixedHuffman(reader, writer);
                    break;
                case DYNAMIC_HUFFMAN:
                    dynamicHuffman(reader, writer);
                    break;
                default:
                    throw new MalformedInputException(reader.offset(), "Invalid block type: " + type);
            }
        }
        while (!last);

        if (reader.available() > 0) {
            throw new MalformedInputException(reader.offset(), "Output buffer too small");
        }

        return toIntExact(writer.offset());
    }

    private static void nonCompressed(InputReader reader, OutputWriter writer)
    {
        reader.clear();

        int lsb = reader.readByte();
        int msb = reader.readByte();

        int checkLsb = reader.readByte();
        int checkMsb = reader.readByte();

        if ((lsb != (~checkLsb & 0xFF)) || (msb != (~checkMsb & 0xFF))) {
            throw new MalformedInputException(reader.offset(), "Block length does not match complement");
        }

        int length = (msb << 8) | lsb;

        writer.copyInput(reader, length);
    }

    private static void fixedHuffman(InputReader reader, OutputWriter writer)
    {
        inflate(InflateTables.FIXED_TABLE, reader, writer);
    }

    private static final short[] CODE_LENGTHS_ORDER = {
            16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
    };

    private static void dynamicHuffman(InputReader reader, OutputWriter writer)
    {
        int lengthSize = reader.bits(5) + 257;
        int distanceSize = reader.bits(5) + 1;
        int codeSize = reader.bits(4) + 4;
        if (lengthSize > MAX_LENGTH_CODES) {
            throw new MalformedInputException(reader.offset(), "Length count is too large: " + lengthSize);
        }
        if (distanceSize > MAX_DISTANCE_CODES) {
            throw new MalformedInputException(reader.offset(), "Distance count is too large: " + distanceSize);
        }

        short[] codeLengths = new short[19];
        for (int i = 0; i < codeSize; i++) {
            codeLengths[CODE_LENGTHS_ORDER[i]] = (short) reader.bits(3);
        }

        int[] codeCode = new int[388];
        int codeBits = buildCodeTable(CodeType.CODES, codeLengths, 0, 19, 7, codeCode);

        short[] lengths = new short[MAX_LENGTH_CODES + MAX_DISTANCE_CODES];

        int index = 0;
        while (index < (lengthSize + distanceSize)) {
            int code = codeCode[reader.peek(codeBits)];
            reader.skip(extractBits(code));
            short value = extractValue(code);

            if (value < 16) {
                lengths[index] = value;
                index++;
                continue;
            }

            short length = 0;
            int copy;
            if (value == 16) {
                if (index == 0) {
                    throw new MalformedInputException(reader.offset(), "No previous length for repeat");
                }
                length = lengths[index - 1];
                copy = reader.bits(2) + 3;
            }
            else if (value == 17) {
                copy = reader.bits(3) + 3;
            }
            else {
                copy = reader.bits(7) + 11;
            }

            if ((index + copy) > (lengthSize + distanceSize)) {
                throw new MalformedInputException(reader.offset(), "Too many lengths for repeat");
            }

            while (copy > 0) {
                lengths[index] = length;
                index++;
                copy--;
            }
        }

        if (lengths[256] == 0) {
            throw new MalformedInputException(reader.offset(), "Missing end-of-block code");
        }

        int[] lengthCode = new int[ENOUGH_LENGTHS];
        int lengthBits = buildCodeTable(CodeType.LENGTHS, lengths, 0, lengthSize, 9, lengthCode);

        int[] distanceCode = new int[ENOUGH_DISTANCES];
        int distanceBits = buildCodeTable(CodeType.DISTANCES, lengths, lengthSize, distanceSize, 6, distanceCode);

        InflateTable table = new InflateTable(lengthCode, lengthBits, distanceCode, distanceBits);

        inflate(table, reader, writer);
    }

    private static void inflate(InflateTable table, InputReader reader, OutputWriter writer)
    {
        int tableLengthBits = table.lengthBits;
        int tableLengthMask = table.lengthMask;
        int[] lengths = table.lengthCode;

        int tableDistanceBits = table.distanceBits;
        int tableDistanceMask = table.distanceMask;
        int[] distances = table.distanceCode;

        // decode literals and length/distances until end-of-block
        while (true) {
            int lengthIndex = reader.peek(tableLengthBits, tableLengthMask);

            while (true) {
                int packedLength = lengths[lengthIndex];
                reader.skip(extractBits(packedLength));
                int lengthOp = extractOp(packedLength);
                int length = extractValue(packedLength);

                if (lengthOp == 0) {
                    // literal
                    writer.writeByte(reader, (byte) length);
                    break;
                }

                if ((lengthOp & 0b0001_0000) != 0) {
                    // length base
                    int lengthBits = lengthOp & 0b1111;
                    if (lengthBits > 0) {
                        length += reader.bits(lengthBits);
                    }

                    int distanceIndex = reader.peek(tableDistanceBits, tableDistanceMask);
                    while (true) {
                        int packedDistance = distances[distanceIndex];
                        reader.skip(extractBits(packedDistance));
                        int distanceOp = extractOp(packedDistance);
                        int distance = extractValue(packedDistance);

                        if ((distanceOp & 0b0001_0000) != 0) {
                            // distance base
                            int distanceBits = distanceOp & 0b1111;
                            if (distanceBits > 0) {
                                distance += reader.bits(distanceBits);
                            }
                            writer.copyOutput(reader, distance, length);
                            break;
                        }

                        if ((distanceOp & INVALID_CODE) == 0) {
                            // second level distance code
                            distanceIndex = distance + reader.peek(distanceOp);
                            continue;
                        }

                        throw new MalformedInputException(reader.offset(), "Invalid distance code");
                    }
                    break;
                }

                if ((lengthOp & INVALID_CODE) == 0) {
                    // second level length code
                    lengthIndex = length + reader.peek(lengthOp);
                    continue;
                }

                if ((lengthOp & END_OF_BLOCK) != 0) {
                    // end-of-block
                    return;
                }

                throw new MalformedInputException(reader.offset(), "Invalid length/literal code");
            }
        }
    }
}
