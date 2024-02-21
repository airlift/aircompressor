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

import static io.airlift.compress.zlib.InputReader.mask;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Arrays.fill;

// This implementation is based on zlib by Jean-loup Gailly and Mark Adler
final class InflateTables
{
    // Length codes 257..285 base
    private static final short[] LENGTH_CODES_BASE = {
            3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31,
            35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258, 0, 0,
    };

    // Length codes 257..285 extra
    private static final short[] LENGTH_CODES_EXTRA = {
            16, 16, 16, 16, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18,
            19, 19, 19, 19, 20, 20, 20, 20, 21, 21, 21, 21, 16, 194, 65,
    };

    // Distance codes 0..29 base
    private static final short[] DISTANCE_CODES_BASE = {
            1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193,
            257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145,
            8193, 12289, 16385, 24577, 0, 0,
    };

    // Distance codes 0..29 extra
    private static final short[] DISTANCE_CODES_EXTRA = {
            16, 16, 16, 16, 17, 17, 18, 18, 19, 19, 20, 20, 21, 21, 22, 22,
            23, 23, 24, 24, 25, 25, 26, 26, 27, 27,
            28, 28, 29, 29, 64, 64,
    };

    public static final int MAX_BITS = 15;              // max bits in a code
    public static final int MAX_COUNTS = MAX_BITS + 1;  // max number of counts

    public static final int ENOUGH_LENGTHS = 852;       // max size of lengths dynamic table
    public static final int ENOUGH_DISTANCES = 592;     // max size of distances dynamic table

    public static final byte END_OF_BLOCK = 0b0010_0000;
    public static final byte INVALID_CODE = 0b0100_0000;

    public static final InflateTable FIXED_TABLE;

    static {
        int[] lengthFixed = new int[512];
        int[] distanceFixed = new int[32];

        short[] lengths = new short[288];
        fill(lengths, 0, 144, (short) 8);
        fill(lengths, 144, 256, (short) 9);
        fill(lengths, 256, 280, (short) 7);
        fill(lengths, 280, 288, (short) 8);
        buildCodeTable(CodeType.LENGTHS, lengths, 0, 288, 9, lengthFixed);

        lengths = new short[32];
        fill(lengths, (short) 5);
        buildCodeTable(CodeType.DISTANCES, lengths, 0, 32, 5, distanceFixed);

        FIXED_TABLE = new InflateTable(lengthFixed, 9, distanceFixed, 5);
    }

    private InflateTables() {}

    public static int buildCodeTable(CodeType type, short[] lengths, int offset, int codes, int bits, int[] table)
    {
        short[] counts = new short[MAX_COUNTS];
        short[] offsets = new short[MAX_COUNTS];

        // accumulate lengths for codes -- assumes lengths all in [0, MAX_BITS]
        for (int i = 0; i < codes; i++) {
            counts[lengths[offset + i]]++;
        }

        // bound code lengths, force root to be within code lengths
        int root = bits;
        int max;
        for (max = MAX_BITS; max >= 1; max--) {
            if (counts[max] != 0) {
                break;
            }
        }
        root = min(root, max);

        if (max == 0) {
            // no symbols to code at all
            // make a table to force an error
            table[0] = tableEntry(INVALID_CODE, (byte) 1, (short) 0);
            table[1] = table[0];

            // no symbols, but wait for decoding to report error
            return 1;
        }

        int min;
        for (min = 1; min < max; min++) {
            if (counts[min] != 0) {
                break;
            }
        }
        root = max(root, min);

        // check for an over-subscribed or incomplete set of lengths
        int left = 1;
        for (int length = 1; length < MAX_COUNTS; length++) {
            left <<= 1;
            left -= counts[length];
            if (left < 0) {
                throw new IllegalArgumentException("over-subscribed lengths");
            }
        }
        if ((left > 0) && ((type == CodeType.CODES) || (max != 1))) {
            throw new IllegalArgumentException("incomplete set of lengths");
        }

        // generate offsets into symbol table for each length for sorting
        offsets[1] = 0;
        for (short length = 1; length < MAX_BITS; length++) {
            offsets[length + 1] = (short) (offsets[length] + counts[length]);
        }

        // sort symbols by length, by symbol order within each length
        short[] symbols = new short[codes];
        for (short symbol = 0; symbol < codes; symbol++) {
            short length = lengths[offset + symbol];
            if (length != 0) {
                symbols[offsets[length]] = symbol;
                offsets[length]++;
            }
        }

        // set up for code type
        short[] base;
        short[] extra;
        int match;
        switch (type) {
            case CODES:
                base = new short[0];
                extra = new short[0];
                match = 20;
                break;
            case LENGTHS:
                base = LENGTH_CODES_BASE;
                extra = LENGTH_CODES_EXTRA;
                match = 257;
                break;
            case DISTANCES:
                base = DISTANCE_CODES_BASE;
                extra = DISTANCE_CODES_EXTRA;
                match = 0;
                break;
            default:
                throw new AssertionError();
        }

        // initialize state for loop
        int huffman = 0;            // starting code
        int symbol = 0;             // starting code symbol
        int length = min;           // starting code length
        int next = 0;               // current table to fill in
        int current = root;         // current table index bits
        int drop = 0;               // current bits to drop from code for index
        int low = -1;               // trigger new sub-table when length > root
        int used = 1 << root;       // used root table entries
        int mask = used - 1;        // mask for comparing low

        // check available table space
        checkAvailableSpace(type, used);

        // process all codes and make table entries
        while (true) {
            // create table entry
            byte hereBits = (byte) (length - drop);
            byte hereOp;
            short hereValue;
            short value = symbols[symbol];
            if ((value + 1) < match) {
                hereOp = 0;
                hereValue = value;
            }
            else if (value >= match) {
                hereOp = (byte) extra[value - match];
                hereValue = base[value - match];
            }
            else {
                hereOp = END_OF_BLOCK | INVALID_CODE;
                hereValue = 0;
            }

            // replicate for those indices with low length bits equal to huffman
            int increment = 1 << (length - drop);
            int fill = 1 << current;
            min = fill;                             // save offset to next table
            do {
                fill -= increment;
                int index = next + (huffman >> drop) + fill;
                table[index] = tableEntry(hereOp, hereBits, hereValue);
            }
            while (fill != 0);

            // backwards increment the length-bit code huffman
            increment = 1 << (length - 1);
            while ((huffman & increment) != 0) {
                increment >>= 1;
            }
            if (increment == 0) {
                huffman = 0;
            }
            else {
                huffman &= increment - 1;
                huffman += increment;
            }

            // go to next symbol, update count, length
            symbol++;
            counts[length]--;
            if (counts[length] == 0) {
                if (length == max) {
                    break;
                }
                length = lengths[offset + symbols[symbol]];
            }

            // create new sub-table if needed
            if ((length > root) && ((huffman & mask) != low)) {
                // if first time, transition to sub-tables
                if (drop == 0) {
                    drop = root;
                }

                // increment past last table (min is 1 << current)
                next += min;

                // determine length of next table
                current = length - drop;
                left = 1 << current;
                while ((current + drop) < max) {
                    left -= counts[current + drop];
                    if (left <= 0) {
                        break;
                    }
                    current++;
                    left <<= 1;
                }

                // check for enough space
                used += 1 << current;
                checkAvailableSpace(type, used);

                // point entry in root table to sub-table
                low = huffman & mask;
                table[low] = tableEntry((byte) current, (byte) root, (short) next);
            }
        }

        // fill in remaining table entry if code is incomplete (guaranteed to have
        // at most one remaining entry, since if the code is incomplete, the
        // maximum code length that was allowed to get this far is one bit)
        if (huffman != 0) {
            table[next + huffman] = tableEntry(INVALID_CODE, (byte) (length - drop), (short) 0);
        }

        return root;
    }

    private static void checkAvailableSpace(CodeType type, int used)
    {
        if ((type == CodeType.LENGTHS) && (used > ENOUGH_LENGTHS)) {
            throw new IllegalArgumentException("too many lengths");
        }
        if ((type == CodeType.DISTANCES) && (used > ENOUGH_DISTANCES)) {
            throw new IllegalArgumentException("too many distances");
        }
    }

    public static byte extractOp(int packed)
    {
        return (byte) ((packed >> 24) & 0xFF);
    }

    public static byte extractBits(int packed)
    {
        return (byte) ((packed >> 16) & 0xFF);
    }

    public static short extractValue(int packed)
    {
        return (short) (packed & 0xFFFF);
    }

    private static int tableEntry(byte op, byte bits, short value)
    {
        return ((op & 0xFF) << 24) | ((bits & 0xFF) << 16) | (value & 0xFFFF);
    }

    @SuppressWarnings("PublicField")
    public static class InflateTable
    {
        public final int[] lengthCode;
        public final int lengthBits;
        public final int lengthMask;
        public final int[] distanceCode;
        public final int distanceBits;
        public final int distanceMask;

        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        public InflateTable(int[] lengthCode, int lengthBits, int[] distanceCode, int distanceBits)
        {
            this.lengthCode = lengthCode;
            this.lengthBits = lengthBits;
            this.lengthMask = mask(lengthBits);
            this.distanceCode = distanceCode;
            this.distanceBits = distanceBits;
            this.distanceMask = mask(distanceBits);
        }
    }

    public enum CodeType
    {
        CODES, LENGTHS, DISTANCES
    }
}
