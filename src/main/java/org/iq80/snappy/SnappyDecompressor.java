package org.iq80.snappy;

public final class SnappyDecompressor
{
    private static final int MAX_INCREMENT_COPY_OVERFLOW = 10;

    public static int getUncompressedLength(byte[] compressed, int compressedOffset)
    {
        return readUncompressedLength(compressed, compressedOffset)[0];
    }

    public static byte[] uncompress(byte[] compressed, int compressedOffset, int compressedSize)
    {
        // Read the uncompressed length from the front of the compressed input
        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        int expectedLength = varInt[0];
        compressedOffset += varInt[1];
        compressedSize -= varInt[1];

        // allocate the uncompressed buffer
        byte[] uncompressed = new byte[expectedLength];

        // Process the entire input
        int uncompressedSize = decompressAllTags(
                compressed,
                compressedOffset,
                compressedSize,
                uncompressed,
                0);

        SnappyInternalUtils.checkArgument(expectedLength == uncompressedSize,
                "Corrupt Input: recorded length is %s bytes but actual length after decompression is %s bytes ",
                expectedLength,
                uncompressedSize);

        return uncompressed;
    }

    public static int uncompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
    {
        // Read the uncompressed length from the front of the compressed input
        int[] varInt = readUncompressedLength(compressed, compressedOffset);
        int expectedLength = varInt[0];
        compressedOffset += varInt[1];
        compressedSize -= varInt[1];

        SnappyInternalUtils.checkArgument(expectedLength <= uncompressed.length - uncompressedOffset,
                "Uncompressed length %s must be less than %s", expectedLength, uncompressed.length - uncompressedOffset);

        // Process the entire input
        int uncompressedSize = decompressAllTags(
                compressed,
                compressedOffset,
                compressedSize,
                uncompressed,
                uncompressedOffset);

        SnappyInternalUtils.checkArgument(expectedLength == uncompressedSize,
                "Corrupt Input: recorded length is %s bytes but actual length after decompression is %s bytes ",
                expectedLength,
                uncompressedSize);

        return expectedLength;
    }

    private static int decompressAllTags(
            final byte[] input,
            final int inputOffset,
            final int inputSize,
            final byte[] output,
            final int outputOffset)
    {
        final int ipLimit = inputOffset + inputSize;

        int opIndex = outputOffset;
        for (int ipIndex = inputOffset; ipIndex < ipLimit; ) {
            // read the opcode
            int opCode = input[ipIndex++] & 0xFF;
            // use the quick lookup table to determine the decode the opcode
            int entry = opLookupTable[opCode] & 0xFFFF;
            int trailerBytes = entry >>> 11;
            int trailer = readTrailer(input, ipIndex, trailerBytes);
            int length = entry & 0xff;

            // advance the ipIndex past the op code bytes
            ipIndex += trailerBytes;

            if ((opCode & 0x3) == Snappy.LITERAL) {
                // trailer of a literal contains more of the length
                int literalLength = length + trailer;

                copyLiteral(input, ipIndex, output, opIndex, literalLength);
                ipIndex += literalLength;
                opIndex += literalLength;
            }
            else {
                // copyOffset/256 is encoded in bits 8..10.  By just fetching
                // those bits, we get copyOffset (since the bit-field starts at
                // bit 8), and the trailer of a literal contains additional bits
                // for the copy offset length
                copyFromSelf(output, outputOffset, (entry & 0x700) + trailer, opIndex, length);
                opIndex += length;
            }
        }

        return opIndex - outputOffset;
    }

    private static int readTrailer(byte[] data, int index, int bytes)
    {
        if (data.length > index + 4) {
            return SnappyInternalUtils.loadInt(data, index) & wordmask[bytes];
        }
        int value = 0;
        switch (bytes) {
            case 4:
                value = (data[index + 3] & 0xff) << 24;
            case 3:
                value |= (data[index + 2] & 0xff) << 16;
            case 2:
                value |= (data[index + 1] & 0xff) << 8;
            case 1:
                value |= (data[index] & 0xff);
        }
        return value;
    }

    private static void copyLiteral(byte[] input, int ipIndex, byte[] output, int opIndex, int literalLength)
    {
        if (literalLength < 0 || ipIndex + literalLength > input.length || opIndex + literalLength > output.length) {
            throw new IndexOutOfBoundsException();
        }

        int spaceLeft = output.length - opIndex;
        int readableBytes = input.length - ipIndex;

        // most literals are less than 16 bytes to handle them specially
        if (literalLength <= 16 && spaceLeft >= 16 && readableBytes >= 16) {
            SnappyInternalUtils.copyLong(input, ipIndex, output, opIndex);
            SnappyInternalUtils.copyLong(input, ipIndex + 8, output, opIndex + 8);
        }
        else
        {
            if (literalLength <= 32) {
                // copy long-by-long
                int fastLength = literalLength & 0xFFFFFFF8;
                for (int i = 0; i < literalLength; i += 8) {
                    SnappyInternalUtils.copyLong(input, ipIndex + i, output, opIndex + i);
                }

                // copy byte-by-byte
                int slowLength = literalLength & 0x7;
                for (int i = 0; i < slowLength; i += 1) {
                    output[opIndex + fastLength + i] = input[ipIndex + fastLength + i];
                }
            } else {
                SnappyInternalUtils.copyMemory(input, ipIndex, output, opIndex, literalLength);
            }
        }
    }

    // Equivalent to IncrementalCopy except that it can write up to ten extra
    // bytes after the end of the copy, and that it is faster.
    //
    // The main part of this loop is a simple copy of eight bytes at a time until
    // we've copied (at least) the requested amount of bytes.  However, if op and
    // src are less than eight bytes apart (indicating a repeating pattern of
    // length < 8), we first need to expand the pattern in order to get the correct
    // results. For instance, if the buffer looks like this, with the eight-byte
    // <src> and <op> patterns marked as intervals:
    //
    //    abxxxxxxxxxxxx
    //    [------]           src
    //      [------]         op
    //
    // a single eight-byte copy from <src> to <op> will repeat the pattern once,
    // after which we can move <op> two bytes without moving <src>:
    //
    //    ababxxxxxxxxxx
    //    [------]           src
    //        [------]       op
    //
    // and repeat the exercise until the two no longer overlap.
    //
    // This allows us to do very well in the special case of one single byte
    // repeated many times, without taking a big hit for more general cases.
    //
    // The worst case of extra writing past the end of the match occurs when
    // op - src == 1 and len == 1; the last copy will read from byte positions
    // [0..7] and write to [4..11], whereas it was only supposed to write to
    // position 1. Thus, ten excess bytes.
    private static void copyFromSelf(byte[] output, int outputBase, int copyOffset, int opIndex, int length)
    {
        int spaceLeft = output.length - opIndex;
        int srcIndex = opIndex - copyOffset;    // opIndex = srcIndex + copyOffset

        if (length < 0 || srcIndex < outputBase || srcIndex >= opIndex || spaceLeft < length) {
            throw new IndexOutOfBoundsException();
        }

        if (length <= 16 && copyOffset >= 8 && spaceLeft >= 16) {
            // Fast path, used for the majority (70-80%) of dynamic invocations.
            SnappyInternalUtils.copyLong(output, srcIndex, output, opIndex);
            SnappyInternalUtils.copyLong(output, srcIndex + 8, output, opIndex + 8);
        }
        else if (spaceLeft >= length + MAX_INCREMENT_COPY_OVERFLOW) {
            incrementalCopyFastPath(output, srcIndex, copyOffset, srcIndex + length + copyOffset);
        }
        else {
            incrementalCopy(output, srcIndex, output, opIndex, length);
        }
    }

    private static void incrementalCopyFastPath(byte[] output, int srcIndex, int copyOffset, int limit)
    {
        int available = copyOffset;
        for (; available < 8; available <<= 1) {
            SnappyInternalUtils.copyLong(output, srcIndex, output, srcIndex + available);
        }

        limit -= available;
        for (int i = srcIndex; i < limit; i += 8) {
            SnappyInternalUtils.copyLong(output, i, output, i + available);
        }
    }

    // Copy "len" bytes from "src" to "op", one byte at a time.  Used for
    // handling COPY operations where the input and output regions may
    // overlap.  For example, suppose:
    //    src    == "ab"
    //    op     == src + 2
    //    len    == 20
    // After IncrementalCopy(src, op, len), the result will have
    // eleven copies of "ab"
    //    ababababababababababab
    // Note that this does not match the semantics of either memcpy()
    // or memmove().
    private static void incrementalCopy(byte[] src, int srcIndex, byte[] op, int opIndex, int length)
    {
        do {
            op[opIndex++] = src[srcIndex++];
        } while (--length > 0);
    }


    // Mapping from i in range [0,4] to a mask to extract the bottom 8*i bits
    private static final int[] wordmask = new int[]{
            0, 0xff, 0xffff, 0xffffff, 0xffffffff
    };

    // Data stored per entry in lookup table:
    //      Range   Bits-used       Description
    //      ------------------------------------
    //      1..64   0..7            Literal/copy length encoded in opcode byte
    //      0..7    8..10           Copy offset encoded in opcode byte / 256
    //      0..4    11..13          Extra bytes after opcode
    //
    // We use eight bits for the length even though 7 would have sufficed
    // because of efficiency reasons:
    //      (1) Extracting a byte is faster than a bit-field
    //      (2) It properly aligns copy offset so we do not need a <<8
    private static final short[] opLookupTable = new short[]{
            0x0001, 0x0804, 0x1001, 0x2001, 0x0002, 0x0805, 0x1002, 0x2002,
            0x0003, 0x0806, 0x1003, 0x2003, 0x0004, 0x0807, 0x1004, 0x2004,
            0x0005, 0x0808, 0x1005, 0x2005, 0x0006, 0x0809, 0x1006, 0x2006,
            0x0007, 0x080a, 0x1007, 0x2007, 0x0008, 0x080b, 0x1008, 0x2008,
            0x0009, 0x0904, 0x1009, 0x2009, 0x000a, 0x0905, 0x100a, 0x200a,
            0x000b, 0x0906, 0x100b, 0x200b, 0x000c, 0x0907, 0x100c, 0x200c,
            0x000d, 0x0908, 0x100d, 0x200d, 0x000e, 0x0909, 0x100e, 0x200e,
            0x000f, 0x090a, 0x100f, 0x200f, 0x0010, 0x090b, 0x1010, 0x2010,
            0x0011, 0x0a04, 0x1011, 0x2011, 0x0012, 0x0a05, 0x1012, 0x2012,
            0x0013, 0x0a06, 0x1013, 0x2013, 0x0014, 0x0a07, 0x1014, 0x2014,
            0x0015, 0x0a08, 0x1015, 0x2015, 0x0016, 0x0a09, 0x1016, 0x2016,
            0x0017, 0x0a0a, 0x1017, 0x2017, 0x0018, 0x0a0b, 0x1018, 0x2018,
            0x0019, 0x0b04, 0x1019, 0x2019, 0x001a, 0x0b05, 0x101a, 0x201a,
            0x001b, 0x0b06, 0x101b, 0x201b, 0x001c, 0x0b07, 0x101c, 0x201c,
            0x001d, 0x0b08, 0x101d, 0x201d, 0x001e, 0x0b09, 0x101e, 0x201e,
            0x001f, 0x0b0a, 0x101f, 0x201f, 0x0020, 0x0b0b, 0x1020, 0x2020,
            0x0021, 0x0c04, 0x1021, 0x2021, 0x0022, 0x0c05, 0x1022, 0x2022,
            0x0023, 0x0c06, 0x1023, 0x2023, 0x0024, 0x0c07, 0x1024, 0x2024,
            0x0025, 0x0c08, 0x1025, 0x2025, 0x0026, 0x0c09, 0x1026, 0x2026,
            0x0027, 0x0c0a, 0x1027, 0x2027, 0x0028, 0x0c0b, 0x1028, 0x2028,
            0x0029, 0x0d04, 0x1029, 0x2029, 0x002a, 0x0d05, 0x102a, 0x202a,
            0x002b, 0x0d06, 0x102b, 0x202b, 0x002c, 0x0d07, 0x102c, 0x202c,
            0x002d, 0x0d08, 0x102d, 0x202d, 0x002e, 0x0d09, 0x102e, 0x202e,
            0x002f, 0x0d0a, 0x102f, 0x202f, 0x0030, 0x0d0b, 0x1030, 0x2030,
            0x0031, 0x0e04, 0x1031, 0x2031, 0x0032, 0x0e05, 0x1032, 0x2032,
            0x0033, 0x0e06, 0x1033, 0x2033, 0x0034, 0x0e07, 0x1034, 0x2034,
            0x0035, 0x0e08, 0x1035, 0x2035, 0x0036, 0x0e09, 0x1036, 0x2036,
            0x0037, 0x0e0a, 0x1037, 0x2037, 0x0038, 0x0e0b, 0x1038, 0x2038,
            0x0039, 0x0f04, 0x1039, 0x2039, 0x003a, 0x0f05, 0x103a, 0x203a,
            0x003b, 0x0f06, 0x103b, 0x203b, 0x003c, 0x0f07, 0x103c, 0x203c,
            0x0801, 0x0f08, 0x103d, 0x203d, 0x1001, 0x0f09, 0x103e, 0x203e,
            0x1801, 0x0f0a, 0x103f, 0x203f, 0x2001, 0x0f0b, 0x1040, 0x2040
    };

    /**
     * Reads the variable length integer encoded a the specified offset, and
     * returns this length with the number of bytes read.
     */
    private static int[] readUncompressedLength(byte[] compressed, int compressedOffset)
    {
        int result = 0;
        int bytesRead = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = compressed[compressedOffset + bytesRead++] & 0xFF;

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return new int[]{result, bytesRead};
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }

}
