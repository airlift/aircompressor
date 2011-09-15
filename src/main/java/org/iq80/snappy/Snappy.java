package org.iq80.snappy;

public final class Snappy
{
    private Snappy()
    {
    }

    public static int getUncompressedLength(byte[] compressed, int compressedOffset)
    {
        return SnappyDecompressor.getUncompressedLength(compressed, compressedOffset);
    }

    public static byte[] uncompress(byte[] compressed, int compressedOffset, int compressedSize)
    {
        return SnappyDecompressor.uncompress(compressed, compressedOffset, compressedSize);
    }

    public static int uncompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
    {
        return SnappyDecompressor.uncompress(compressed, compressedOffset, compressedSize, uncompressed, uncompressedOffset);
    }

    public static int maxCompressedLength(int sourceLength)
    {
        return SnappyCompressor.maxCompressedLength(sourceLength);
    }

    public static int compress(
            byte[] uncompressed,
            int uncompressedOffset,
            int uncompressedLength,
            byte[] compressed,
            int compressedOffset)
    {
        return SnappyCompressor.compress(uncompressed,
                uncompressedOffset,
                uncompressedLength,
                compressed,
                compressedOffset);
    }

    static final int LITERAL = 0;
    static final int COPY_1_BYTE_OFFSET = 1;  // 3 bit length + 3 bits of offset in opcode
    static final int COPY_2_BYTE_OFFSET = 2;
    static final int COPY_4_BYTE_OFFSET = 3;
}
