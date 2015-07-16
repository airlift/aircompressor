package io.airlift.compress.lz4;

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

class HadoopLz4InputStream
        extends CompressionInputStream
{
    private final Lz4Decompressor decompressor = new Lz4Decompressor();
    private final InputStream in;

    private byte[] uncompressed = new byte[0];
    private int uncompressedOffset;
    private int uncompressedLength;

    private byte[] compressed = new byte[0];

    public HadoopLz4InputStream(InputStream in)
    {
        super(in);
        this.in = in;
    }

    @Override
    public int read()
            throws IOException
    {
        if (uncompressedOffset >= uncompressedLength) {
            readNextChunk();
            if (uncompressedLength == 0) {
                return -1;
            }
        }
        return uncompressed[uncompressedOffset--] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        if (uncompressedOffset >= uncompressedLength) {
            readNextChunk();
            if (uncompressedLength == 0) {
                return -1;
            }
        }
        int size = Math.min(length, uncompressedLength - uncompressedOffset);
        System.arraycopy(uncompressed, uncompressedOffset, output, offset, size);
        uncompressedOffset += size;
        return size;
    }

    @Override
    public void resetState()
            throws IOException
    {
        throw new UnsupportedOperationException("resetState not supported for LZ4");
    }

    private void readNextChunk()
            throws IOException
    {
        uncompressedOffset = 0;
        do {
            uncompressedLength = readBigEndianInt();
            if (uncompressedLength == -1) {
                uncompressedLength = 0;
                return;
            }
        } while (uncompressedLength == 0);

        int compressedLength = readBigEndianInt();
        if (compressedLength == -1) {
            uncompressedLength = 0;
            return;
        }

        if (compressed.length < compressedLength) {
            compressed = new byte[compressedLength];
        }
        readInput(compressedLength, compressed);

        if (uncompressed.length < uncompressedLength) {
            uncompressed = new byte[uncompressedLength];
        }

        int bytes = decompressor.decompress(compressed, 0, compressedLength, uncompressed, 0, uncompressed.length);
        if (uncompressedLength != bytes) {
            throw new IOException("Expected to read " + uncompressedLength + " bytes, but data only contained " + bytes + " bytes");
        }
    }

    private void readInput(int length, byte[] buffer)
            throws IOException
    {
        int offset = 0;
        while (offset < length) {
            int size = in.read(buffer, offset, length - offset);
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            offset += size;
        }
    }

    private int readBigEndianInt()
            throws IOException
    {
        int b1 = in.read();
        if (b1 < 0) {
            return -1;
        }
        int b2 = in.read();
        int b3 = in.read();
        int b4 = in.read();

        // If any of the other bits are negative, the stream it truncated
        if ((b2 | b3 | b4) < 0) {
            throw new IOException("Stream is truncated");
        }
        return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4));
    }
}
