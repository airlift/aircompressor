package io.airlift.compress.snappy;

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static io.airlift.compress.snappy.SnappyConstants.SIZE_OF_LONG;

class HadoopSnappyInputStream
        extends CompressionInputStream
{
    private final SnappyDecompressor decompressor = new SnappyDecompressor();
    private final InputStream in;

    private int uncompressedBlockLength;
    private byte[] uncompressedChunk = new byte[0];
    private int uncompressedChunkOffset;
    private int uncompressedChunkLength;

    private byte[] compressed = new byte[0];

    public HadoopSnappyInputStream(InputStream in)
            throws IOException
    {
        super(in);
        this.in = in;
    }

    @Override
    public int read()
            throws IOException
    {
        if (uncompressedChunkOffset >= uncompressedChunkLength) {
            readNextChunk();
            if (uncompressedChunkLength == 0) {
                return -1;
            }
        }
        return uncompressedChunk[uncompressedChunkOffset--] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        if (uncompressedChunkOffset >= uncompressedChunkLength) {
            readNextChunk();
            if (uncompressedChunkLength == 0) {
                return -1;
            }
        }
        int size = Math.min(length, uncompressedChunkLength - uncompressedChunkOffset);
        System.arraycopy(uncompressedChunk, uncompressedChunkOffset, output, offset, size);
        uncompressedChunkOffset += size;
        return size;
    }

    @Override
    public void resetState()
            throws IOException
    {
        throw new UnsupportedOperationException("resetState not supported for Snappy");
    }

    private void readNextChunk()
            throws IOException
    {
        uncompressedBlockLength -= uncompressedChunkOffset;
        uncompressedChunkOffset = 0;
        uncompressedChunkLength = 0;
        while (uncompressedBlockLength == 0) {
            uncompressedBlockLength = readBigEndianInt();
            if (uncompressedBlockLength == -1) {
                uncompressedBlockLength = 0;
                return;
            }
        }

        int compressedChunkLength = readBigEndianInt();
        if (compressedChunkLength == -1) {
            return;
        }

        if (compressed.length < compressedChunkLength) {
             // over allocate buffer which makes decompression easier
            compressed = new byte[compressedChunkLength + SIZE_OF_LONG];
        }
        readInput(compressedChunkLength, compressed);

        uncompressedChunkLength = SnappyDecompressor.getUncompressedLength(compressed, 0);
        if (uncompressedChunkLength > uncompressedBlockLength) {
            throw new IOException("Chunk uncompressed size is greater than block size");
        }
        if (uncompressedChunk.length < uncompressedChunkLength) {
            // over allocate buffer which makes decompression easier
            uncompressedChunk = new byte[uncompressedChunkLength + SIZE_OF_LONG];
        }

        int bytes = decompressor.decompress(compressed, 0, compressedChunkLength, uncompressedChunk, 0, uncompressedChunkLength);
        if (uncompressedChunkLength != bytes) {
            throw new IOException("Expected to read " + uncompressedChunkLength + " bytes, but data only contained " + bytes + " bytes");
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
