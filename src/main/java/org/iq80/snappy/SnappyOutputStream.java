package org.iq80.snappy;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class implements an output stream for writing Snappy compressed data.
 * The output format is one or more compressed blocks of data, each of which
 * is preceded by a two byte header.
 * <p/>
 * The first bit of the header is a flag indicating if the block is compressed
 * or not. The remaining bits are the size of the block, minus one, as a big
 * endian number. Subtracting one from the size allows us to store sizes from
 * 1 to 32768 rather than 0 to 32767. This is possible because we never write
 * a block size of zero.
 * <p/>
 * An uncompressed block is simply copied from the input, thus guaranteeing
 * that the output is never larger than the input (not including the header).
 */
public class SnappyOutputStream
        extends OutputStream
{
    // the header format requires the max block size to fit in 15 bits -- do not change!
    private final byte[] buffer = new byte[32768];
    private final byte[] outputBuffer = new byte[Snappy.maxCompressedLength(buffer.length)];
    private final OutputStream out;

    private int position = 0;

    /**
     * Creates a Snappy output stream to write data to the specified underlying output stream.
     *
     * @param out the underlying output stream
     */
    public SnappyOutputStream(OutputStream out)
    {
        this.out = out;
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (position >= buffer.length) {
            flushBuffer();
        }
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] input, int offset, int length)
            throws IOException
    {
        int free = buffer.length - position;

        // easy case: enough free space in buffer for entire input
        if (free >= length) {
            copyToBuffer(input, offset, length);
            return;
        }

        // fill partial buffer as much as possible and flush
        if (position > 0) {
            copyToBuffer(input, offset, free);
            flushBuffer();
            offset += free;
            length -= free;
        }

        // write remaining full blocks directly from input array
        while (length >= buffer.length) {
            writeCompressed(input, offset, buffer.length);
            offset += buffer.length;
            length -= buffer.length;
        }

        // copy remaining partial block into now-empty buffer
        copyToBuffer(input, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        flushBuffer();
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flush();
        out.close();
    }

    private void copyToBuffer(byte[] input, int offset, int length)
    {
        System.arraycopy(input, offset, buffer, position, length);
        position += length;
    }

    private void flushBuffer()
            throws IOException
    {
        if (position > 0) {
            writeCompressed(buffer, 0, position);
            position = 0;
        }
    }

    private void writeCompressed(byte[] input, int offset, int length)
            throws IOException
    {
        int compressed = Snappy.compress(input, offset, length, outputBuffer, 0);

        // use uncompressed input if less than 12.5% compression
        if (compressed >= (length - (length / 8))) {
            writeUncompressedBlock(input, offset, length);
        }
        else {
            writeCompressedBlock(outputBuffer, 0, compressed);
        }
    }

    private void writeUncompressedBlock(byte[] uncompressed, int offset, int length)
            throws IOException
    {
        int n = length - 1;
        out.write(n >>> 8);
        out.write(n & 0xFF);
        out.write(uncompressed, offset, length);
    }

    private void writeCompressedBlock(byte[] compressed, int offset, int length)
            throws IOException
    {
        int n = length - 1;
        out.write((n >>> 8) | 0x80);
        out.write(n & 0xFF);
        out.write(compressed, offset, length);
    }
}
