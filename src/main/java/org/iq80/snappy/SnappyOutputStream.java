package org.iq80.snappy;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class implements an output stream for writing Snappy compressed data.
 * The output format is one or more compressed blocks of data, each of which
 * is preceded by a three byte header.
 * <p/>
 * The first byte of the header is a flag indicating if the block is compressed
 * or not. A value of 0x00 means uncompressed, and 0x01 means compressed.
 * <p/>
 * The second and third bytes are the size of the block in the stream as a big
 * endian number. This value is never zero as empty blocks are never written.
 * <p/>
 * An uncompressed block is simply copied from the input, thus guaranteeing
 * that the output is never larger than the input (not including the header).
 */
public class SnappyOutputStream
        extends OutputStream
{
    // the header format requires the max block size to fit in 15 bits -- do not change!
    static final int MAX_BLOCK_SIZE = 1 << 15;
    private final byte[] buffer = new byte[MAX_BLOCK_SIZE];
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
            writeBlock(input, offset, length, false);
        }
        else {
            writeBlock(outputBuffer, 0, compressed, true);
        }
    }

    private void writeBlock(byte[] data, int offset, int length, boolean compressed)
            throws IOException
    {
        out.write(compressed ? 0x01 : 0x00);
        out.write(length >>> 8);
        out.write(length & 0xFF);
        out.write(data, offset, length);
    }
}
