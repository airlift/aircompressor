package org.iq80.snappy;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;

/**
 * This class implements an input stream for reading Snappy compressed data
 * of the format produced by {@link SnappyOutputStream}.
 */
public class SnappyInputStream
        extends InputStream
{
    // The buffer size is the same as the block size.
    // This works because the original data is not allowed to expand.
    private final byte[] input = new byte[32768];
    private final byte[] uncompressed = new byte[32768];
    private final byte[] headerBytes = new byte[2];
    private final InputStream in;

    // Buffer is a reference to the real buffer for the current block:
    // uncompressed if the block is compressed, or input if it is not.
    // Valid is the total valid bytes in the referenced buffer.
    private byte[] buffer;
    private int valid = 0;
    private int position = 0;
    private boolean eof = false;

    /**
     * Creates a Snappy input stream to read data from the specified underlying input stream.
     *
     * @param in the underlying input stream
     */
    public SnappyInputStream(InputStream in)
    {
        this.in = in;
    }

    @Override
    public int read()
            throws IOException
    {
        if (!ensureBuffer()) {
            return -1;
        }
        return buffer[position++];
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        if (length == 0) {
            return 0;
        }
        if (!ensureBuffer()) {
            return -1;
        }

        int size = min(length, available());
        System.arraycopy(buffer, position, output, offset, size);
        position += size;
        return size;
    }

    @Override
    public int available()
            throws IOException
    {
        return valid - position;
    }

    @Override
    public void close()
            throws IOException
    {
        in.close();
    }

    private boolean ensureBuffer()
            throws IOException
    {
        if (available() > 0) {
            return true;
        }
        if (eof) {
            return false;
        }

        if (!readBlockHeaderBytes()) {
            eof = true;
            return false;
        }
        int header = convertBlockHeaderBytes();

        // extract compressed flag and length from header
        boolean compressed = (header & 0x8000) != 0;
        int length = (header & 0x7FFF) + 1;

        readInput(length);

        handleInput(length, compressed);

        return true;
    }

    private void handleInput(int length, boolean compressed)
    {
        if (compressed) {
            buffer = uncompressed;
            valid = Snappy.uncompress(input, 0, length, uncompressed, 0);
        }
        else {
            buffer = input;
            valid = length;
        }
        position = 0;
    }

    private void readInput(int length)
            throws IOException
    {
        int offset = 0;
        while (offset < length) {
            int size = in.read(input, offset, length - offset);
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            offset += size;
        }
    }

    private boolean readBlockHeaderBytes()
            throws IOException
    {
        int n = in.read(headerBytes, 0, 2);
        if (n == -1) {
            return false;
        }
        if (n == 1) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("encountered EOF while reading block header");
            }
            headerBytes[1] = (byte) b;
        }
        return true;
    }

    private int convertBlockHeaderBytes()
    {
        int a = headerBytes[0] & 0xFF;
        int b = headerBytes[1] & 0xFF;
        return (a << 8) | b;
    }
}
