package org.iq80.snappy;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;
import static java.lang.String.format;
import static org.iq80.snappy.SnappyOutputStream.MAX_BLOCK_SIZE;

/**
 * This class implements an input stream for reading Snappy compressed data
 * of the format produced by {@link SnappyOutputStream}.
 */
public class SnappyInputStream
        extends InputStream
{
    // The buffer size is the same as the block size.
    // This works because the original data is not allowed to expand.
    private final BufferRecycler recycler;
    private final byte[] input;
    private final byte[] uncompressed;
    private final byte[] header = new byte[3];
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
        recycler = BufferRecycler.instance();
        input = recycler.allocInputBuffer(MAX_BLOCK_SIZE);
        uncompressed = recycler.allocDecodeBuffer(MAX_BLOCK_SIZE);

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
        try {
            in.close();
        }
        finally {
            recycler.releaseInputBuffer(input);
            recycler.releaseDecodeBuffer(uncompressed);
        }
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

        if (!readBlockHeader()) {
            eof = true;
            return false;
        }
        boolean compressed = getHeaderCompressedFlag();
        int length = getHeaderLength();

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

    private boolean readBlockHeader()
            throws IOException
    {
        int offset = 0;
        while (offset < header.length) {
            int size = in.read(header, offset, header.length - offset);
            if (size == -1) {
                // EOF on first byte means the stream ended cleanly
                if (offset == 0) {
                    return false;
                }
                throw new EOFException("encounted EOF while reading block header");
            }
            offset += size;
        }
        return true;
    }

    private boolean getHeaderCompressedFlag()
            throws IOException
    {
        int x = header[0] & 0xFF;
        switch (x) {
            case 0x00:
                return false;
            case 0x01:
                return true;
            default:
                throw new IOException(format("invalid compressed flag in header: 0x%02x", x));
        }
    }

    private int getHeaderLength()
            throws IOException
    {
        int a = header[1] & 0xFF;
        int b = header[2] & 0xFF;
        int length = (a << 8) | b;
        if ((length <= 0) || (length > MAX_BLOCK_SIZE)) {
            throw new IOException("invalid block size in header: " + length);
        }
        return length;
    }
}
