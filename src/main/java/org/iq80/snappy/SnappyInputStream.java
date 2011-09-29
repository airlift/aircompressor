package org.iq80.snappy;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;
import static java.lang.String.format;
import static org.iq80.snappy.SnappyInternalUtils.checkNotNull;
import static org.iq80.snappy.SnappyInternalUtils.checkPositionIndexes;
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
    private final byte[] header = new byte[7];
    private final InputStream in;
    private final boolean verifyChecksums;

    // Buffer is a reference to the real buffer for the current block:
    // uncompressed if the block is compressed, or input if it is not.
    // Valid is the total valid bytes in the referenced buffer.
    private byte[] buffer;
    private int valid;
    private int position;
    private boolean closed;
    private boolean eof;

    /**
     * Creates a Snappy input stream to read data from the specified underlying input stream.
     *
     * @param in the underlying input stream
     */
    public SnappyInputStream(InputStream in)
    {
        this(in, true);
    }
    /**
     * Creates a Snappy input stream to read data from the specified underlying input stream.
     *
     * @param in the underlying input stream
     * @param verifyChecksums if true, checksums in input stream will be verified
     */
    public SnappyInputStream(InputStream in, boolean verifyChecksums)
    {
        this.in = in;
        this.verifyChecksums = verifyChecksums;
        recycler = BufferRecycler.instance();
        input = recycler.allocInputBuffer(MAX_BLOCK_SIZE);
        uncompressed = recycler.allocDecodeBuffer(MAX_BLOCK_SIZE);

    }

    @Override
    public int read()
            throws IOException
    {
        if (closed) {
            return -1;
        }
        if (!ensureBuffer()) {
            return -1;
        }
        return buffer[position++];
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        checkNotNull(output, "output is null");
        checkPositionIndexes(offset, offset + length, output.length);
        if (closed) {
            throw new IOException("Stream is closed");
        }

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
        if (closed) {
            return 0;
        }
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
            if (!closed) {
                closed = true;
                recycler.releaseInputBuffer(input);
                recycler.releaseDecodeBuffer(uncompressed);
            }
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
            throws IOException
    {
        if (compressed) {
            buffer = uncompressed;
            try {
                valid = Snappy.uncompress(input, 0, length, uncompressed, 0);
            }
            catch (CorruptionException e) {
                throw new IOException("Corrupt input", e);
            }
        }
        else {
            buffer = input;
            valid = length;
        }

        if (verifyChecksums) {
            int expectedCrc32c = getCrc32c();
            int actualCrc32c = Crc32C.maskedCrc32c(buffer, 0, valid);
            if (expectedCrc32c != actualCrc32c) {
                throw new IOException("Corrupt input: invalid checksum");
            }
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
                throw new EOFException("encountered EOF while reading block header");
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

    private int getCrc32c()
            throws IOException
    {
        return ((header[3] & 0xFF) << 24) |
                ((header[4] & 0xFF) << 16) |
                ((header[5] & 0xFF) << 8) |
                (header[6] & 0xFF);

    }
}
