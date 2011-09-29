package org.iq80.snappy;

import java.io.IOException;
import java.io.OutputStream;

import static org.iq80.snappy.Crc32C.maskedCrc32c;
import static org.iq80.snappy.SnappyInternalUtils.checkNotNull;
import static org.iq80.snappy.SnappyInternalUtils.checkPositionIndexes;

/**
 * This class implements an output stream for writing Snappy compressed data.
 * The output format is a file header "snappy\0" followed by one or more
 * compressed blocks of data, each of which is preceded by a seven byte header.
 * <p/>
 * The first byte of the header is a flag indicating if the block is compressed
 * or not. A value of 0x00 means uncompressed, and 0x01 means compressed.
 * <p/>
 * The second and third bytes are the size of the block in the stream as a big
 * endian number. This value is never zero as empty blocks are never written.
 * The maximum allowed length is 32k (1 << 15).
 * <p/>
 * The remaining four byes are crc32c checksum of the user input data masked
 * with the following function: {@code ((crc >>> 15) | (crc << 17)) + 0xa282ead8 }
 * <p/>
 * An uncompressed block is simply copied from the input, thus guaranteeing
 * that the output is never larger than the input (not including the header).
 */
public class SnappyOutputStream
        extends OutputStream
{
    static final byte[] FILE_HEADER = new byte[] { 's', 'n', 'a', 'p', 'p', 'y', 0};

    // the header format requires the max block size to fit in 15 bits -- do not change!
    static final int MAX_BLOCK_SIZE = 1 << 15;

    private final BufferRecycler recycler;
    private final byte[] buffer;
    private final byte[] outputBuffer;
    private final OutputStream out;

    private int position;
    private boolean closed;

    /**
     * Creates a Snappy output stream to write data to the specified underlying output stream.
     *
     * @param out the underlying output stream
     */
    public SnappyOutputStream(OutputStream out)
            throws IOException
    {
        this.out = out;
        recycler = BufferRecycler.instance();
        buffer = recycler.allocOutputBuffer(MAX_BLOCK_SIZE);
        outputBuffer = recycler.allocEncodingBuffer(Snappy.maxCompressedLength(MAX_BLOCK_SIZE));
        out.write(FILE_HEADER);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed") ;
        }
        if (position >= MAX_BLOCK_SIZE) {
            flushBuffer();
        }
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] input, int offset, int length)
            throws IOException
    {
        checkNotNull(input, "input is null");
        checkPositionIndexes(offset, offset + length, input.length);
        if (closed) {
            throw new IOException("Stream is closed") ;
        }

        int free = MAX_BLOCK_SIZE - position;

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
        while (length >= MAX_BLOCK_SIZE) {
            writeCompressed(input, offset, MAX_BLOCK_SIZE);
            offset += MAX_BLOCK_SIZE;
            length -= MAX_BLOCK_SIZE;
        }

        // copy remaining partial block into now-empty buffer
        copyToBuffer(input, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed") ;
        }
        flushBuffer();
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            flush();
            out.close();
        }
        finally {
            if (closed) {
                closed = true;
                recycler.releaseOutputBuffer(outputBuffer);
                recycler.releaseEncodeBuffer(buffer);
            }
        }
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
        // crc is based on the user supplied input data
        int crc32c = maskedCrc32c(input, offset, length);

        int compressed = Snappy.compress(input, offset, length, outputBuffer, 0);

        // use uncompressed input if less than 12.5% compression
        if (compressed >= (length - (length / 8))) {
            writeBlock(input, offset, length, false, crc32c);
        }
        else {
            writeBlock(outputBuffer, 0, compressed, true, crc32c);
        }
    }

    private void writeBlock(byte[] data, int offset, int length, boolean compressed, int crc32c)
            throws IOException
    {
        // write compressed flag
        out.write(compressed ? 0x01 : 0x00);

        // write length
        out.write(length >>> 8);
        out.write(length);

        // write crc32c of user input data
        out.write(crc32c >>> 24);
        out.write(crc32c >>> 16);
        out.write(crc32c >>> 8);
        out.write(crc32c);

        // write data
        out.write(data, offset, length);
    }
}
