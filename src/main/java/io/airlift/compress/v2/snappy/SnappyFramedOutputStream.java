/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.v2.snappy;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

/**
 * Implements the <a href="http://snappy.googlecode.com/svn/trunk/framing_format.txt" >x-snappy-framed</a> as an {@link OutputStream}.
 */
public final class SnappyFramedOutputStream
        extends OutputStream
{
    /**
     * We place an additional restriction that the uncompressed data in
     * a chunk must be no longer than 65536 bytes. This allows consumers to
     * easily use small fixed-size buffers.
     */
    public static final int MAX_BLOCK_SIZE = 65536;

    public static final int DEFAULT_BLOCK_SIZE = MAX_BLOCK_SIZE;

    public static final double DEFAULT_MIN_COMPRESSION_RATIO = 0.85d;
    private final SnappyCompressor compressor;
    private final int blockSize;
    private final byte[] buffer;
    private final byte[] outputBuffer;
    private final double minCompressionRatio;
    private final OutputStream out;
    private final boolean writeChecksums;

    private int position;
    private boolean closed;

    /**
     * Creates a Snappy output stream to write data to the specified underlying output stream.
     *
     * @param out the underlying output stream
     */
    public SnappyFramedOutputStream(SnappyCompressor compressor, OutputStream out)
            throws IOException
    {
        this(compressor, out, true);
    }

    /**
     * Creates a Snappy output stream with block checksums disabled.  This is only useful for
     * apples-to-apples benchmarks with other compressors that do not perform block checksums.
     *
     * @param out the underlying output stream
     */
    public static SnappyFramedOutputStream newChecksumFreeBenchmarkOutputStream(SnappyCompressor compressor, OutputStream out)
            throws IOException
    {
        return new SnappyFramedOutputStream(compressor, out, false);
    }

    private SnappyFramedOutputStream(SnappyCompressor compressor, OutputStream out, boolean writeChecksums)
            throws IOException
    {
        this(compressor, out, writeChecksums, DEFAULT_BLOCK_SIZE, DEFAULT_MIN_COMPRESSION_RATIO);
    }

    public SnappyFramedOutputStream(SnappyCompressor compressor, OutputStream out, boolean writeChecksums, int blockSize, double minCompressionRatio)
            throws IOException
    {
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.out = requireNonNull(out, "out is null");
        this.writeChecksums = writeChecksums;
        SnappyInternalUtils.checkArgument(minCompressionRatio > 0 && minCompressionRatio <= 1.0, "minCompressionRatio %1s must be between (0,1.0].", minCompressionRatio);
        this.minCompressionRatio = minCompressionRatio;
        this.blockSize = blockSize;
        this.buffer = new byte[blockSize];
        this.outputBuffer = new byte[compressor.maxCompressedLength(blockSize)];

        out.write(SnappyFramed.HEADER_BYTES);
        SnappyInternalUtils.checkArgument(blockSize > 0 && blockSize <= MAX_BLOCK_SIZE, "blockSize must be in (0, 65536]", blockSize);
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (position >= blockSize) {
            flushBuffer();
        }
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] input, int offset, int length)
            throws IOException
    {
        SnappyInternalUtils.checkNotNull(input, "input is null");
        SnappyInternalUtils.checkPositionIndexes(offset, offset + length, input.length);
        if (closed) {
            throw new IOException("Stream is closed");
        }

        int free = blockSize - position;

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
        while (length >= blockSize) {
            writeCompressed(input, offset, blockSize);
            offset += blockSize;
            length -= blockSize;
        }

        // copy remaining partial block into now-empty buffer
        copyToBuffer(input, offset, length);
    }

    @Override
    public void flush()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        flushBuffer();
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        try {
            flush();
            out.close();
        }
        finally {
            closed = true;
        }
    }

    private void copyToBuffer(byte[] input, int offset, int length)
    {
        System.arraycopy(input, offset, buffer, position, length);
        position += length;
    }

    /**
     * Compresses and writes out any buffered data. This does nothing if there
     * is no currently buffered data.
     */
    private void flushBuffer()
            throws IOException
    {
        if (position > 0) {
            writeCompressed(buffer, 0, position);
            position = 0;
        }
    }

    /**
     * {@link Crc32C#maskedCrc32c(byte[], int, int) Calculates} the crc, compresses
     * the data, determines if the compression ratio is acceptable and calls
     * {@link #writeBlock(OutputStream, byte[], int, int, boolean, int)} to
     * actually write the frame.
     *
     * @param input The byte[] containing the raw data to be compressed.
     * @param offset The offset into <i>input</i> where the data starts.
     * @param length The amount of data in <i>input</i>.
     */
    private void writeCompressed(byte[] input, int offset, int length)
            throws IOException
    {
        // crc is based on the user supplied input data
        int crc32c = writeChecksums ? Crc32C.maskedCrc32c(input, offset, length) : 0;

        int compressed = compressor.compress(input,
                offset,
                length,
                outputBuffer,
                0,
                outputBuffer.length);

        // only use the compressed data if compression ratio is <= the minCompressionRatio
        if (((double) compressed / (double) length) <= minCompressionRatio) {
            writeBlock(out, outputBuffer, 0, compressed, true, crc32c);
        }
        else {
            // otherwise use the uncompressed data.
            writeBlock(out, input, offset, length, false, crc32c);
        }
    }

    /**
     * Write a frame (block) to <i>out</i>.
     *
     * @param out The {@link OutputStream} to write to.
     * @param data The data to write.
     * @param offset The offset in <i>data</i> to start at.
     * @param length The length of <i>data</i> to use.
     * @param compressed Indicates if <i>data</i> is the compressed or raw content.
     * This is based on whether the compression ratio desired is
     * reached.
     * @param crc32c The calculated checksum.
     */
    private void writeBlock(OutputStream out, byte[] data, int offset, int length, boolean compressed, int crc32c)
            throws IOException
    {
        out.write(compressed ? SnappyFramed.COMPRESSED_DATA_FLAG : SnappyFramed.UNCOMPRESSED_DATA_FLAG);

        // the length written out to the header is both the checksum and the frame
        int headerLength = length + 4;

        // write length
        out.write(headerLength);
        out.write(headerLength >>> 8);
        out.write(headerLength >>> 16);

        // write crc32c of user input data
        out.write(crc32c);
        out.write(crc32c >>> 8);
        out.write(crc32c >>> 16);
        out.write(crc32c >>> 24);

        // write data
        out.write(data, offset, length);
    }
}
