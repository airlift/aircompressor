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
package io.airlift.compress.v2.zstd;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static io.airlift.compress.v2.zstd.CompressionParameters.DEFAULT_COMPRESSION_LEVEL;
import static io.airlift.compress.v2.zstd.Constants.SIZE_OF_BLOCK_HEADER;
import static io.airlift.compress.v2.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.v2.zstd.Util.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ZstdOutputStream
        extends OutputStream
{
    private final OutputStream outputStream;
    private final CompressionContext context;
    private final int maxBufferSize;

    private XxHash64 partialHash;

    private byte[] uncompressed = new byte[0];
    private final byte[] compressed;

    // start of unprocessed data in uncompressed buffer
    private int uncompressedOffset;
    // end of unprocessed data in uncompressed buffer
    private int uncompressedPosition;

    private boolean closed;

    public ZstdOutputStream(OutputStream outputStream)
            throws IOException
    {
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
        this.context = new CompressionContext(CompressionParameters.compute(DEFAULT_COMPRESSION_LEVEL, -1), ARRAY_BYTE_BASE_OFFSET, Integer.MAX_VALUE);
        this.maxBufferSize = context.parameters.getWindowSize() * 4;

        // create output buffer large enough for a single block
        int bufferSize = context.parameters.getBlockSize() + SIZE_OF_BLOCK_HEADER;
        // todo is the "+ (bufferSize >>> 8)" required here?
        // add extra long to give code more leeway
        this.compressed = new byte[bufferSize + (bufferSize >>> 8) + SIZE_OF_LONG];
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        growBufferIfNecessary(1);

        uncompressed[uncompressedPosition++] = (byte) b;

        compressIfNecessary();
    }

    @Override
    public void write(byte[] buffer)
            throws IOException
    {
        write(buffer, 0, buffer.length);
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        growBufferIfNecessary(length);

        while (length > 0) {
            int writeSize = min(length, uncompressed.length - uncompressedPosition);
            System.arraycopy(buffer, offset, uncompressed, uncompressedPosition, writeSize);

            uncompressedPosition += writeSize;
            length -= writeSize;
            offset += writeSize;

            compressIfNecessary();
        }
    }

    private void growBufferIfNecessary(int length)
    {
        if (uncompressedPosition + length <= uncompressed.length || uncompressed.length >= maxBufferSize) {
            return;
        }

        // assume we will need double the current required space
        int newSize = (uncompressed.length + length) * 2;
        // limit to max buffer size
        newSize = min(newSize, maxBufferSize);
        // allocate at least a minimal buffer to start;
        newSize = max(newSize, context.parameters.getBlockSize());
        uncompressed = Arrays.copyOf(uncompressed, newSize);
    }

    private void compressIfNecessary()
            throws IOException
    {
        // only flush when the buffer if is max size, full, and the buffer is larger than the window and one additional block
        if (uncompressed.length >= maxBufferSize &&
                uncompressedPosition == uncompressed.length &&
                uncompressed.length - context.parameters.getWindowSize() > context.parameters.getBlockSize()) {
            writeChunk(false);
        }
    }

    // visible for Hadoop stream
    void finishWithoutClosingSource()
            throws IOException
    {
        if (!closed) {
            writeChunk(true);
            closed = true;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            writeChunk(true);

            closed = true;
            outputStream.close();
        }
    }

    private void writeChunk(boolean lastChunk)
            throws IOException
    {
        int chunkSize;
        if (lastChunk) {
            // write all the data
            chunkSize = uncompressedPosition - uncompressedOffset;
        }
        else {
            int blockSize = context.parameters.getBlockSize();
            chunkSize = uncompressedPosition - uncompressedOffset - context.parameters.getWindowSize() - blockSize;
            checkState(chunkSize > blockSize, "Must write at least one full block");
            // only write full blocks
            chunkSize = (chunkSize / blockSize) * blockSize;
        }

        // if first write
        if (partialHash == null) {
            partialHash = new XxHash64();

            // if this is also the last chunk we know the exact size, otherwise, this is traditional streaming
            int inputSize = lastChunk ? chunkSize : -1;

            int outputAddress = ARRAY_BYTE_BASE_OFFSET;
            outputAddress += ZstdFrameCompressor.writeMagic(compressed, outputAddress, outputAddress + 4);
            outputAddress += ZstdFrameCompressor.writeFrameHeader(compressed, outputAddress, outputAddress + 14, inputSize, context.parameters.getWindowSize());
            outputStream.write(compressed, 0, outputAddress - ARRAY_BYTE_BASE_OFFSET);
        }

        partialHash.update(uncompressed, uncompressedOffset, chunkSize);

        // write one block at a time
        // note this is a do while to ensure that zero length input gets at least one block written
        do {
            int blockSize = min(chunkSize, context.parameters.getBlockSize());
            int compressedSize = ZstdFrameCompressor.writeCompressedBlock(
                    uncompressed,
                    ARRAY_BYTE_BASE_OFFSET + uncompressedOffset,
                    blockSize,
                    compressed,
                    ARRAY_BYTE_BASE_OFFSET,
                    compressed.length,
                    context,
                    lastChunk && blockSize == chunkSize);
            outputStream.write(compressed, 0, compressedSize);
            uncompressedOffset += blockSize;
            chunkSize -= blockSize;
        }
        while (chunkSize > 0);

        if (lastChunk) {
            // write checksum
            int hash = (int) partialHash.hash();
            outputStream.write(hash);
            outputStream.write(hash >> 8);
            outputStream.write(hash >> 16);
            outputStream.write(hash >> 24);
        }
        else {
            // slide window forward, leaving the entire window and the unprocessed data
            int slideWindowSize = uncompressedOffset - context.parameters.getWindowSize();
            context.slideWindow(slideWindowSize);

            System.arraycopy(uncompressed, slideWindowSize, uncompressed, 0, context.parameters.getWindowSize() + (uncompressedPosition - uncompressedOffset));
            uncompressedOffset -= slideWindowSize;
            uncompressedPosition -= slideWindowSize;
        }
    }
}
