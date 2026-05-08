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
package io.airlift.compress.v3.zstdFFM;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import static io.airlift.compress.v3.zstdFFM.CompressionParameters.DEFAULT_COMPRESSION_LEVEL;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_BLOCK_HEADER;
import static io.airlift.compress.v3.zstdFFM.Constants.SIZE_OF_LONG;
import static io.airlift.compress.v3.zstdFFM.Util.checkState;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class ZstdOutputStream
        extends OutputStream
{
    private final OutputStream outputStream;
    private final CompressionContext context;
    private final int maxBufferSize;

    private XxHash64 partialHash;

    private byte[] uncompressed = new byte[0];
    private MemorySegment uncompressedSegment = MemorySegment.ofArray(uncompressed);
    private final byte[] compressed;
    private final MemorySegment compressedSegment;

    // start of unprocessed data in uncompressed buffer
    private int uncompressedOffset;
    // end of unprocessed data in uncompressed buffer
    private int uncompressedPosition;

    private boolean closed;

    public ZstdOutputStream(OutputStream outputStream)
            throws IOException
    {
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
        this.context = new CompressionContext(CompressionParameters.compute(DEFAULT_COMPRESSION_LEVEL, -1), 0, Integer.MAX_VALUE);
        this.maxBufferSize = context.parameters.getWindowSize() * 4;

        int bufferSize = context.parameters.getBlockSize() + SIZE_OF_BLOCK_HEADER;
        this.compressed = new byte[bufferSize + (bufferSize >>> 8) + SIZE_OF_LONG];
        this.compressedSegment = MemorySegment.ofArray(compressed);
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

        int newSize = (uncompressed.length + length) * 2;
        newSize = min(newSize, maxBufferSize);
        newSize = max(newSize, context.parameters.getBlockSize());
        uncompressed = Arrays.copyOf(uncompressed, newSize);
        uncompressedSegment = MemorySegment.ofArray(uncompressed);
    }

    private void compressIfNecessary()
            throws IOException
    {
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
            chunkSize = uncompressedPosition - uncompressedOffset;
        }
        else {
            int blockSize = context.parameters.getBlockSize();
            chunkSize = uncompressedPosition - uncompressedOffset - context.parameters.getWindowSize() - blockSize;
            checkState(chunkSize > blockSize, "Must write at least one full block");
            chunkSize = (chunkSize / blockSize) * blockSize;
        }

        if (partialHash == null) {
            partialHash = new XxHash64();

            int inputSize = lastChunk ? chunkSize : -1;

            long outputAddress = 0;
            outputAddress += ZstdFrameCompressor.writeMagic(compressedSegment, outputAddress, outputAddress + 4);
            outputAddress += ZstdFrameCompressor.writeFrameHeader(compressedSegment, outputAddress, outputAddress + 14, inputSize, context.parameters.getWindowSize());
            outputStream.write(compressed, 0, (int) outputAddress);
        }

        partialHash.update(uncompressed, uncompressedOffset, chunkSize);

        do {
            int blockSize = min(chunkSize, context.parameters.getBlockSize());
            int compressedSize = ZstdFrameCompressor.writeCompressedBlock(
                    uncompressedSegment,
                    uncompressedOffset,
                    blockSize,
                    compressedSegment,
                    0,
                    compressed.length,
                    context,
                    lastChunk && blockSize == chunkSize);
            outputStream.write(compressed, 0, compressedSize);
            uncompressedOffset += blockSize;
            chunkSize -= blockSize;
        }
        while (chunkSize > 0);

        if (lastChunk) {
            int hash = (int) partialHash.hash();
            outputStream.write(hash);
            outputStream.write(hash >> 8);
            outputStream.write(hash >> 16);
            outputStream.write(hash >> 24);
        }
        else {
            int slideWindowSize = uncompressedOffset - context.parameters.getWindowSize();
            context.slideWindow(slideWindowSize);

            System.arraycopy(uncompressed, slideWindowSize, uncompressed, 0, context.parameters.getWindowSize() + (uncompressedPosition - uncompressedOffset));
            uncompressedOffset -= slideWindowSize;
            uncompressedPosition -= slideWindowSize;
        }
    }
}
