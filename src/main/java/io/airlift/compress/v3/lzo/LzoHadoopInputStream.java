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
package io.airlift.compress.v3.lzo;

import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.hadoop.HadoopInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static io.airlift.compress.v3.lzo.LzoConstants.MAX_BLOCK_SIZE;
import static io.airlift.compress.v3.lzo.LzoConstants.SIZE_OF_LONG;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class LzoHadoopInputStream
        extends HadoopInputStream
{
    private static final int END_OF_STREAM = -1;
    private static final int BUFFERED = -2;
    private static final int INITIAL_COMPRESSED_BUFFER_SIZE = 64 * 1024;

    private final LzoDecompressor decompressor = new LzoDecompressor();
    private final InputStream in;
    private byte[] uncompressedChunk;

    private int uncompressedBlockLength;
    private int uncompressedChunkOffset;
    private int uncompressedChunkLength;

    private boolean failed;

    private byte[] compressed = new byte[0];

    public LzoHadoopInputStream(InputStream in, int bufferSize)
    {
        this.in = requireNonNull(in, "in is null");
        // over allocate buffer which makes decompression easier
        uncompressedChunk = new byte[bufferSize + SIZE_OF_LONG];
    }

    @Override
    public int read()
            throws IOException
    {
        while (uncompressedChunkOffset >= uncompressedChunkLength) {
            if (readChunk(null, 0, 0) == END_OF_STREAM) {
                return -1;
            }
        }
        return uncompressedChunk[uncompressedChunkOffset++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, output.length);
        if (length == 0) {
            return 0;
        }

        while (uncompressedChunkOffset >= uncompressedChunkLength) {
            int size = readChunk(output, offset, length);
            if (size == END_OF_STREAM) {
                return -1;
            }
            // an empty chunk decodes to nothing, so continue with the next one instead of returning zero
            if (size > 0) {
                return size;
            }
        }
        int size = Math.min(length, uncompressedChunkLength - uncompressedChunkOffset);
        System.arraycopy(uncompressedChunk, uncompressedChunkOffset, output, offset, size);
        uncompressedChunkOffset += size;
        return size;
    }

    @Override
    public void resetState()
    {
        uncompressedBlockLength = 0;
        uncompressedChunkOffset = 0;
        uncompressedChunkLength = 0;
        failed = false;
    }

    @Override
    public void close()
            throws IOException
    {
        in.close();
    }

    // Reads and decodes the next chunk. Decodes straight into the caller's buffer when it is large enough
    // and returns the decoded size, otherwise decodes into the chunk buffer and returns BUFFERED.
    private int readChunk(byte[] output, int outputOffset, int outputLength)
            throws IOException
    {
        if (failed) {
            throw new IOException("Stream has failed");
        }

        try {
            int compressedChunkLength = bufferCompressedData();
            if (compressedChunkLength < 0) {
                return END_OF_STREAM;
            }

            // favor writing directly to user buffer to avoid extra copy
            if (outputLength >= uncompressedBlockLength) {
                int decodedLength = decompressor.decompress(compressed, 0, compressedChunkLength, output, outputOffset, outputLength);
                validateChunkLength(decodedLength);
                uncompressedChunkLength = decodedLength;
                uncompressedChunkOffset = decodedLength;
                return decodedLength;
            }

            int decodedLength = decompressIntoChunk(compressedChunkLength);
            validateChunkLength(decodedLength);
            uncompressedChunkLength = decodedLength;
            return BUFFERED;
        }
        catch (IOException | RuntimeException e) {
            // the input is left partway through a block, so it cannot be read further
            failed = true;
            uncompressedChunkLength = 0;
            uncompressedChunkOffset = 0;
            throw e;
        }
    }

    private void validateChunkLength(int decodedLength)
            throws IOException
    {
        // the decoder treats the whole output capacity as valid, so a chunk can decode past the block it belongs to
        if (decodedLength > uncompressedBlockLength) {
            throw new IOException("Chunk decodes to " + decodedLength + " bytes, more than the remaining block length " + uncompressedBlockLength);
        }
    }

    private int decompressIntoChunk(int compressedChunkLength)
    {
        // A chunk never exceeds the remaining block length, so that bounds how far the buffer can grow
        int maxLength = Math.min(uncompressedBlockLength, MAX_BLOCK_SIZE) + SIZE_OF_LONG;
        while (true) {
            try {
                return decompressor.decompress(compressed, 0, compressedChunkLength, uncompressedChunk, 0, uncompressedChunk.length);
            }
            catch (MalformedInputException e) {
                // Overflow and corruption are both reported as malformed input. Double the buffer and decode the
                // chunk again until it fits, so the buffer stays close to the chunk size and not the block size.
                if (uncompressedChunk.length >= maxLength) {
                    throw e;
                }
                uncompressedChunk = new byte[(int) Math.min(maxLength, 2L * uncompressedChunk.length)];
            }
        }
    }

    private int bufferCompressedData()
            throws IOException
    {
        uncompressedBlockLength -= uncompressedChunkOffset;
        uncompressedChunkOffset = 0;
        uncompressedChunkLength = 0;
        while (uncompressedBlockLength == 0) {
            long blockLength = readUnsignedInt();
            if (blockLength == -1) {
                return -1;
            }
            // The block length is the aggregate of independently compressed chunks, so it can exceed the buffer size
            if (blockLength > Integer.MAX_VALUE) {
                throw new IOException("Invalid uncompressed block length: " + blockLength);
            }
            uncompressedBlockLength = (int) blockLength;
        }

        long chunkLength = readUnsignedInt();
        if (chunkLength == -1) {
            return -1;
        }
        if (chunkLength > Integer.MAX_VALUE - SIZE_OF_LONG) {
            throw new IOException("Invalid compressed chunk length: " + chunkLength);
        }
        int compressedChunkLength = (int) chunkLength;

        readCompressedChunk(compressedChunkLength);
        return compressedChunkLength;
    }

    private void readCompressedChunk(int length)
            throws IOException
    {
        // The declared length can be corrupt and the format has no useful upper bound for it, so grow the
        // buffer with the bytes that actually arrive instead of allocating the declared length up front
        int offset = 0;
        while (offset < length) {
            if (offset >= compressed.length - SIZE_OF_LONG) {
                int capacity = (int) Math.min(length, Math.max(INITIAL_COMPRESSED_BUFFER_SIZE, 2L * offset));
                // over allocate buffer which makes decompression easier
                compressed = Arrays.copyOf(compressed, capacity + SIZE_OF_LONG);
            }
            int size = in.read(compressed, offset, Math.min(length, compressed.length - SIZE_OF_LONG) - offset);
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            offset += size;
        }
    }

    // Returns the unsigned value, or -1 only at physical end of stream before the first byte
    private long readUnsignedInt()
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
        return ((long) b1 << 24) + (b2 << 16) + (b3 << 8) + b4;
    }
}
