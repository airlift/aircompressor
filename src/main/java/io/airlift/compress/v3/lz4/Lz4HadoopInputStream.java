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
package io.airlift.compress.v3.lz4;

import io.airlift.compress.v3.hadoop.HadoopInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static io.airlift.compress.v3.lz4.Lz4Constants.SIZE_OF_LONG;
import static java.lang.String.format;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class Lz4HadoopInputStream
        extends HadoopInputStream
{
    private final Lz4Decompressor decompressor;
    private final InputStream in;
    private final byte[] uncompressedChunk;

    private int uncompressedBlockLength;
    private int uncompressedChunkOffset;
    private int uncompressedChunkLength;

    private byte[] compressed = new byte[0];

    public Lz4HadoopInputStream(Lz4Decompressor decompressor, InputStream in, int maxUncompressedLength)
    {
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.in = requireNonNull(in, "in is null");
        // over allocate buffer which makes decompression easier
        uncompressedChunk = new byte[maxUncompressedLength + SIZE_OF_LONG];
    }

    @Override
    public int read()
            throws IOException
    {
        while (uncompressedChunkOffset >= uncompressedChunkLength) {
            int compressedChunkLength = bufferCompressedData();
            if (compressedChunkLength < 0) {
                return -1;
            }
            uncompressedChunkLength = decompressChunk(compressedChunkLength, uncompressedChunk, 0, uncompressedChunk.length);
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
            int compressedChunkLength = bufferCompressedData();
            if (compressedChunkLength < 0) {
                return -1;
            }

            // favor writing directly to user buffer to avoid extra copy
            if (length >= uncompressedBlockLength) {
                uncompressedChunkLength = decompressChunk(compressedChunkLength, output, offset, length);
                uncompressedChunkOffset = uncompressedChunkLength;
                // an empty chunk decodes to nothing, so continue with the next one instead of returning zero
                if (uncompressedChunkLength == 0) {
                    continue;
                }
                return uncompressedChunkLength;
            }

            uncompressedChunkLength = decompressChunk(compressedChunkLength, uncompressedChunk, 0, uncompressedChunk.length);
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
    }

    @Override
    public void close()
            throws IOException
    {
        in.close();
    }

    private int bufferCompressedData()
            throws IOException
    {
        uncompressedBlockLength -= uncompressedChunkOffset;
        uncompressedChunkOffset = 0;
        uncompressedChunkLength = 0;
        while (uncompressedBlockLength == 0) {
            long blockLength = readBigEndianInt();
            if (blockLength == -1) {
                return -1;
            }
            if (blockLength > Integer.MAX_VALUE) {
                throw new IOException("Invalid uncompressed block length: " + blockLength);
            }
            uncompressedBlockLength = (int) blockLength;
        }

        long chunkLength = readBigEndianInt();
        if (chunkLength == -1) {
            throw new EOFException("encountered EOF while reading compressed chunk length");
        }
        if (chunkLength == 0 || chunkLength > Integer.MAX_VALUE - SIZE_OF_LONG) {
            throw new IOException("Invalid compressed chunk length: " + chunkLength);
        }
        int compressedChunkLength = (int) chunkLength;
        // A chunk holds at most the rest of the block, so it cannot be longer than the LZ4 worst case for that
        // many bytes. This is Lz4RawCompressor.maxCompressedLength, computed in long to avoid overflow.
        long maxCompressedLength = (long) uncompressedBlockLength + uncompressedBlockLength / 255 + 16;
        if (compressedChunkLength > maxCompressedLength) {
            throw new IOException(format(
                    "Compressed chunk length %s exceeds LZ4 bound %s for remaining block length %s",
                    compressedChunkLength,
                    maxCompressedLength,
                    uncompressedBlockLength));
        }
        readInput(compressedChunkLength);
        return compressedChunkLength;
    }

    private int decompressChunk(int compressedChunkLength, byte[] output, int offset, int length)
            throws IOException
    {
        int decodedLength = decompressor.decompress(compressed, 0, compressedChunkLength, output, offset, length);
        // checked before the length is stored, so a later read cannot return bytes of a rejected chunk
        if (decodedLength < 0 || decodedLength > uncompressedBlockLength) {
            throw new IOException(format("Invalid decompressed chunk length %s for remaining block length %s", decodedLength, uncompressedBlockLength));
        }
        return decodedLength;
    }

    private void readInput(int length)
            throws IOException
    {
        // The declared length can be forged, so grow the buffer only as data arrives
        // instead of allocating the declared length up front
        int offset = 0;
        while (offset < length) {
            if (compressed.length - SIZE_OF_LONG <= offset) {
                int capacity = (int) Math.min((long) length, Math.max(8192L, (long) offset * 2));
                // over allocate buffer which makes decompression easier
                compressed = Arrays.copyOf(compressed, capacity + SIZE_OF_LONG);
            }
            int size = in.read(compressed, offset, Math.min(length - offset, compressed.length - SIZE_OF_LONG - offset));
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            offset += size;
        }
    }

    private long readBigEndianInt()
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
        return ((long) b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    }
}
