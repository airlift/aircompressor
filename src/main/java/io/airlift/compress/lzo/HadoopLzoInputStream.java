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
package io.airlift.compress.lzo;

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_LONG;

class HadoopLzoInputStream
        extends CompressionInputStream
{
    private final LzoDecompressor decompressor = new LzoDecompressor();
    private final InputStream in;
    private final byte[] uncompressedChunk;

    private int uncompressedBlockLength;
    private int uncompressedChunkOffset;
    private int uncompressedChunkLength;

    private byte[] compressed = new byte[0];

    public HadoopLzoInputStream(InputStream in, int maxUncompressedLength)
            throws IOException
    {
        super(in);
        this.in = in;
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
            uncompressedChunkLength = decompressor.decompress(compressed, 0, compressedChunkLength, uncompressedChunk, 0, uncompressedChunk.length);
        }
        return uncompressedChunk[uncompressedChunkOffset++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        while (uncompressedChunkOffset >= uncompressedChunkLength) {
            int compressedChunkLength = bufferCompressedData();
            if (compressedChunkLength < 0) {
                return -1;
            }

            // favor writing directly to user buffer to avoid extra copy
            if (length >= uncompressedBlockLength) {
                uncompressedChunkLength = decompressor.decompress(compressed, 0, compressedChunkLength, output, offset, length);
                uncompressedChunkOffset = uncompressedChunkLength;
                return uncompressedChunkLength;
            }

            uncompressedChunkLength = decompressor.decompress(compressed, 0, compressedChunkLength, uncompressedChunk, 0, uncompressedChunk.length);
        }
        int size = Math.min(length, uncompressedChunkLength - uncompressedChunkOffset);
        System.arraycopy(uncompressedChunk, uncompressedChunkOffset, output, offset, size);
        uncompressedChunkOffset += size;
        return size;
    }

    @Override
    public void resetState()
            throws IOException
    {
        throw new UnsupportedOperationException("resetState not supported for Lzo");
    }

    private int bufferCompressedData()
            throws IOException
    {
        uncompressedBlockLength -= uncompressedChunkOffset;
        uncompressedChunkOffset = 0;
        uncompressedChunkLength = 0;
        while (uncompressedBlockLength == 0) {
            uncompressedBlockLength = readBigEndianInt();
            if (uncompressedBlockLength == -1) {
                uncompressedBlockLength = 0;
                return -1;
            }
        }

        int compressedChunkLength = readBigEndianInt();
        if (compressedChunkLength == -1) {
            return -1;
        }

        if (compressed.length < compressedChunkLength) {
            // over allocate buffer which makes decompression easier
            compressed = new byte[compressedChunkLength + SIZE_OF_LONG];
        }
        readInput(compressedChunkLength, compressed);
        return compressedChunkLength;
    }

    private void readInput(int length, byte[] buffer)
            throws IOException
    {
        int offset = 0;
        while (offset < length) {
            int size = in.read(buffer, offset, length - offset);
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            offset += size;
        }
    }

    private int readBigEndianInt()
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
        return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4));
    }
}
