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

import io.airlift.compress.v3.hadoop.HadoopInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static io.airlift.compress.v3.lzo.LzoConstants.SIZE_OF_LONG;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class LzoHadoopInputStream
        extends HadoopInputStream
{
    private final LzoDecompressor decompressor = new LzoDecompressor();
    private final InputStream in;
    private final byte[] uncompressedChunk;

    private int uncompressedBlockLength;
    private int uncompressedChunkOffset;
    private int uncompressedChunkLength;

    private byte[] compressed = new byte[0];

    public LzoHadoopInputStream(InputStream in, int maxUncompressedLength)
    {
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
            uncompressedChunkLength = decompressor.decompress(compressed, 0, compressedChunkLength, uncompressedChunk, 0, uncompressedChunk.length);
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
            uncompressedBlockLength = readBigEndianInt(true);
            if (uncompressedBlockLength == -1) {
                uncompressedBlockLength = 0;
                return -1;
            }
        }

        int compressedChunkLength = readBigEndianInt(false);

        compressed = readCompressedBlock(in, compressed, compressedChunkLength);
        return compressedChunkLength;
    }

    private int readBigEndianInt(boolean allowEndOfStream)
            throws IOException
    {
        int b1 = in.read();
        if (b1 < 0) {
            if (allowEndOfStream) {
                return -1;
            }
            throw new EOFException("encountered EOF while reading block header");
        }
        int b2 = in.read();
        int b3 = in.read();
        int b4 = in.read();

        // If any of the other bits are negative, the stream it truncated
        if ((b2 | b3 | b4) < 0) {
            throw new IOException("Stream is truncated");
        }
        int value = (b1 << 24) + (b2 << 16) + (b3 << 8) + b4;
        if (value < 0) {
            throw new IOException("Negative block length: " + value);
        }
        return value;
    }
}
