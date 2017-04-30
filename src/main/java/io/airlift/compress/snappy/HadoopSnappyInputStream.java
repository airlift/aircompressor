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
package io.airlift.compress.snappy;

import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static io.airlift.compress.snappy.SnappyConstants.SIZE_OF_LONG;

class HadoopSnappyInputStream
        extends CompressionInputStream
{
    private final SnappyDecompressor decompressor = new SnappyDecompressor();
    private final InputStream in;

    private int uncompressedBlockLength;
    private byte[] uncompressedChunk = new byte[0];
    private int uncompressedChunkOffset;
    private int uncompressedChunkLength;

    private byte[] compressed = new byte[0];

    public HadoopSnappyInputStream(InputStream in)
            throws IOException
    {
        super(in);
        this.in = in;
    }

    @Override
    public int read()
            throws IOException
    {
        if (uncompressedChunkOffset >= uncompressedChunkLength) {
            readNextChunk(uncompressedChunk, 0, uncompressedChunk.length);
            if (uncompressedChunkLength == 0) {
                return -1;
            }
        }
        return uncompressedChunk[uncompressedChunkOffset++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        if (uncompressedChunkOffset >= uncompressedChunkLength) {
            boolean directDecompress = readNextChunk(output, offset, length);
            if (uncompressedChunkLength == 0) {
                return -1;
            }
            if (directDecompress) {
                uncompressedChunkOffset += uncompressedChunkLength;
                return uncompressedChunkLength;
            }
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
        throw new UnsupportedOperationException("resetState not supported for Snappy");
    }

    private boolean readNextChunk(byte[] userBuffer, int userOffset, int userLength)
            throws IOException
    {
        uncompressedBlockLength -= uncompressedChunkOffset;
        uncompressedChunkOffset = 0;
        uncompressedChunkLength = 0;
        while (uncompressedBlockLength == 0) {
            uncompressedBlockLength = readBigEndianInt();
            if (uncompressedBlockLength == -1) {
                uncompressedBlockLength = 0;
                return false;
            }
        }

        int compressedChunkLength = readBigEndianInt();
        if (compressedChunkLength == -1) {
            return false;
        }

        if (compressed.length < compressedChunkLength) {
             // over allocate buffer which makes decompression easier
            compressed = new byte[compressedChunkLength + SIZE_OF_LONG];
        }
        readInput(compressedChunkLength, compressed);

        uncompressedChunkLength = SnappyDecompressor.getUncompressedLength(compressed, 0);
        if (uncompressedChunkLength > uncompressedBlockLength) {
            throw new IOException("Chunk uncompressed size is greater than block size");
        }

        boolean directUncompress = true;
        if (uncompressedChunkLength > userLength) {
            if (uncompressedChunk.length < uncompressedChunkLength) {
                // over allocate buffer which makes decompression easier
                uncompressedChunk = new byte[uncompressedChunkLength + SIZE_OF_LONG];
            }
            directUncompress = false;
            userBuffer = uncompressedChunk;
            userOffset = 0;
            userLength = uncompressedChunk.length;
        }

        int bytes = decompressor.decompress(compressed, 0, compressedChunkLength, userBuffer, userOffset, userLength);
        if (uncompressedChunkLength != bytes) {
            throw new IOException("Expected to read " + uncompressedChunkLength + " bytes, but data only contained " + bytes + " bytes");
        }
        return directUncompress;
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
