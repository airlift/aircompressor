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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import static io.airlift.compress.lzo.LzoConstants.SIZE_OF_LONG;
import static io.airlift.compress.lzo.LzopCodec.LZOP_IMPLEMENTATION_VERSION;
import static io.airlift.compress.lzo.LzopCodec.LZOP_MAGIC;
import static io.airlift.compress.lzo.LzopCodec.LZO_1X_VARIANT;
import static java.lang.String.format;

class HadoopLzopInputStream
        extends CompressionInputStream
{
    private static final int LZO_IMPLEMENTATION_VERSION = 0x2060;

    private final LzoDecompressor decompressor = new LzoDecompressor();
    private final InputStream in;
    private final byte[] uncompressedChunk;

    private int uncompressedLength;
    private int uncompressedOffset;

    private boolean finished;

    private byte[] compressed = new byte[0];

    public HadoopLzopInputStream(InputStream in, int maxUncompressedLength)
            throws IOException
    {
        super(in);
        this.in = in;
        // over allocate buffer which makes decompression easier
        uncompressedChunk = new byte[maxUncompressedLength + SIZE_OF_LONG];

        byte[] magic = new byte[LZOP_MAGIC.length];
        readInput(magic, 0, magic.length);
        if (!Arrays.equals(magic, LZOP_MAGIC)) {
            throw new IOException("Not an LZOP file");
        }

        byte[] header = new byte[25];
        readInput(header, 0, header.length);
        ByteArrayInputStream headerStream = new ByteArrayInputStream(header);

        // lzop version: ignored
        readBigEndianShort(headerStream);

        // lzo version
        int lzoVersion = readBigEndianShort(headerStream);
        if (lzoVersion > LZO_IMPLEMENTATION_VERSION) {
            throw new IOException(format("Unsupported LZO version 0x%08X", lzoVersion));
        }

        // lzop version of the format
        int lzopCompatibility = readBigEndianShort(headerStream);
        if (lzopCompatibility > LZOP_IMPLEMENTATION_VERSION) {
            throw new IOException(format("Unsupported LZOP version 0x%08X", lzopCompatibility));
        }

        // variant: must be LZO 1X
        int variant = headerStream.read();
        if (variant != LZO_1X_VARIANT) {
            throw new IOException(format("Unsupported LZO variant %s", variant));
        }

        // level: ignored
        headerStream.read();

        // flags: none supported
        int flags = readBigEndianInt(headerStream);
        if (flags != 0) {
            throw new IOException(format("Unsupported LZO flags %s", flags));
        }

        // output file mode: ignored
        readBigEndianInt(headerStream);

        // output file modified time: ignored
        readBigEndianInt(headerStream);

        // output file time zone offset: ignored
        readBigEndianInt(headerStream);

        // output file name: ignored
        int fileNameLength = headerStream.read();
        byte[] fileName = new byte[fileNameLength];
        readInput(fileName, 0, fileName.length);

        // verify header checksum
        int headerChecksumValue = readBigEndianInt(in);

        Checksum headerChecksum = new Adler32();
        headerChecksum.update(header, 0, header.length);
        headerChecksum.update(fileName, 0, fileName.length);
        if (headerChecksumValue != (int) headerChecksum.getValue()) {
            throw new IOException("Invalid header checksum");
        }
    }

    @Override
    public int read()
            throws IOException
    {
        if (finished) {
            return -1;
        }

        while (uncompressedOffset >= uncompressedLength) {
            int compressedLength = bufferCompressedData();
            if (finished) {
                return -1;
            }

            decompress(compressedLength, uncompressedChunk, 0, uncompressedChunk.length);
        }
        return uncompressedChunk[uncompressedOffset++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        if (finished) {
            return -1;
        }

        while (uncompressedOffset >= uncompressedLength) {
            int compressedLength = bufferCompressedData();
            if (finished) {
                return -1;
            }

            // favor writing directly to user buffer to avoid extra copy
            if (length >= uncompressedLength) {
                decompress(compressedLength, output, offset, length);
                uncompressedOffset = uncompressedLength;
                return uncompressedLength;
            }

            decompress(compressedLength, uncompressedChunk, 0, uncompressedChunk.length);
        }
        int size = Math.min(length, uncompressedLength - uncompressedOffset);
        System.arraycopy(uncompressedChunk, uncompressedOffset, output, offset, size);
        uncompressedOffset += size;
        return size;
    }

    @Override
    public void resetState()
            throws IOException
    {
        uncompressedLength = 0;
        uncompressedOffset = 0;
        finished = false;
    }

    private int bufferCompressedData()
            throws IOException
    {
        uncompressedOffset = 0;
        uncompressedLength = readBigEndianInt(in);
        if (uncompressedLength == -1) {
            // LZOP file MUST end with uncompressedLength == 0
            throw new EOFException("encountered EOF while reading block data");
        }
        if (uncompressedLength == 0) {
            finished = true;
            return -1;
        }

        int compressedLength = readBigEndianInt(in);
        if (compressedLength == -1) {
            throw new EOFException("encountered EOF while reading block data");
        }

        return compressedLength;
    }

    private void decompress(int compressedLength, byte[] output, int outputOffset, int outputLength)
            throws IOException
    {
        if (uncompressedLength == compressedLength) {
            readInput(output, outputOffset, compressedLength);
        }
        else {
            if (compressed.length < compressedLength) {
                // over allocate buffer which makes decompression easier
                compressed = new byte[compressedLength + SIZE_OF_LONG];
            }
            readInput(compressed, 0, compressedLength);
            int actualUncompressedLength = decompressor.decompress(compressed, 0, compressedLength, output, outputOffset, outputLength);
            if (actualUncompressedLength != uncompressedLength) {
                throw new IOException("Decompressor did not decompress the entire block");
            }
        }
    }

    private void readInput(byte[] buffer, int offset, int length)
            throws IOException
    {
        while (length > 0) {
            int size = in.read(buffer, offset, length);
            if (size == -1) {
                throw new EOFException("encountered EOF while reading block data");
            }
            offset += size;
            length -= size;
        }
    }

    private static int readBigEndianShort(InputStream in)
            throws IOException
    {
        int b1 = in.read();
        if (b1 < 0) {
            return -1;
        }

        int b2 = in.read();
        // If second byte is negative, the stream it truncated
        if ((b2) < 0) {
            throw new IOException("Stream is truncated");
        }
        return (b1 << 8) + (b2);
    }

    private static int readBigEndianInt(InputStream in)
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
