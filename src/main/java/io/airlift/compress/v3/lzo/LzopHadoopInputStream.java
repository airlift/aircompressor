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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static io.airlift.compress.v3.lzo.LzoConstants.LZOP_MAGIC;
import static io.airlift.compress.v3.lzo.LzoConstants.LZO_1X_VARIANT;
import static io.airlift.compress.v3.lzo.LzoConstants.MAX_BLOCK_SIZE;
import static io.airlift.compress.v3.lzo.LzoConstants.SIZE_OF_LONG;
import static java.lang.String.format;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class LzopHadoopInputStream
        extends HadoopInputStream
{
    private static final int END_OF_STREAM = -1;
    private static final int BUFFERED = -2;

    private static final int LZO_VERSION_MAX = 0x20A0;
    private static final int LZOP_FILE_VERSION_MIN = 0x0940;
    private static final int LZOP_FORMAT_VERSION_MAX = 0x1010;

    private static final int LZOP_FLAG_ADLER32_DECOMPRESSED = 0x0000_0001;
    private static final int LZOP_FLAG_ADLER32_COMPRESSED = 0x0000_0002;
    private static final int LZOP_FLAG_CRC32_DECOMPRESSED = 0x0000_0100;
    private static final int LZOP_FLAG_CRC32_COMPRESSED = 0x0000_0200;
    private static final int LZOP_FLAG_CRC32_HEADER = 0x0000_1000;
    private static final int LZOP_FLAG_IO_MASK = 0x0000_000c;
    private static final int LZOP_FLAG_OPERATING_SYSTEM_MASK = 0xff00_0000;
    private static final int LZOP_FLAG_CHARACTER_SET_MASK = 0x00f0_0000;

    private final LzoDecompressor decompressor = new LzoDecompressor();
    private final InputStream in;
    private byte[] uncompressedChunk;

    private int uncompressedLength;
    private int uncompressedOffset;

    private boolean finished;
    private boolean failed;

    private byte[] compressed = new byte[0];
    private final boolean adler32Decompressed;
    private final boolean adler32Compressed;
    private final boolean crc32Decompressed;
    private final boolean crc32Compressed;

    public LzopHadoopInputStream(InputStream in, int bufferSize)
            throws IOException
    {
        this.in = requireNonNull(in, "in is null");
        // over allocate buffer which makes decompression easier
        uncompressedChunk = new byte[bufferSize + SIZE_OF_LONG];

        byte[] magic = new byte[LZOP_MAGIC.length];
        readInput(magic, 0, magic.length);
        if (!Arrays.equals(magic, LZOP_MAGIC)) {
            throw new IOException("Not an LZOP file");
        }

        byte[] header = new byte[25];
        readInput(header, 0, header.length);
        ByteArrayInputStream headerStream = new ByteArrayInputStream(header);

        // lzop version: ignored
        int lzopFileVersion = readBigEndianShort(headerStream);
        if (lzopFileVersion < LZOP_FILE_VERSION_MIN) {
            throw new IOException(format("Unsupported LZOP file version 0x%08X", lzopFileVersion));
        }

        // lzo version
        int lzoVersion = readBigEndianShort(headerStream);
        if (lzoVersion > LZO_VERSION_MAX) {
            throw new IOException(format("Unsupported LZO version 0x%08X", lzoVersion));
        }

        // lzop version of the format
        int lzopFormatVersion = readBigEndianShort(headerStream);
        if (lzopFormatVersion > LZOP_FORMAT_VERSION_MAX) {
            throw new IOException(format("Unsupported LZOP format version 0x%08X", lzopFormatVersion));
        }

        // variant: must be LZO 1X
        int variant = headerStream.read();
        if (variant != LZO_1X_VARIANT) {
            throw new IOException(format("Unsupported LZO variant %s", variant));
        }

        // level: ignored
        headerStream.read();

        // flags
        int flags = readBigEndianInt(headerStream);

        // ignore flags about the compression environment
        flags &= ~LZOP_FLAG_IO_MASK;
        flags &= ~LZOP_FLAG_OPERATING_SYSTEM_MASK;
        flags &= ~LZOP_FLAG_CHARACTER_SET_MASK;

        // checksum flags
        adler32Decompressed = (flags & LZOP_FLAG_ADLER32_DECOMPRESSED) != 0;
        adler32Compressed = (flags & LZOP_FLAG_ADLER32_COMPRESSED) != 0;
        crc32Decompressed = (flags & LZOP_FLAG_CRC32_DECOMPRESSED) != 0;
        crc32Compressed = (flags & LZOP_FLAG_CRC32_COMPRESSED) != 0;
        boolean crc32Header = (flags & LZOP_FLAG_CRC32_HEADER) != 0;

        flags &= ~LZOP_FLAG_ADLER32_DECOMPRESSED;
        flags &= ~LZOP_FLAG_ADLER32_COMPRESSED;
        flags &= ~LZOP_FLAG_CRC32_DECOMPRESSED;
        flags &= ~LZOP_FLAG_CRC32_COMPRESSED;
        flags &= ~LZOP_FLAG_CRC32_HEADER;

        // no other flags are supported
        if (flags != 0) {
            throw new IOException(format("Unsupported LZO flags 0x%08X", flags));
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

        Checksum headerChecksum = crc32Header ? new CRC32() : new Adler32();
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
        while (uncompressedOffset >= uncompressedLength) {
            if (readBlock(null, 0, 0) == END_OF_STREAM) {
                return -1;
            }
        }
        return uncompressedChunk[uncompressedOffset++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        checkFromIndexSize(offset, length, output.length);
        if (length == 0) {
            return 0;
        }

        while (uncompressedOffset >= uncompressedLength) {
            int size = readBlock(output, offset, length);
            if (size == END_OF_STREAM) {
                return -1;
            }
            if (size != BUFFERED) {
                return size;
            }
        }
        int size = Math.min(length, uncompressedLength - uncompressedOffset);
        System.arraycopy(uncompressedChunk, uncompressedOffset, output, offset, size);
        uncompressedOffset += size;
        return size;
    }

    @Override
    public void resetState()
    {
        uncompressedLength = 0;
        uncompressedOffset = 0;
        finished = false;
        failed = false;
    }

    @Override
    public void close()
            throws IOException
    {
        in.close();
    }

    // Reads and decodes the next block. Decodes straight into the caller's buffer when it is large enough
    // and returns the decoded size, otherwise decodes into the chunk buffer and returns BUFFERED.
    private int readBlock(byte[] output, int outputOffset, int outputLength)
            throws IOException
    {
        if (failed) {
            throw new IOException("Stream has failed");
        }
        if (finished) {
            return END_OF_STREAM;
        }

        try {
            int compressedLength = bufferCompressedData();
            if (finished) {
                return END_OF_STREAM;
            }

            // favor writing directly to user buffer to avoid extra copy
            if (outputLength >= uncompressedLength) {
                decompress(compressedLength, output, outputOffset, outputLength);
                uncompressedOffset = uncompressedLength;
                return uncompressedLength;
            }

            ensureChunkCapacity(uncompressedLength);
            decompress(compressedLength, uncompressedChunk, 0, uncompressedChunk.length);
            return BUFFERED;
        }
        catch (IOException | RuntimeException e) {
            // the input is left partway through a block, so it cannot be read further
            failed = true;
            uncompressedLength = 0;
            uncompressedOffset = 0;
            throw e;
        }
    }

    private void validateBlockLength(long blockLength)
            throws IOException
    {
        // Lengths above the limit are only accepted when the existing buffer already fits them
        if (blockLength > Integer.MAX_VALUE || (blockLength > MAX_BLOCK_SIZE && blockLength > uncompressedChunk.length - SIZE_OF_LONG)) {
            throw new IOException("Invalid uncompressed block length: " + blockLength);
        }
    }

    private void ensureChunkCapacity(int blockLength)
    {
        // The block header declares the uncompressed size, so do not depend on the configured buffer size
        if (uncompressedChunk.length < blockLength + SIZE_OF_LONG) {
            uncompressedChunk = new byte[blockLength + SIZE_OF_LONG];
        }
    }

    private int bufferCompressedData()
            throws IOException
    {
        long blockLength = readUnsignedInt(in);
        if (blockLength == -1) {
            // LZOP file MUST end with uncompressedLength == 0
            throw new EOFException("encountered EOF while reading block data");
        }
        validateBlockLength(blockLength);
        uncompressedOffset = 0;
        uncompressedLength = (int) blockLength;
        if (uncompressedLength == 0) {
            finished = true;
            return -1;
        }

        int compressedLength = readBigEndianInt(in);
        if (compressedLength == -1) {
            throw new EOFException("encountered EOF while reading block data");
        }
        if (compressedLength < 0 || compressedLength > uncompressedLength) {
            throw new IOException("Invalid compressed block length: " + compressedLength);
        }

        skipChecksums(compressedLength < uncompressedLength);

        return compressedLength;
    }

    private void skipChecksums(boolean compressed)
            throws IOException
    {
        if (adler32Decompressed) {
            readBigEndianInt(in);
        }
        if (crc32Decompressed) {
            readBigEndianInt(in);
        }
        if (compressed && adler32Compressed) {
            readBigEndianInt(in);
        }
        if (compressed && crc32Compressed) {
            readBigEndianInt(in);
        }
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
        return (int) readUnsignedInt(in);
    }

    // Returns the unsigned value, or -1 only at physical end of stream before the first byte
    private static long readUnsignedInt(InputStream in)
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
