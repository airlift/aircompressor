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
package io.airlift.compress.v2.lzo;

import io.airlift.compress.v2.hadoop.HadoopOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Adler32;

import static io.airlift.compress.v2.lzo.LzoConstants.LZOP_MAGIC;
import static io.airlift.compress.v2.lzo.LzoConstants.LZO_1X_VARIANT;
import static io.airlift.compress.v2.lzo.LzoConstants.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

class LzopHadoopOutputStream
        extends HadoopOutputStream
{
    private static final int LZOP_FILE_VERSION = 0x1010;
    private static final int LZOP_FORMAT_VERSION = 0x0940;
    private static final int LZO_FORMAT_VERSION = 0x2050;
    private static final int LEVEL = 5;

    private final LzoCompressor compressor = new LzoCompressor();

    private final OutputStream out;
    private final byte[] inputBuffer;
    private final int inputMaxSize;
    private int inputOffset;

    private final byte[] outputBuffer;

    public LzopHadoopOutputStream(OutputStream out, int bufferSize)
            throws IOException
    {
        this.out = requireNonNull(out, "out is null");
        inputBuffer = new byte[bufferSize];
        // leave extra space free at end of buffers to make compression (slightly) faster
        inputMaxSize = inputBuffer.length - compressionOverhead(bufferSize);
        outputBuffer = new byte[compressor.maxCompressedLength(inputMaxSize) + SIZE_OF_LONG];

        out.write(LZOP_MAGIC);

        ByteArrayOutputStream headerOut = new ByteArrayOutputStream(25);
        DataOutputStream headerDataOut = new DataOutputStream(headerOut);
        headerDataOut.writeShort(LZOP_FILE_VERSION);
        headerDataOut.writeShort(LZO_FORMAT_VERSION);
        headerDataOut.writeShort(LZOP_FORMAT_VERSION);
        headerDataOut.writeByte(LZO_1X_VARIANT);
        headerDataOut.writeByte(LEVEL);

        // flags (none)
        headerDataOut.writeInt(0);
        // file mode (non-executable regular file)
        headerDataOut.writeInt(0x81a4);
        // modified time (in seconds from epoch)
        headerDataOut.writeInt((int) (System.currentTimeMillis() / 1000));
        // time zone modifier for above, which is UTC so 0
        headerDataOut.writeInt(0);
        // file name length (none)
        headerDataOut.writeByte(0);

        byte[] header = headerOut.toByteArray();
        out.write(header);

        Adler32 headerChecksum = new Adler32();
        headerChecksum.update(header);
        writeBigEndianInt((int) headerChecksum.getValue());
    }

    @Override
    public void write(int b)
            throws IOException
    {
        inputBuffer[inputOffset++] = (byte) b;
        if (inputOffset >= inputMaxSize) {
            writeNextChunk(inputBuffer, 0, this.inputOffset);
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        while (length > 0) {
            int chunkSize = Math.min(length, inputMaxSize - inputOffset);
            // favor writing directly from the user buffer to avoid the extra copy
            if (inputOffset == 0 && length > inputMaxSize) {
                writeNextChunk(buffer, offset, chunkSize);
            }
            else {
                System.arraycopy(buffer, offset, inputBuffer, inputOffset, chunkSize);
                inputOffset += chunkSize;

                if (inputOffset >= inputMaxSize) {
                    writeNextChunk(inputBuffer, 0, inputOffset);
                }
            }
            length -= chunkSize;
            offset += chunkSize;
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        if (inputOffset > 0) {
            writeNextChunk(inputBuffer, 0, this.inputOffset);
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        out.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        try {
            finish();
            writeBigEndianInt(0);
        }
        finally {
            out.close();
        }
    }

    private void writeNextChunk(byte[] input, int inputOffset, int inputLength)
            throws IOException
    {
        int compressedSize = compressor.compress(input, inputOffset, inputLength, outputBuffer, 0, outputBuffer.length);

        writeBigEndianInt(inputLength);
        if (compressedSize < inputLength) {
            writeBigEndianInt(compressedSize);
            out.write(outputBuffer, 0, compressedSize);
        }
        else {
            writeBigEndianInt(inputLength);
            out.write(input, inputOffset, inputLength);
        }

        this.inputOffset = 0;
    }

    private void writeBigEndianInt(int value)
            throws IOException
    {
        out.write(value >>> 24);
        out.write(value >>> 16);
        out.write(value >>> 8);
        out.write(value);
    }

    private static int compressionOverhead(int size)
    {
        return (size / 16) + 64 + 3;
    }
}
