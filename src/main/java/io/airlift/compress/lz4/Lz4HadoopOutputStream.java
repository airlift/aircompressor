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
package io.airlift.compress.lz4;

import io.airlift.compress.hadoop.HadoopOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static io.airlift.compress.lz4.Lz4Constants.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

class Lz4HadoopOutputStream
        extends HadoopOutputStream
{
    private final Lz4Compressor compressor;

    private final OutputStream out;
    private final byte[] inputBuffer;
    private final int inputMaxSize;
    private int inputOffset;

    private final byte[] outputBuffer;

    public Lz4HadoopOutputStream(Lz4Compressor compressor, OutputStream out, int bufferSize)
    {
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.out = requireNonNull(out, "out is null");
        inputBuffer = new byte[bufferSize];
        // leave extra space free at end of buffers to make compression (slightly) faster
        inputMaxSize = inputBuffer.length - compressionOverhead(bufferSize);
        outputBuffer = new byte[compressor.maxCompressedLength(inputMaxSize) + SIZE_OF_LONG];
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
        writeBigEndianInt(compressedSize);
        out.write(outputBuffer, 0, compressedSize);

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
        return Math.max((int) (size * 0.01), 10);
    }
}
