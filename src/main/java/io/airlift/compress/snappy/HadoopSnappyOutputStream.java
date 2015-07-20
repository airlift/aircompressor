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

import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static io.airlift.compress.snappy.SnappyConstants.SIZE_OF_LONG;

class HadoopSnappyOutputStream
        extends CompressionOutputStream
{
    private final SnappyCompressor compressor = new SnappyCompressor();

    private final byte[] inputBuffer;
    private final int inputMaxSize;
    private int inputOffset;

    private final byte[] outputBuffer;

    public HadoopSnappyOutputStream(OutputStream out, int bufferSize)
    {
        super(out);
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
    public void resetState()
            throws IOException
    {
        finish();
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
        return (size / 6) + 32;
    }
}
