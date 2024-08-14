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
package io.airlift.compress.v3.deflate;

import io.airlift.compress.v3.hadoop.HadoopInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static java.util.Objects.requireNonNull;

class JdkDeflateHadoopInputStream
        extends HadoopInputStream
{
    private final byte[] oneByte = new byte[1];
    private final InputStream input;
    private final Inflater inflater;
    private final byte[] inputBuffer;
    private int inputBufferEnd;
    private boolean closed;

    public JdkDeflateHadoopInputStream(InputStream input, int bufferSize)
    {
        this.input = requireNonNull(input, "input is null");
        this.inflater = new Inflater();
        this.inputBuffer = new byte[bufferSize];
    }

    @Override
    public int read()
            throws IOException
    {
        int length = read(oneByte, 0, 1);
        if (length < 0) {
            return length;
        }
        return oneByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length)
            throws IOException
    {
        if (closed) {
            throw new IOException("Closed");
        }

        while (true) {
            try {
                int outputSize = inflater.inflate(output, offset, length);
                if (outputSize > 0) {
                    return outputSize;
                }
            }
            catch (DataFormatException e) {
                throw new IOException(e);
            }

            // This behavior is defined in Hadoop DecompressorStream which treats
            // the need for a dictionary as the end of the stream.
            // This should be an Exception because the stream is not actually decompressed.
            if (inflater.needsDictionary()) {
                return -1;
            }

            if (inflater.finished()) {
                // current stream block is finished, but there could be another stream after this
                int remainingBytes = inflater.getRemaining();
                if (remainingBytes > 0) {
                    // there is still unprocessed data in the input buffer, so reset and continue processing that data
                    inflater.reset();
                    // after the reset, redeclare the unprocessed data with the decompressor
                    inflater.setInput(inputBuffer, inputBufferEnd - remainingBytes, remainingBytes);
                }
                else {
                    int bufferedBytes = input.read(inputBuffer, 0, inputBuffer.length);
                    if (bufferedBytes < 0) {
                        // normal end of input stream
                        return -1;
                    }
                    inflater.reset();
                    inflater.setInput(inputBuffer, 0, bufferedBytes);
                    inputBufferEnd = bufferedBytes;
                }
            }
            else if (inflater.needsInput()) {
                int bufferedBytes = input.read(inputBuffer, 0, inputBuffer.length);
                if (bufferedBytes < 0) {
                    throw new EOFException("Unexpected end of input stream");
                }
                inflater.setInput(inputBuffer, 0, bufferedBytes);
                inputBufferEnd = bufferedBytes;
            }
        }
    }

    @Override
    public void resetState()
    {
        inflater.reset();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            inflater.end();
            input.close();
        }
    }
}
