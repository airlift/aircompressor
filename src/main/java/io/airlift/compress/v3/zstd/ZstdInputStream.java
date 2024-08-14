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
package io.airlift.compress.v3.zstd;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static io.airlift.compress.v3.zstd.Util.checkPositionIndexes;
import static io.airlift.compress.v3.zstd.Util.checkState;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class ZstdInputStream
        extends InputStream
{
    private static final int MIN_BUFFER_SIZE = 4096;

    private final InputStream inputStream;
    private final ZstdIncrementalFrameDecompressor decompressor = new ZstdIncrementalFrameDecompressor();

    private byte[] inputBuffer = new byte[decompressor.getInputRequired()];
    private int inputBufferOffset;
    private int inputBufferLimit;

    private byte[] singleByteOutputBuffer;

    private boolean closed;

    public ZstdInputStream(InputStream inputStream)
    {
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    @Override
    public int read()
            throws IOException
    {
        if (singleByteOutputBuffer == null) {
            singleByteOutputBuffer = new byte[1];
        }
        int readSize = read(singleByteOutputBuffer, 0, 1);
        checkState(readSize != 0, "A zero read size should never be returned");
        if (readSize != 1) {
            return -1;
        }
        return singleByteOutputBuffer[0] & 0xFF;
    }

    @Override
    public int read(final byte[] outputBuffer, final int outputOffset, final int outputLength)
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        if (outputBuffer == null) {
            throw new NullPointerException();
        }
        checkPositionIndexes(outputOffset, outputOffset + outputLength, outputBuffer.length);
        if (outputLength == 0) {
            return 0;
        }

        final int outputLimit = outputOffset + outputLength;
        int outputUsed = 0;
        while (outputUsed < outputLength) {
            boolean enoughInput = fillInputBufferIfNecessary(decompressor.getInputRequired());
            if (!enoughInput) {
                if (decompressor.isAtStoppingPoint()) {
                    return outputUsed > 0 ? outputUsed : -1;
                }
                throw new IOException("Not enough input bytes");
            }

            decompressor.partialDecompress(
                    inputBuffer,
                    inputBufferOffset + ARRAY_BYTE_BASE_OFFSET,
                    inputBufferLimit + ARRAY_BYTE_BASE_OFFSET,
                    outputBuffer,
                    outputOffset + outputUsed,
                    outputLimit);

            inputBufferOffset += decompressor.getInputConsumed();
            outputUsed += decompressor.getOutputBufferUsed();
        }
        return outputUsed;
    }

    private boolean fillInputBufferIfNecessary(int requiredSize)
            throws IOException
    {
        if (inputBufferLimit - inputBufferOffset >= requiredSize) {
            return true;
        }

        // compact existing buffered data to the front of the buffer
        if (inputBufferOffset > 0) {
            int copySize = inputBufferLimit - inputBufferOffset;
            System.arraycopy(inputBuffer, inputBufferOffset, inputBuffer, 0, copySize);
            inputBufferOffset = 0;
            inputBufferLimit = copySize;
        }

        if (inputBuffer.length < requiredSize) {
            inputBuffer = Arrays.copyOf(inputBuffer, max(requiredSize, MIN_BUFFER_SIZE));
        }

        while (inputBufferLimit < inputBuffer.length) {
            int readSize = inputStream.read(inputBuffer, inputBufferLimit, inputBuffer.length - inputBufferLimit);
            if (readSize < 0) {
                break;
            }
            inputBufferLimit += readSize;
        }
        return inputBufferLimit >= requiredSize;
    }

    @Override
    public int available()
            throws IOException
    {
        if (closed) {
            return 0;
        }
        return decompressor.getRequestedOutputSize();
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            inputStream.close();
        }
    }
}
