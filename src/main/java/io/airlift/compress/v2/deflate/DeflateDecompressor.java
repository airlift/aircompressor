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
package io.airlift.compress.v2.deflate;

import io.airlift.compress.v2.Decompressor;
import io.airlift.compress.v2.MalformedInputException;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeflateDecompressor
        implements Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(input, inputOffset, inputLength);

            int uncompressedLength = 0;
            while (true) {
                uncompressedLength += inflater.inflate(output, outputOffset + uncompressedLength, maxOutputLength - uncompressedLength);
                if (inflater.finished() || uncompressedLength >= maxOutputLength) {
                    break;
                }
                if (inflater.needsInput()) {
                    throw new MalformedInputException(0, format("Premature end of input stream. Input length = %s, uncompressed length = %d", inputLength, uncompressedLength));
                }
            }

            if (!inflater.finished()) {
                throw new MalformedInputException(0, "Output buffer too small");
            }

            return uncompressedLength;
        }
        catch (DataFormatException e) {
            throw new RuntimeException("Invalid compressed stream", e);
        }
        finally {
            inflater.end();
        }
    }

    @Override
    public int decompress(MemorySegment input, MemorySegment output)
            throws MalformedInputException
    {
        ByteBuffer inputByteBuffer = input.asByteBuffer();
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(inputByteBuffer);

            ByteBuffer outputByteBuffer = output.asByteBuffer();
            int uncompressedLength = 0;
            while (true) {
                uncompressedLength += inflater.inflate(outputByteBuffer);
                if (inflater.finished() || uncompressedLength >= output.byteSize()) {
                    break;
                }
                if (inflater.needsInput()) {
                    throw new MalformedInputException(inputByteBuffer.position(), "Premature end of input stream. Input length = " + input.byteSize() + ", uncompressed length = " + uncompressedLength);
                }
            }

            if (!inflater.finished()) {
                throw new MalformedInputException(inputByteBuffer.position(), "Could not decompress all input (output buffer too small?)");
            }

            return uncompressedLength;
        }
        catch (DataFormatException e) {
            throw new RuntimeException("Invalid compressed stream", e);
        }
        finally {
            inflater.end();
        }
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
