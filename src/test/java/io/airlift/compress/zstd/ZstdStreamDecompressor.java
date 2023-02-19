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
package io.airlift.compress.zstd;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ZstdStreamDecompressor
        implements Decompressor
{
    @Override
    public int decompress(final byte[] input, final int inputOffset, final int inputLength, final byte[] output, final int outputOffset, final int maxOutputLength)
            throws MalformedInputException
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        ZstdInputStream inputStream = new ZstdInputStream(new ByteArrayInputStream(input, inputOffset, inputLength));
        try {
            int readSize = inputStream.read(output, outputOffset, maxOutputLength);
            if (inputStream.read() != -1) {
                throw new RuntimeException("All input was not consumed");
            }
            return readSize == -1 ? 0 : readSize;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void decompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
            throws MalformedInputException
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
