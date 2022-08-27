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

import io.airlift.compress.Compressor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static io.airlift.compress.zstd.Constants.MAX_BLOCK_SIZE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ZstdStreamCompressor
        implements Compressor
{
    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        int result = uncompressedSize + (uncompressedSize >>> 8);

        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 11;
        }

        return result;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(maxOutputLength);
        try (ZstdOutputStream zstdOutputStream = new ZstdOutputStream(byteArrayOutputStream)) {
            zstdOutputStream.write(input, inputOffset, inputLength);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        byte[] compressed = byteArrayOutputStream.toByteArray();
        if (compressed.length > maxOutputLength) {
            throw new IllegalArgumentException("Output buffer too small");
        }
        System.arraycopy(compressed, 0, output, outputOffset, compressed.length);
        return compressed.length;
    }

    @Override
    public void compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer)
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
