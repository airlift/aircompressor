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
package io.airlift.compress.deflate;

import io.airlift.compress.Compressor;

import java.lang.foreign.MemorySegment;
import java.util.zip.Deflater;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.zip.Deflater.FULL_FLUSH;

public class DeflateCompressor
        implements Compressor
{
    private static final int EXTRA_COMPRESSION_SPACE = 16;
    private static final int COMPRESSION_LEVEL = 4;

    private final int compressionLevel;

    public DeflateCompressor()
    {
        this(COMPRESSION_LEVEL);
    }

    public DeflateCompressor(int compressionLevel)
    {
        if (compressionLevel < 0 || compressionLevel > 9) {
            throw new IllegalArgumentException("Invalid compression level: %d (must be 0-9)".formatted(compressionLevel));
        }
        this.compressionLevel = compressionLevel;
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        // From Mark Adler's post http://stackoverflow.com/questions/1207877/java-size-of-compression-output-bytearray
        return uncompressedSize + ((uncompressedSize + 7) >> 3) + ((uncompressedSize + 63) >> 6) + 5 + EXTRA_COMPRESSION_SPACE;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        verifyRange(input, inputOffset, inputLength);
        verifyRange(output, outputOffset, maxOutputLength);

        Deflater deflater = new Deflater(compressionLevel, true);
        try {
            deflater.setInput(input, inputOffset, inputLength);
            deflater.finish();

            int compressedDataLength = deflater.deflate(output, outputOffset, maxOutputLength, FULL_FLUSH);
            if (!deflater.finished()) {
                throw new IllegalStateException("Output buffer too small");
            }
            return compressedDataLength;
        }
        finally {
            deflater.end();
        }
    }

    @Override
    public int compress(MemorySegment input, MemorySegment output)
    {
        int maxCompressedLength = maxCompressedLength(toIntExact(input.byteSize()));
        if (output.byteSize() < maxCompressedLength) {
            throw new IllegalArgumentException("Output buffer must be at least " + maxCompressedLength + " bytes");
        }

        Deflater deflater = new Deflater(compressionLevel, true);
        try {
            deflater.setInput(input.asByteBuffer());
            deflater.finish();

            int compressedDataLength = deflater.deflate(output.asByteBuffer(), FULL_FLUSH);
            if (!deflater.finished()) {
                throw new IllegalStateException("maxCompressedLength formula is incorrect, because deflate produced more data");
            }
            return compressedDataLength;
        }
        finally {
            deflater.end();
        }
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("Invalid offset or length (%s, %s) in array of length %s".formatted(offset, length, data.length));
        }
    }
}
