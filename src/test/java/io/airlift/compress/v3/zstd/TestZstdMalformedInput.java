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

import com.google.common.io.Resources;
import io.airlift.compress.v3.MalformedInputException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

// The Java and native decompressors must report invalid input the same way, so callers can rely on MalformedInputException
class TestZstdMalformedInput
{
    private static final byte[] UNCOMPRESSED = new byte[1000];

    @Test
    void testDecompressTruncatedInput()
    {
        byte[] compressed = compress(UNCOMPRESSED);
        byte[] output = new byte[UNCOMPRESSED.length];
        for (ZstdDecompressor decompressor : decompressors()) {
            for (int length = 1; length < compressed.length; length++) {
                int truncatedLength = length;
                assertThatThrownBy(() -> decompressor.decompress(compressed, 0, truncatedLength, output, 0, output.length))
                        .as("%s with %s input bytes", decompressor.getClass().getSimpleName(), truncatedLength)
                        .isInstanceOf(MalformedInputException.class);
            }
        }
    }

    @Test
    void testDecompressCorruptInput()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/offset-before-start.zst"));
        byte[] output = new byte[compressed.length * 10];
        for (ZstdDecompressor decompressor : decompressors()) {
            assertThatThrownBy(() -> decompressor.decompress(compressed, 0, compressed.length, output, 0, output.length))
                    .as(decompressor.getClass().getSimpleName())
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageMatching("Input is corrupted.*|Data corruption detected.*");
        }
    }

    @Test
    void testDecompressBadChecksum()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/with-checksum.zst"));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/zstd/with-checksum"));
        compressed[compressed.length - 1] ^= 1;
        byte[] output = new byte[uncompressed.length];
        for (ZstdDecompressor decompressor : decompressors()) {
            assertThatThrownBy(() -> decompressor.decompress(compressed, 0, compressed.length, output, 0, output.length))
                    .as(decompressor.getClass().getSimpleName())
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageMatching("Bad checksum.*|Restored data doesn't match checksum.*");
        }
    }

    @Test
    void testDecompressOutputTooSmall()
    {
        byte[] compressed = compress(UNCOMPRESSED);
        byte[] output = new byte[UNCOMPRESSED.length - 1];
        for (ZstdDecompressor decompressor : decompressors()) {
            assertThatThrownBy(() -> decompressor.decompress(compressed, 0, compressed.length, output, 0, output.length))
                    .as(decompressor.getClass().getSimpleName())
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageMatching("Output buffer too small.*|Destination buffer is too small.*");
        }
    }

    @Test
    void testGetDecompressedSizeTruncatedHeader()
    {
        byte[] compressed = compress(UNCOMPRESSED);
        for (ZstdDecompressor decompressor : decompressors()) {
            for (int length = 0; length <= 5; length++) {
                int truncatedLength = length;
                assertThatThrownBy(() -> decompressor.getDecompressedSize(compressed, 0, truncatedLength))
                        .as("%s with %s input bytes", decompressor.getClass().getSimpleName(), truncatedLength)
                        .isInstanceOf(MalformedInputException.class)
                        .hasMessageMatching("Not enough input bytes.*|Invalid or truncated frame header.*");
            }
        }
    }

    @Test
    void testGetDecompressedSizeInvalidMagic()
    {
        byte[] compressed = compress(UNCOMPRESSED);
        compressed[0] ^= 1;
        for (ZstdDecompressor decompressor : decompressors()) {
            assertThatThrownBy(() -> decompressor.getDecompressedSize(compressed, 0, compressed.length))
                    .as(decompressor.getClass().getSimpleName())
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageMatching("Invalid magic prefix.*|Invalid or truncated frame header.*");
        }
    }

    private static List<ZstdDecompressor> decompressors()
    {
        return List.of(new ZstdJavaDecompressor(), new ZstdNativeDecompressor());
    }

    private static byte[] compress(byte[] uncompressed)
    {
        ZstdCompressor compressor = new ZstdJavaCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];
        int compressedLength = compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0, compressed.length);
        return Arrays.copyOf(compressed, compressedLength);
    }
}
