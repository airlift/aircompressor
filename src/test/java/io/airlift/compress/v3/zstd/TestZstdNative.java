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
import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.MalformedInputException;
import io.airlift.compress.v3.thirdparty.ZstdJniCompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniDecompressor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestZstdNative
        extends AbstractTestZstd
{
    @Override
    protected ZstdCompressor getCompressor()
    {
        return new ZstdNativeCompressor();
    }

    @Override
    protected ZstdDecompressor getDecompressor()
    {
        return new ZstdNativeDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJniDecompressor();
    }

    @Test
    void testDecompressTruncatedInput()
    {
        byte[] input = "payload".repeat(1024).getBytes(UTF_8);
        ZstdCompressor compressor = getCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(input.length)];
        int compressedLength = compressor.compress(input, 0, input.length, compressed, 0, compressed.length);

        for (int length = 1; length < compressedLength; length++) {
            assertMalformedInput(compressed, length, input.length);
        }
    }

    @Test
    void testDecompressInvalidMagic()
    {
        byte[] input = new byte[16];
        assertMalformedInput(input, input.length, 1024);
    }

    @Test
    void testDecompressInvalidChecksum()
            throws IOException
    {
        byte[] compressed = Resources.toByteArray(Resources.getResource("data/zstd/with-checksum.zst"));
        byte[] uncompressed = Resources.toByteArray(Resources.getResource("data/zstd/with-checksum"));
        compressed[compressed.length - 1] ^= 1;
        assertMalformedInput(compressed, compressed.length, uncompressed.length);
    }

    private void assertMalformedInput(byte[] input, int inputLength, int outputLength)
    {
        int inputOffset = 7;
        int outputOffset = 11;
        byte[] paddedInput = new byte[inputOffset + input.length];
        System.arraycopy(input, 0, paddedInput, inputOffset, input.length);
        byte[] output = new byte[outputOffset + outputLength];
        ZstdDecompressor decompressor = getDecompressor();

        assertThatThrownBy(() -> decompressor.decompress(paddedInput, inputOffset, inputLength, output, outputOffset, outputLength))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("offset=0");
        assertThatThrownBy(() -> decompressor.decompress(MemorySegment.ofArray(paddedInput).asSlice(inputOffset, inputLength), MemorySegment.ofArray(output).asSlice(outputOffset)))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageContaining("offset=0");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeInput = arena.allocate(paddedInput.length);
            nativeInput.copyFrom(MemorySegment.ofArray(paddedInput));
            MemorySegment nativeOutput = arena.allocate(output.length);
            assertThatThrownBy(() -> decompressor.decompress(nativeInput.asSlice(inputOffset, inputLength), nativeOutput.asSlice(outputOffset)))
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageContaining("offset=0");
        }
    }

    @Test
    void testGetDecompressedSizeInvalidInput()
    {
        byte[] invalidMagic = new byte[16];
        byte[] truncatedHeader = {40, -75, 47, -3, 32};
        for (byte[] input : new byte[][] {new byte[0], invalidMagic, truncatedHeader}) {
            assertThatThrownBy(() -> getDecompressor().getDecompressedSize(input, 0, input.length))
                    .isInstanceOf(MalformedInputException.class)
                    .hasMessageContaining("offset=0");
        }
    }

    @Test
    void testGetDecompressedSizeUnknown()
    {
        byte[] input = {40, -75, 47, -3, 0, 0, 1, 0, 0};
        assertThat(getDecompressor().getDecompressedSize(input, 0, input.length)).isEqualTo(-1);
    }
}
