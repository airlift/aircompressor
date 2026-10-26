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
package io.airlift.compress.v3.lz4;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TestLz4FrameCompression
{
    @Test
    void testReusedJavaScratchAcrossBlockSizes()
    {
        assertReusedScratch(new Lz4FrameJavaCompressor());
    }

    @Test
    void testReusedNativeScratchAcrossBlockSizes()
    {
        assumeTrue(Lz4FrameNativeCompressor.isEnabled());
        assertReusedScratch(new Lz4FrameNativeCompressor());
    }

    private static void assertReusedScratch(Lz4FrameCompressor compressor)
    {
        Lz4FrameJavaDecompressor decompressor = new Lz4FrameJavaDecompressor();
        Random random = new Random(0);
        for (int inputSize : new int[] {4 * 1024 * 1024, 64 * 1024, 4 * 1024 * 1024, 0}) {
            byte[] input = new byte[inputSize];
            random.nextBytes(input);
            byte[] output = new byte[compressor.maxCompressedLength(input.length)];
            int compressedSize = compressor.compress(input, 0, input.length, output, 0, output.length);
            byte[] decoded = new byte[input.length];
            assertThat(decompressor.decompress(output, 0, compressedSize, decoded, 0, decoded.length)).isEqualTo(input.length);
            assertThat(decoded).isEqualTo(input);
        }
        assertThat(compressor.getRetainedSizeInBytes(0)).isGreaterThanOrEqualTo(4 * 1024 * 1024);
    }

    @Test
    void testDirectJavaCompressionMatchesScratchCompression()
    {
        assertDirectCompression(new Lz4FrameJavaCompressor(), new Lz4FrameJavaCompressor(), new Lz4JavaCompressor().getRetainedSizeInBytes(1024 * 1024));
    }

    @Test
    void testDirectNativeCompressionMatchesScratchCompression()
    {
        assumeTrue(Lz4FrameNativeCompressor.isEnabled());
        assertDirectCompression(new Lz4FrameNativeCompressor(), new Lz4FrameNativeCompressor(), new Lz4NativeCompressor().getRetainedSizeInBytes(1024 * 1024));
    }

    private static void assertDirectCompression(Lz4FrameCompressor directCompressor, Lz4FrameCompressor scratchCompressor, int blockRetainedSize)
    {
        for (boolean compressible : new boolean[] {true, false}) {
            byte[] input = new byte[1024 * 1024];
            if (!compressible) {
                new Random(0).nextBytes(input);
            }
            byte[] directOutput = new byte[new Lz4JavaCompressor().maxCompressedLength(input.length) + 32];
            byte[] scratchOutput = new byte[scratchCompressor.maxCompressedLength(input.length)];
            int directSize = directCompressor.compress(input, 0, input.length, directOutput, 0, directOutput.length);
            int scratchSize = scratchCompressor.compress(input, 0, input.length, scratchOutput, 0, scratchOutput.length);
            assertThat(Arrays.copyOf(directOutput, directSize)).isEqualTo(Arrays.copyOf(scratchOutput, scratchSize));
            assertThat(directCompressor.getRetainedSizeInBytes(input.length)).isEqualTo(blockRetainedSize);
        }
    }
}
