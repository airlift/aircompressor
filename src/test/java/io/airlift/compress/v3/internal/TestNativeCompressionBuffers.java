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
package io.airlift.compress.v3.internal;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.lz4.Lz4NativeCompressor;
import io.airlift.compress.v3.lz4.Lz4NativeDecompressor;
import io.airlift.compress.v3.snappy.SnappyNativeCompressor;
import io.airlift.compress.v3.snappy.SnappyNativeDecompressor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TestNativeCompressionBuffers
{
    @Test
    void testLz4AcrossCriticalCallLimit()
    {
        assumeTrue(Lz4NativeCompressor.isEnabled());
        verifyBuffers(new Lz4NativeCompressor(), new Lz4NativeDecompressor());
    }

    @Test
    void testSnappyAcrossCriticalCallLimit()
    {
        assumeTrue(SnappyNativeCompressor.isEnabled());
        verifyBuffers(new SnappyNativeCompressor(), new SnappyNativeDecompressor());
    }

    private static void verifyBuffers(Compressor compressor, Decompressor decompressor)
    {
        int padding = 7;
        for (int size : new int[] {65535, 65536, 65537}) {
            byte[] input = new byte[size + 2 * padding];
            new Random(42).nextBytes(input);
            int compressedBound = compressor.maxCompressedLength(size);
            // A small input with a large caller buffer must still bound the native output capacity.
            byte[] compressed = new byte[compressedBound + 2 * padding + 128 * 1024];
            Arrays.fill(compressed, (byte) 42);
            int written = compressor.compress(input, padding, size, compressed, padding, compressed.length - 2 * padding);
            assertThat(Arrays.copyOfRange(compressed, 0, padding)).containsOnly((byte) 42);
            assertThat(Arrays.copyOfRange(compressed, padding + compressedBound, compressed.length)).containsOnly((byte) 42);
            if (size > 65536) {
                assertThat(Arrays.copyOfRange(compressed, padding + written, compressed.length)).containsOnly((byte) 42);
            }

            for (int extraOutput : new int[] {0, 128 * 1024}) {
                byte[] output = new byte[size + 2 * padding + extraOutput];
                Arrays.fill(output, (byte) 42);
                assertThat(decompressor.decompress(compressed, padding, written, output, padding, size + extraOutput)).isEqualTo(size);
                assertThat(Arrays.copyOfRange(output, padding, padding + size))
                        .isEqualTo(Arrays.copyOfRange(input, padding, padding + size));
                assertThat(Arrays.copyOfRange(output, 0, padding)).containsOnly((byte) 42);
                assertThat(Arrays.copyOfRange(output, padding + size, output.length)).containsOnly((byte) 42);
            }
        }
    }
}
