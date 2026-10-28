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
package io.airlift.compress.v3.snappy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIf("isNativeEnabled")
class TestNativeSnappyChunks
{
    static boolean isNativeEnabled()
    {
        return SnappyNativeCompressor.isEnabled();
    }

    @Test
    void testChunksMatchWholeNativeCompression()
    {
        SnappyNativeCompressor compressor = new SnappyNativeCompressor();
        for (int size : new int[] {0, 1, 127, 128, 16383, 16384, 65535, 65536, 65537, 131071, 131072, 131073, 2097151, 2097152, 4194304}) {
            for (boolean compressible : new boolean[] {false, true}) {
                byte[] original = new byte[size];
                new Random(42).nextBytes(original);
                if (compressible) {
                    for (int i = 0; i < original.length; i++) {
                        original[i] &= 3;
                    }
                }
                verifyCompression(compressor, original);
            }
        }
    }

    private static void verifyCompression(SnappyNativeCompressor compressor, byte[] original)
    {
        int inputPadding = 7;
        int outputPadding = 13;
        byte[] input = new byte[original.length + 2 * inputPadding];
        System.arraycopy(original, 0, input, inputPadding, original.length);
        int bound = compressor.maxCompressedLength(original.length);
        byte[] output = new byte[bound + 2 * outputPadding + 31];
        Arrays.fill(output, (byte) 42);
        int written = compressor.compress(input, inputPadding, original.length, output, outputPadding, bound + 31);
        byte[] compressed = Arrays.copyOfRange(output, outputPadding, outputPadding + written);
        assertThat(Arrays.copyOfRange(output, 0, outputPadding)).containsOnly((byte) 42);
        assertThat(Arrays.copyOfRange(output, outputPadding + bound + 31, output.length)).containsOnly((byte) 42);
        assertThat(Arrays.copyOfRange(input, inputPadding, inputPadding + original.length)).isEqualTo(original);

        // Native memory takes the unchanged one-shot path, providing an independent byte-for-byte oracle.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeInput = arena.allocateFrom(JAVA_BYTE, original);
            MemorySegment nativeOutput = arena.allocate(bound);
            int expectedSize = compressor.compress(nativeInput, nativeOutput);
            assertThat(compressed).isEqualTo(nativeOutput.asSlice(0, expectedSize).toArray(JAVA_BYTE));
        }

        byte[] decoded = new byte[original.length + 2 * outputPadding];
        Arrays.fill(decoded, (byte) 42);
        assertThat(new SnappyNativeDecompressor().decompress(output, outputPadding, written, decoded, outputPadding, original.length))
                .isEqualTo(original.length);
        assertThat(Arrays.copyOfRange(decoded, outputPadding, outputPadding + original.length)).isEqualTo(original);
        assertThat(Arrays.copyOfRange(decoded, 0, outputPadding)).containsOnly((byte) 42);
        assertThat(Arrays.copyOfRange(decoded, outputPadding + original.length, decoded.length)).containsOnly((byte) 42);
        Arrays.fill(decoded, (byte) 42);
        assertThat(new SnappyJavaDecompressor().decompress(output, outputPadding, written, decoded, outputPadding, original.length))
                .isEqualTo(original.length);
        assertThat(Arrays.copyOfRange(decoded, outputPadding, outputPadding + original.length)).isEqualTo(original);
        assertThat(Arrays.copyOfRange(decoded, 0, outputPadding)).containsOnly((byte) 42);
        assertThat(Arrays.copyOfRange(decoded, outputPadding + original.length, decoded.length)).containsOnly((byte) 42);
    }

    @Test
    void testReadOnlyHeapInput()
    {
        byte[] input = new byte[131073];
        new Random(456).nextBytes(input);
        SnappyNativeCompressor compressor = new SnappyNativeCompressor();
        byte[] output = new byte[compressor.maxCompressedLength(input.length)];
        int written = compressor.compress(MemorySegment.ofArray(input).asReadOnly(), MemorySegment.ofArray(output));
        byte[] decoded = new byte[input.length];
        assertThat(new SnappyNativeDecompressor().decompress(output, 0, written, decoded, 0, decoded.length)).isEqualTo(input.length);
        assertThat(decoded).isEqualTo(input);
    }

    @Test
    void testSmallOutputRejectedBeforeWriting()
    {
        byte[] input = new byte[65537];
        SnappyNativeCompressor compressor = new SnappyNativeCompressor();
        int bound = compressor.maxCompressedLength(input.length);
        byte[] output = new byte[bound + 20];
        Arrays.fill(output, (byte) 42);
        for (int capacity : new int[] {0, 1, bound - 1}) {
            assertThatThrownBy(() -> compressor.compress(input, 0, input.length, output, 7, capacity))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Output buffer too small");
            assertThat(output).containsOnly((byte) 42);
        }
    }

    @Test
    void testUncompressedLengthVarintBoundaries()
    {
        SnappyNative nativeCodec = new SnappyNative();
        for (long length : new long[] {0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455, 268435456, 2147483647, 2147483648L, 0xFFFF_FFFFL}) {
            byte[] output = new byte[11];
            Arrays.fill(output, (byte) 42);
            MemorySegment header = MemorySegment.ofArray(output).asSlice(3, 5);
            int written = SnappyNative.writeUncompressedLength(header, length);
            assertThat(written).isBetween(1, 5);
            assertThat(nativeCodec.decompressedLength(header.asSlice(0, written), written)).isEqualTo(length);
            assertThat(Arrays.copyOfRange(output, 0, 3)).containsOnly((byte) 42);
            assertThat(Arrays.copyOfRange(output, 3 + written, output.length)).containsOnly((byte) 42);
        }
        for (long length : new long[] {-1, 0x1_0000_0000L}) {
            assertThatThrownBy(() -> SnappyNative.writeUncompressedLength(MemorySegment.ofArray(new byte[5]), length))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
