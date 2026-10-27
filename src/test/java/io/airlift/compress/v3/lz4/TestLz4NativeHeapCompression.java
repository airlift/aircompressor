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
import org.junit.jupiter.api.condition.EnabledIf;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static io.airlift.compress.v3.lz4.Lz4RawCompressor.MAX_TABLE_SIZE;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@EnabledIf("io.airlift.compress.v3.lz4.Lz4NativeCompressor#isEnabled")
class TestLz4NativeHeapCompression
{
    @Test
    void testHeapPathMatchesNativeCompression()
    {
        for (int size : new int[] {131071, 131072, 131073, 262144, 4194304}) {
            for (int kind = 0; kind < 10; kind++) {
                byte[] input = data(size, kind);
                for (int acceleration : new int[] {1, 3, 16, Lz4Native.MAX_ACCELERATION}) {
                    Lz4NativeCompressor compressor = new Lz4NativeCompressor(acceleration);
                    byte[] expected = compressNative(input, acceleration);
                    byte[] paddedInput = new byte[input.length + 14];
                    System.arraycopy(input, 0, paddedInput, 7, input.length);
                    byte[] output = new byte[compressor.maxCompressedLength(input.length) + 26];
                    Arrays.fill(output, (byte) 42);
                    int written = compressor.compress(paddedInput, 7, input.length, output, 13, output.length - 26);
                    assertThat(Arrays.copyOfRange(output, 13, 13 + written)).isEqualTo(expected);
                    assertThat(Arrays.copyOf(output, 13)).containsOnly((byte) 42);
                    assertThat(Arrays.copyOfRange(output, 13 + written, output.length)).containsOnly((byte) 42);

                    MemorySegment heapInput = MemorySegment.ofArray(paddedInput).asSlice(7, input.length);
                    MemorySegment heapOutput = MemorySegment.ofArray(output).asSlice(13, output.length - 26);
                    assertThat(compressor.compress(heapInput, heapOutput)).isEqualTo(expected.length);
                    assertThat(heapOutput.asSlice(0, expected.length).toArray(JAVA_BYTE)).isEqualTo(expected);
                    assertThat(compressor.compress(heapInput.asReadOnly(), heapOutput)).isEqualTo(expected.length);
                    assertThat(heapOutput.asSlice(0, expected.length).toArray(JAVA_BYTE)).isEqualTo(expected);

                    byte[] decoded = new byte[input.length];
                    assertThat(new Lz4JavaDecompressor().decompress(expected, 0, expected.length, decoded, 0, decoded.length)).isEqualTo(input.length);
                    assertThat(decoded).isEqualTo(input);
                }
            }
        }
    }

    @Test
    void testSingleMatchTailBoundaries()
    {
        Lz4NativeCompressor compressor = new Lz4NativeCompressor();
        for (int tail = 0; tail < 32; tail++) {
            byte[] input = new byte[131072];
            byte[] suffix = new byte[tail];
            new Random(123 + tail).nextBytes(suffix);
            System.arraycopy(suffix, 0, input, input.length - suffix.length, suffix.length);
            byte[] expected = compressNative(input, 1);
            byte[] output = new byte[compressor.maxCompressedLength(input.length)];
            int written = compressor.compress(input, 0, input.length, output, 0, output.length);
            assertThat(Arrays.copyOf(output, written)).isEqualTo(expected);
        }
    }

    @Test
    void testSimpleProbeDoesNotWriteOnFallback()
    {
        byte[] input = data(262144, 3);
        byte[] output = new byte[Lz4RawCompressor.maxCompressedLength(input.length)];
        Arrays.fill(output, (byte) 42);
        assertThat(Lz4RawCompressor.tryCompressSimple(input, ARRAY_BYTE_BASE_OFFSET, input.length, output, ARRAY_BYTE_BASE_OFFSET, 1, new int[MAX_TABLE_SIZE])).isEqualTo(-1);
        assertThat(output).containsOnly((byte) 42);
    }

    @Test
    void testOverlappingRangesAndSmallOutputKeepNativeBehavior()
    {
        byte[] input = data(262144, 2);
        Lz4NativeCompressor compressor = new Lz4NativeCompressor();
        byte[] expected = compressNative(input, 1);
        byte[] overlapping = new byte[compressor.maxCompressedLength(input.length) + 16];
        System.arraycopy(input, 0, overlapping, 16, input.length);
        assertThat(compressor.compress(overlapping, 16, input.length, overlapping, 0, overlapping.length)).isEqualTo(expected.length);
        assertThat(Arrays.copyOf(overlapping, expected.length)).isEqualTo(expected);

        byte[] smallOutput = new byte[expected.length + 32];
        assertThat(compressor.compress(input, 0, input.length, smallOutput, 0, smallOutput.length)).isEqualTo(expected.length);
        assertThat(Arrays.copyOf(smallOutput, expected.length)).isEqualTo(expected);

        byte[] random = data(262144, 0);
        byte[] tooSmall = new byte[compressor.maxCompressedLength(random.length)];
        Arrays.fill(tooSmall, (byte) 42);
        assertThatThrownBy(() -> compressor.compress(random, 0, random.length, tooSmall, 0, random.length))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Arrays.copyOfRange(tooSmall, random.length, tooSmall.length)).containsOnly((byte) 42);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeInput = arena.allocateFrom(JAVA_BYTE, input);
            MemorySegment nativeOutput = arena.allocate(compressor.maxCompressedLength(input.length));
            assertThat(compressor.compress(nativeInput, nativeOutput)).isEqualTo(expected.length);
            assertThat(nativeOutput.asSlice(0, expected.length).toArray(JAVA_BYTE)).isEqualTo(expected);
        }
    }

    private static byte[] compressNative(byte[] input, int acceleration)
    {
        byte[] output = new byte[Lz4Native.maxCompressedLength(input.length)];
        int written = Lz4Native.compress(MemorySegment.ofArray(input), input.length, MemorySegment.ofArray(output), output.length, acceleration);
        return Arrays.copyOf(output, written);
    }

    private static byte[] data(int size, int kind)
    {
        byte[] input = new byte[size];
        new Random(42).nextBytes(input);
        switch (kind) {
            case 0 -> {}
            case 1 -> Arrays.fill(input, (byte) 0);
            case 2 -> {
                for (int i = 32768; i < size; i++) {
                    input[i] = input[i % 32768];
                }
            }
            case 3 -> {
                for (int i = 0; i < size; i++) {
                    input[i] &= 3;
                }
            }
            case 4 -> Arrays.fill(input, 0, size - 4096, (byte) 0);
            case 5 -> Arrays.fill(input, size - 8192, size, (byte) 0);
            case 6, 7 -> {
                int period = kind == 6 ? 65535 : 65536;
                for (int i = period; i < size; i++) {
                    input[i] = input[i % period];
                }
            }
            case 8 -> {
                Arrays.fill(input, (byte) 0);
                input[size / 2] = 1;
            }
            case 9 -> {
                Arrays.fill(input, (byte) 0);
                input[size / 4] = 1;
            }
            default -> throw new AssertionError();
        }
        return input;
    }
}
