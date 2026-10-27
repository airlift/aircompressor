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

import com.google.common.primitives.Bytes;
import io.airlift.compress.v3.MalformedInputException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIf("io.airlift.compress.v3.zstd.ZstdNative#isEnabled")
class TestZstdOutputStaging
{
    private static final int OUTPUT_CAPACITY = 1024 * 1024;

    @Test
    void testKnownContentSizesIncludingConcatenatedAndSkippableFrames()
    {
        byte[] first = new byte[8192];
        new Random(42).nextBytes(first);
        byte[] second = new byte[333];
        Arrays.fill(second, (byte) 11);
        byte[] skippable = {0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 1, 2, 3};
        assertStaging(compress(first), first.length, first);
        assertStaging(Bytes.concat(skippable, compress(first), compress(new byte[0]), skippable, compress(second)), first.length + second.length, Bytes.concat(first, second));
        assertStaging(Bytes.concat(skippable, compress(new byte[0])), 0, new byte[0]);
    }

    @Test
    void testUnknownContentSizePreservesCapacity()
            throws Exception
    {
        byte[] original = new byte[8192];
        new Random(43).nextBytes(original);
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (ZstdNativeOutputStream out = new ZstdNativeOutputStream(compressed)) {
            out.write(original);
        }
        assertStaging(compressed.toByteArray(), OUTPUT_CAPACITY, original);
        assertStaging(Bytes.concat(compress(new byte[0]), compressed.toByteArray()), OUTPUT_CAPACITY, original);
    }

    @Test
    void testTruncationAndInsufficientOutputStillFail()
    {
        byte[] original = new byte[8192];
        new Random(44).nextBytes(original);
        byte[] compressed = compress(original);
        ZstdNativeDecompressor decompressor = new ZstdNativeDecompressor();
        byte[] output = new byte[OUTPUT_CAPACITY];
        assertThatThrownBy(() -> decompressor.decompress(compressed, 0, compressed.length - 1, output, 0, output.length))
                .isInstanceOf(MalformedInputException.class);
        assertThatThrownBy(() -> decompressor.decompress(compressed, 0, compressed.length, output, 0, original.length - 1))
                .isInstanceOf(MalformedInputException.class);
    }

    private static byte[] compress(byte[] input)
    {
        ZstdNativeCompressor compressor = new ZstdNativeCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(input.length)];
        int written = compressor.compress(input, 0, input.length, compressed, 0, compressed.length);
        return Arrays.copyOf(compressed, written);
    }

    private static void assertStaging(byte[] compressed, long expectedBound, byte[] original)
    {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeInput = arena.allocateFrom(JAVA_BYTE, compressed);
            assertThat(ZstdNative.decompressedOutputBound(nativeInput, compressed.length, OUTPUT_CAPACITY)).isEqualTo(expectedBound);
            ZstdNativeDecompressor decompressor = new ZstdNativeDecompressor();
            byte[] output = new byte[OUTPUT_CAPACITY];
            Arrays.fill(output, (byte) 42);
            assertThat(decompressor.decompress(compressed, 0, compressed.length, output, 0, output.length)).isEqualTo(original.length);
            assertThat(Arrays.copyOf(output, original.length)).isEqualTo(original);
            assertThat(Arrays.copyOfRange(output, original.length, output.length)).containsOnly((byte) 42);

            MemorySegment nativeOutput = arena.allocate(OUTPUT_CAPACITY).fill((byte) 42);
            assertThat(decompressor.decompress(nativeInput, nativeOutput)).isEqualTo(original.length);
            assertThat(nativeOutput.asSlice(0, original.length).toArray(JAVA_BYTE)).isEqualTo(original);
        }
    }
}
