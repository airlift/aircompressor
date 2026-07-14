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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies the pure-Java LZ4 decompressor produces byte-for-byte identical output to the native (liblz4) decompressor
 * on the same compressed input, exercising the zero-copy {@code MemorySegment} overload with native segments.
 */
class TestLz4NativeInterop
{
    @BeforeAll
    static void requireNative()
    {
        assumeTrue(Lz4NativeDecompressor.isEnabled(), "native LZ4 is not available on this platform");
    }

    @Test
    void testText()
    {
        assertMatchesNative(generateText(1_000_000));
    }

    @Test
    void testHighlyCompressible()
    {
        byte[] data = new byte[1_000_000];
        // long runs -> long matches, exercises the match-copy paths heavily
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i / 997) & 0x0F);
        }
        assertMatchesNative(data);
    }

    @Test
    void testIncompressible()
    {
        byte[] data = new byte[1_000_000];
        new Random(1).nextBytes(data);
        assertMatchesNative(data);
    }

    @Test
    void testSmall()
    {
        assertMatchesNative("hello hello hello world".getBytes());
        assertMatchesNative(new byte[0]);
        assertMatchesNative(new byte[1]);
    }

    private static void assertMatchesNative(byte[] original)
    {
        Lz4JavaCompressor compressor = new Lz4JavaCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLength = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment inputBuffer = arena.allocate(Math.max(compressedLength, 1));
            MemorySegment.copy(compressed, 0, inputBuffer, ValueLayout.JAVA_BYTE, 0, compressedLength);
            MemorySegment input = inputBuffer.asSlice(0, compressedLength);

            MemorySegment javaOutput = arena.allocate(Math.max(original.length, 1));
            MemorySegment nativeOutput = arena.allocate(Math.max(original.length, 1));

            int javaSize = new Lz4JavaDecompressor().decompress(input, javaOutput.asSlice(0, original.length));
            int nativeSize = new Lz4NativeDecompressor().decompress(input, nativeOutput.asSlice(0, original.length));

            assertThat(javaSize).isEqualTo(original.length);
            assertThat(nativeSize).isEqualTo(original.length);

            byte[] javaBytes = new byte[javaSize];
            byte[] nativeBytes = new byte[nativeSize];
            MemorySegment.copy(javaOutput, ValueLayout.JAVA_BYTE, 0, javaBytes, 0, javaSize);
            MemorySegment.copy(nativeOutput, ValueLayout.JAVA_BYTE, 0, nativeBytes, 0, nativeSize);

            assertThat(javaBytes).isEqualTo(original);
            assertThat(javaBytes).isEqualTo(nativeBytes);
        }
    }

    private static byte[] generateText(int length)
    {
        String[] words = {"the ", "quick ", "brown ", "fox ", "jumps ", "over ", "lazy ", "dog ", "lorem ", "ipsum "};
        Random random = new Random(42);
        StringBuilder builder = new StringBuilder();
        while (builder.length() < length) {
            builder.append(words[random.nextInt(words.length)]);
        }
        return builder.substring(0, length).getBytes();
    }
}
