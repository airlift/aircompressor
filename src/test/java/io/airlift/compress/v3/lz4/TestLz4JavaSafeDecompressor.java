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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the safe ({@code Unsafe}-free) {@link Lz4JavaSafeDecompressor} produces byte-for-byte identical output to
 * the {@code Unsafe}-based {@link Lz4JavaDecompressor}, which serves as the reference/oracle. Exercises both the
 * {@code byte[]} overload and the zero-copy {@link MemorySegment} overload with native segments.
 */
class TestLz4JavaSafeDecompressor
{
    @Test
    void testText()
    {
        assertMatchesOracle(generateText(1_000_000));
    }

    @Test
    void testHighlyCompressible()
    {
        byte[] data = new byte[1_000_000];
        // long runs -> long matches, exercises the match-copy paths heavily
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ((i / 997) & 0x0F);
        }
        assertMatchesOracle(data);
    }

    @Test
    void testIncompressible()
    {
        byte[] data = new byte[1_000_000];
        new Random(1).nextBytes(data);
        assertMatchesOracle(data);
    }

    @Test
    void testSmall()
    {
        assertMatchesOracle("hello hello hello world".getBytes());
        assertMatchesOracle(new byte[0]);
        assertMatchesOracle(new byte[1]);
    }

    @Test
    void testShortcutPaths()
    {
        // periodic data over a sweep of periods drives matches at every small offset (incl. 8..15, which the
        // fast-loop shortcut now handles); the length sweep hits the boundary where the shortcut turns off
        for (int period = 1; period <= 40; period++) {
            byte[] data = new byte[200_000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i % period);
            }
            assertMatchesOracle(data);
        }
        for (int len = 0; len <= 300; len++) {
            assertMatchesOracle("abcdefghijklmnopqrst".repeat(30).substring(0, len).getBytes());
            byte[] r = new byte[len];
            new java.util.Random(len).nextBytes(r);
            assertMatchesOracle(r);
        }
    }

    private static void assertMatchesOracle(byte[] original)
    {
        Lz4JavaCompressor compressor = new Lz4JavaCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLength = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        // byte[] overload: safe vs Unsafe oracle
        byte[] oracleArray = new byte[original.length];
        byte[] safeArray = new byte[original.length];
        int oracleArraySize = new Lz4JavaDecompressor().decompress(compressed, 0, compressedLength, oracleArray, 0, oracleArray.length);
        int safeArraySize = new Lz4JavaSafeDecompressor().decompress(compressed, 0, compressedLength, safeArray, 0, safeArray.length);

        assertThat(safeArraySize).isEqualTo(oracleArraySize);
        assertThat(safeArray).isEqualTo(original);
        assertThat(safeArray).isEqualTo(oracleArray);

        // MemorySegment overload with native segments (zero-copy path)
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment inputBuffer = arena.allocate(Math.max(compressedLength, 1));
            MemorySegment.copy(compressed, 0, inputBuffer, ValueLayout.JAVA_BYTE, 0, compressedLength);
            MemorySegment input = inputBuffer.asSlice(0, compressedLength);

            MemorySegment oracleOutput = arena.allocate(Math.max(original.length, 1));
            MemorySegment safeOutput = arena.allocate(Math.max(original.length, 1));

            int oracleSize = new Lz4JavaDecompressor().decompress(input, oracleOutput.asSlice(0, original.length));
            int safeSize = new Lz4JavaSafeDecompressor().decompress(input, safeOutput.asSlice(0, original.length));

            assertThat(safeSize).isEqualTo(original.length);
            assertThat(oracleSize).isEqualTo(original.length);

            byte[] oracleBytes = new byte[oracleSize];
            byte[] safeBytes = new byte[safeSize];
            MemorySegment.copy(oracleOutput, ValueLayout.JAVA_BYTE, 0, oracleBytes, 0, oracleSize);
            MemorySegment.copy(safeOutput, ValueLayout.JAVA_BYTE, 0, safeBytes, 0, safeSize);

            assertThat(safeBytes).isEqualTo(original);
            assertThat(safeBytes).isEqualTo(oracleBytes);
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
