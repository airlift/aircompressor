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
package io.airlift.compress.v3.xxhash;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

abstract class AbstractTestXxHash64
{
    // Test vectors from xxhash reference implementation
    // https://github.com/Cyan4973/xxHash/blob/v0.8.3/cli/xsum_sanity_check.c

    // Prime constants used as seeds in sanity tests
    protected static final long PRIME32 = 2654435761L;
    protected static final long PRIME64 = 0x9E3779B185EBCA8DL;

    // Abstract factory methods for implementations to override
    protected abstract XxHash64Hasher createHasher();

    protected abstract XxHash64Hasher createHasher(long seed);

    protected abstract long hash(byte[] input);

    protected abstract long hash(byte[] input, long seed);

    protected abstract long hash(byte[] input, int offset, int length);

    protected abstract long hash(MemorySegment input);

    protected abstract long hash(long value);

    protected abstract long hash(long value, long seed);

    // Sanity buffer pattern from xxhash reference
    protected static byte[] createSanityBuffer(int length)
    {
        byte[] buffer = new byte[length];
        long byteGen = PRIME32 & 0xFFFFFFFFL;
        for (int i = 0; i < length; i++) {
            buffer[i] = (byte) (byteGen >>> 56);
            byteGen *= PRIME64;
        }
        return buffer;
    }

    // ========== One-shot tests ==========

    @Test
    void testHash64Empty()
    {
        // From XSUM_XXH64_testdata: { 0, 0, 0xEF46DB3751D8E999ULL }
        byte[] empty = new byte[0];
        assertThat(hash(empty)).isEqualTo(0xEF46DB3751D8E999L);
    }

    @Test
    void testHash64EmptyWithSeed()
    {
        // From XSUM_XXH64_testdata: { 0, PRIME32, 0xAC75FDA2929B17EFULL }
        byte[] empty = new byte[0];
        assertThat(hash(empty, PRIME32)).isEqualTo(0xAC75FDA2929B17EFL);
    }

    @Test
    void testHash64SanityBuffer()
    {
        // Test vectors from XSUM_XXH64_testdata (XXH64)
        // https://github.com/Cyan4973/xxHash/blob/v0.8.3/cli/xsum_sanity_check.c
        assertSanityHash64(1, 0, 0xE934A84ADB052768L);
        assertSanityHash64(1, PRIME32, 0x5014607643A9B4C3L);
        assertSanityHash64(4, 0, 0x9136A0DCA57457EEL);
        assertSanityHash64(14, 0, 0x8282DCC4994E35C8L);
        assertSanityHash64(14, PRIME32, 0xC3BD6BF63DEB6DF0L);
        assertSanityHash64(222, 0, 0xB641AE8CB691C174L);
        assertSanityHash64(222, PRIME32, 0x20CB8AB7AE10C14AL);
    }

    private void assertSanityHash64(int length, long seed, long expected)
    {
        byte[] buffer = createSanityBuffer(length);
        if (seed == 0) {
            assertThat(hash(buffer))
                    .describedAs("XXH64 with length=%d", length)
                    .isEqualTo(expected);
        }
        else {
            assertThat(hash(buffer, seed))
                    .describedAs("XXH64 with length=%d, seed=0x%X", length, seed)
                    .isEqualTo(expected);
        }
    }

    @Test
    void testHash64WithMemorySegment()
    {
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        long expectedHash = hash(data);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(data.length);
            segment.copyFrom(MemorySegment.ofArray(data));
            assertThat(hash(segment)).isEqualTo(expectedHash);
        }
    }

    @Test
    void testHash64WithOffset()
    {
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[data.length + 10];
        System.arraycopy(data, 0, padded, 5, data.length);

        assertThat(hash(padded, 5, data.length)).isEqualTo(hash(data));
    }

    // ========== Streaming tests ==========

    @Test
    void testStreamingMatchesOneShot()
    {
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        long expected = hash(data);

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingMultipleUpdates()
    {
        byte[] part1 = "Hello, ".getBytes(StandardCharsets.UTF_8);
        byte[] part2 = "World!".getBytes(StandardCharsets.UTF_8);
        byte[] combined = "Hello, World!".getBytes(StandardCharsets.UTF_8);

        long expected = hash(combined);

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.update(part1);
            hasher.update(part2);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingWithSeed()
    {
        byte[] data = createSanityBuffer(222);
        long expected = hash(data, PRIME32);

        try (XxHash64Hasher hasher = createHasher(PRIME32)) {
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingReset()
    {
        byte[] data1 = "First data".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "Second data".getBytes(StandardCharsets.UTF_8);

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.update(data1);
            long hash1 = hasher.digest();
            assertThat(hash1).isEqualTo(hash(data1));

            hasher.reset();
            hasher.update(data2);
            long hash2 = hasher.digest();
            assertThat(hash2).isEqualTo(hash(data2));

            assertThat(hash1).isNotEqualTo(hash2);
        }
    }

    @Test
    void testStreamingResetWithSeed()
    {
        byte[] data = "Test data".getBytes(StandardCharsets.UTF_8);
        long seed = 12345L;

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.update(data);
            long hashNoSeed = hasher.digest();

            hasher.reset(seed);
            hasher.update(data);
            long hashWithSeed = hasher.digest();

            assertThat(hashNoSeed).isEqualTo(hash(data));
            assertThat(hashWithSeed).isEqualTo(hash(data, seed));
            assertThat(hashNoSeed).isNotEqualTo(hashWithSeed);
        }
    }

    @Test
    void testStreamingDigestDoesNotModifyState()
    {
        byte[] data = "Test data".getBytes(StandardCharsets.UTF_8);

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.update(data);

            long hash1 = hasher.digest();
            long hash2 = hasher.digest();
            assertThat(hash1).isEqualTo(hash2);

            hasher.update(data);
            long hash3 = hasher.digest();
            assertThat(hash3).isNotEqualTo(hash1);
        }
    }

    @Test
    void testStreamingEmpty()
    {
        try (XxHash64Hasher hasher = createHasher()) {
            assertThat(hasher.digest()).isEqualTo(0xEF46DB3751D8E999L);
        }
    }

    @Test
    void testStreamingChunkedMatchesOneShot()
    {
        byte[] data = createSanityBuffer(2048);
        long expected = hash(data);

        try (XxHash64Hasher hasher = createHasher()) {
            int chunkSize = 100;
            for (int i = 0; i < data.length; i += chunkSize) {
                int len = Math.min(chunkSize, data.length - i);
                hasher.update(data, i, len);
            }
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingFluentApi()
    {
        byte[] part1 = "Hello".getBytes(StandardCharsets.UTF_8);
        byte[] part2 = ", ".getBytes(StandardCharsets.UTF_8);
        byte[] part3 = "World!".getBytes(StandardCharsets.UTF_8);

        try (XxHash64Hasher hasher = createHasher()) {
            long hashResult = hasher
                    .update(part1)
                    .update(part2)
                    .update(part3)
                    .digest();

            assertThat(hashResult).isEqualTo(hash("Hello, World!".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void testUpdateLELong()
    {
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}; // LE order

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(hash(bytes));
        }
    }

    @Test
    void testUpdateLEInt()
    {
        int value = 0x01020304;
        byte[] bytes = new byte[] {0x04, 0x03, 0x02, 0x01}; // LE order

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(hash(bytes));
        }
    }

    @Test
    void testUpdateLELengthPrefixed()
    {
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        int length = data.length;

        // Build expected hash by manually constructing length prefix + data
        byte[] prefixed = new byte[4 + data.length];
        prefixed[0] = (byte) length;
        prefixed[1] = (byte) (length >> 8);
        prefixed[2] = (byte) (length >> 16);
        prefixed[3] = (byte) (length >> 24);
        System.arraycopy(data, 0, prefixed, 4, data.length);

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.updateLE(length).update(data);
            assertThat(hasher.digest()).isEqualTo(hash(prefixed));
        }
    }

    // ========== Single long hash tests ==========

    @Test
    void testHashLong()
    {
        // hash(long) should produce the same result as hashing the 8 bytes in LE order
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}; // LE order

        assertThat(hash(value)).isEqualTo(hash(bytes));
    }

    @Test
    void testHashLongWithSeed()
    {
        // hash(long, seed) should produce the same result as hashing the 8 bytes with seed
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}; // LE order
        long seed = PRIME32;

        assertThat(hash(value, seed)).isEqualTo(hash(bytes, seed));
    }

    @Test
    void testHashLongKnownValues()
    {
        // Test that different seeds produce different results
        assertThat(hash(0L)).isNotEqualTo(hash(0L, PRIME32));
        assertThat(hash(Long.MAX_VALUE)).isNotEqualTo(hash(Long.MAX_VALUE, PRIME32));

        // Test consistency - calling with same input should return same result
        assertThat(hash(12345L)).isEqualTo(hash(12345L));
        assertThat(hash(12345L, PRIME32)).isEqualTo(hash(12345L, PRIME32));
    }

    @Test
    void testHashLongMatchesStreaming()
    {
        // hash(long) should produce same result as streaming updateLE
        long value = 0xDEADBEEFCAFEBABEL;

        try (XxHash64Hasher hasher = createHasher()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(hash(value));
        }
    }
}
