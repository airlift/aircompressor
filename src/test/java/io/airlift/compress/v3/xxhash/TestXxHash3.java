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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TestXxHash3
{
    // Test vectors from xxhash reference implementation
    // https://github.com/Cyan4973/xxHash/blob/v0.8.3/cli/xsum_sanity_check.c

    // Empty input test vectors (from XSUM_XXH3_testdata and XSUM_XXH128_testdata)
    private static final long EMPTY_64 = 0x2D06800538D394C2L;
    // XXH128 struct: first value is low64, second is high64
    private static final XxHash128 EMPTY_128 = new XxHash128(0x6001C324468D497FL, 0x99AA06D3014798D8L);

    // Prime constants from xxhash (used as seeds in sanity tests)
    // PRIME32 must be kept as a long to avoid sign-extension when used as seed
    private static final long PRIME32 = 2654435761L;
    private static final long PRIME64 = 0x9E3779B185EBCA8DL;

    // Sanity buffer pattern from xxhash reference:
    // buffer[i] = (byteGen >> 56); byteGen *= PRIME64;
    private static byte[] createSanityBuffer(int length)
    {
        byte[] buffer = new byte[length];
        long byteGen = PRIME32 & 0xFFFFFFFFL; // Use unsigned value
        for (int i = 0; i < length; i++) {
            buffer[i] = (byte) (byteGen >>> 56);
            byteGen *= PRIME64;
        }
        return buffer;
    }

    @Test
    void testIsEnabled()
    {
        // Just verify that isEnabled() can be called without error
        XxHash3Native.isEnabled();
    }

    @Test
    void testHash64Empty()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] empty = new byte[0];
        assertThat(XxHash3Native.hash(empty)).isEqualTo(EMPTY_64);
        assertThat(XxHash3Native.hash(empty, 0, 0)).isEqualTo(EMPTY_64);
    }

    @Test
    void testHash64EmptyWithSeed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // From XSUM_XXH3_testdata: { 0, PRIME64, 0xA8A6B918B2F0364AULL }
        byte[] empty = new byte[0];
        assertThat(XxHash3Native.hash(empty, PRIME64)).isEqualTo(0xA8A6B918B2F0364AL);
    }

    @Test
    void testHash128Empty()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] empty = new byte[0];
        assertThat(XxHash3Native.hash128(empty)).isEqualTo(EMPTY_128);
        assertThat(XxHash3Native.hash128(empty, 0, 0)).isEqualTo(EMPTY_128);
    }

    @Test
    void testHash128EmptyWithSeed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // From XSUM_XXH128_testdata: { 0, PRIME32, { 0x5444F7869C671AB0ULL, 0x92220AE55E14AB50ULL } }
        byte[] empty = new byte[0];
        XxHash128 expected = new XxHash128(0x5444F7869C671AB0L, 0x92220AE55E14AB50L);
        assertThat(XxHash3Native.hash128(empty, PRIME32)).isEqualTo(expected);
    }

    @Test
    void testHash64SanityBuffer()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Test vectors from XSUM_XXH3_testdata (XXH3_64bits)
        // https://github.com/Cyan4973/xxHash/blob/v0.8.3/cli/xsum_sanity_check.c
        // Note: seeded tests use PRIME64 as seed
        assertSanityHash64(1, 0, 0xC44BDFF4074EECDBL);
        assertSanityHash64(1, PRIME64, 0x032BE332DD766EF8L);
        assertSanityHash64(6, 0, 0x27B56A84CD2D7325L);
        assertSanityHash64(6, PRIME64, 0x84589C116AB59AB9L);
        assertSanityHash64(12, 0, 0xA713DAF0DFBB77E7L);
        assertSanityHash64(12, PRIME64, 0xE7303E1B2336DE0EL);
        assertSanityHash64(24, 0, 0xA3FE70BF9D3510EBL);
        assertSanityHash64(24, PRIME64, 0x850E80FC35BDD690L);
        assertSanityHash64(48, 0, 0x397DA259ECBA1F11L);
        assertSanityHash64(48, PRIME64, 0xADC2CBAA44ACC616L);
        assertSanityHash64(80, 0, 0xBCDEFBBB2C47C90AL);
        assertSanityHash64(80, PRIME64, 0xC6DD0CB699532E73L);
        assertSanityHash64(195, 0, 0xCD94217EE362EC3AL);
        assertSanityHash64(195, PRIME64, 0xBA68003D370CB3D9L);
    }

    private void assertSanityHash64(int length, long seed, long expected)
    {
        byte[] buffer = createSanityBuffer(length);
        if (seed == 0) {
            assertThat(XxHash3Native.hash(buffer))
                    .describedAs("XXH3_64bits with length=%d", length)
                    .isEqualTo(expected);
        }
        else {
            assertThat(XxHash3Native.hash(buffer, seed))
                    .describedAs("XXH3_64bits_withSeed with length=%d, seed=0x%X", length, seed)
                    .isEqualTo(expected);
        }
    }

    @Test
    void testHash128SanityBuffer()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Test vectors from XSUM_XXH128_testdata (XXH3_128bits)
        // https://github.com/Cyan4973/xxHash/blob/v0.8.3/cli/xsum_sanity_check.c
        // Note: seeded tests use PRIME32 as seed (not PRIME64!)
        // Values are {low64, high64}
        assertSanityHash128(1, 0, 0xC44BDFF4074EECDBL, 0xA6CD5E9392000F6AL);
        assertSanityHash128(1, PRIME32, 0xB53D5557E7F76F8DL, 0x89B99554BA22467CL);
        assertSanityHash128(6, 0, 0x3E7039BDDA43CFC6L, 0x082AFE0B8162D12AL);
        assertSanityHash128(6, PRIME32, 0x269D8F70BE98856EL, 0x5A865B5389ABD2B1L);
        assertSanityHash128(12, 0, 0x061A192713F69AD9L, 0x6E3EFD8FC7802B18L);
        assertSanityHash128(12, PRIME32, 0x9BE9F9A67F3C7DFBL, 0xD7E09D518A3405D3L);
        assertSanityHash128(24, 0, 0x1E7044D28B1B901DL, 0x0CE966E4678D3761L);
        assertSanityHash128(24, PRIME32, 0xD7304C54EBAD40A9L, 0x3162026714A6A243L);
        assertSanityHash128(48, 0, 0xF942219AED80F67BL, 0xA002AC4E5478227EL);
        assertSanityHash128(48, PRIME32, 0x7BA3C3E453A1934EL, 0x163ADDE36C072295L);
        assertSanityHash128(81, 0, 0x5E8BAFB9F95FB803L, 0x4952F58181AB0042L);
        assertSanityHash128(81, PRIME32, 0x703FBB3D7A5F755CL, 0x2724EC7ADC750FB6L);
    }

    private void assertSanityHash128(int length, long seed, long expectedLow, long expectedHigh)
    {
        byte[] buffer = createSanityBuffer(length);
        XxHash128 expected = new XxHash128(expectedLow, expectedHigh);
        if (seed == 0) {
            assertThat(XxHash3Native.hash128(buffer))
                    .describedAs("XXH3_128bits with length=%d", length)
                    .isEqualTo(expected);
        }
        else {
            assertThat(XxHash3Native.hash128(buffer, seed))
                    .describedAs("XXH3_128bits_withSeed with length=%d, seed=0x%X", length, seed)
                    .isEqualTo(expected);
        }
    }

    @Test
    void testHash64WithMemorySegment()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        long expectedHash = XxHash3Native.hash(data);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(data.length);
            segment.copyFrom(MemorySegment.ofArray(data));
            assertThat(XxHash3Native.hash(segment)).isEqualTo(expectedHash);
        }
    }

    @Test
    void testHash64WithMemorySegmentAndSeed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        long seed = 42L;
        long expectedHash = XxHash3Native.hash(data, seed);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(data.length);
            segment.copyFrom(MemorySegment.ofArray(data));
            assertThat(XxHash3Native.hash(segment, seed)).isEqualTo(expectedHash);
        }
    }

    @Test
    void testHash128WithMemorySegment()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        XxHash128 expectedHash = XxHash3Native.hash128(data);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(data.length);
            segment.copyFrom(MemorySegment.ofArray(data));
            assertThat(XxHash3Native.hash128(segment)).isEqualTo(expectedHash);
        }
    }

    @Test
    void testHash128WithMemorySegmentAndSeed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        long seed = 42L;
        XxHash128 expectedHash = XxHash3Native.hash128(data, seed);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(data.length);
            segment.copyFrom(MemorySegment.ofArray(data));
            assertThat(XxHash3Native.hash128(segment, seed)).isEqualTo(expectedHash);
        }
    }

    @Test
    void testHash64WithOffset()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[data.length + 10];
        System.arraycopy(data, 0, padded, 5, data.length);

        assertThat(XxHash3Native.hash(padded, 5, data.length)).isEqualTo(XxHash3Native.hash(data));
    }

    @Test
    void testHash128WithOffset()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[data.length + 10];
        System.arraycopy(data, 0, padded, 5, data.length);

        assertThat(XxHash3Native.hash128(padded, 5, data.length)).isEqualTo(XxHash3Native.hash128(data));
    }

    // ========== Streaming tests ==========

    @Test
    void testStreaming64MatchesOneShot()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        long expected = XxHash3Native.hash(data);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreaming128MatchesOneShot()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        XxHash128 expected = XxHash3Native.hash128(data);

        try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128()) {
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingMultipleUpdates64()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] part1 = "Hello, ".getBytes(StandardCharsets.UTF_8);
        byte[] part2 = "World!".getBytes(StandardCharsets.UTF_8);
        byte[] combined = "Hello, World!".getBytes(StandardCharsets.UTF_8);

        long expected = XxHash3Native.hash(combined);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.update(part1);
            hasher.update(part2);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingMultipleUpdates128()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] part1 = "Hello, ".getBytes(StandardCharsets.UTF_8);
        byte[] part2 = "World!".getBytes(StandardCharsets.UTF_8);
        byte[] combined = "Hello, World!".getBytes(StandardCharsets.UTF_8);

        XxHash128 expected = XxHash3Native.hash128(combined);

        try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128()) {
            hasher.update(part1);
            hasher.update(part2);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingWithSeed64()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = createSanityBuffer(195);
        long expected = XxHash3Native.hash(data, PRIME64);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher(PRIME64)) {
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingWithSeed128()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = createSanityBuffer(81);
        XxHash128 expected = XxHash3Native.hash128(data, PRIME32);

        try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128(PRIME32)) {
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);
        }
    }

    @Test
    void testStreamingReset()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data1 = "First data".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "Second data".getBytes(StandardCharsets.UTF_8);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            // First hash
            hasher.update(data1);
            long hash1 = hasher.digest();
            assertThat(hash1).isEqualTo(XxHash3Native.hash(data1));

            // Reset and compute second hash
            hasher.reset();
            hasher.update(data2);
            long hash2 = hasher.digest();
            assertThat(hash2).isEqualTo(XxHash3Native.hash(data2));

            // Hashes should be different
            assertThat(hash1).isNotEqualTo(hash2);
        }
    }

    @Test
    void testStreamingResetWithSeed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Test data".getBytes(StandardCharsets.UTF_8);
        long seed = 12345L;

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            // First hash without seed
            hasher.update(data);
            long hashNoSeed = hasher.digest();

            // Reset with seed
            hasher.reset(seed);
            hasher.update(data);
            long hashWithSeed = hasher.digest();

            // Should match one-shot versions
            assertThat(hashNoSeed).isEqualTo(XxHash3Native.hash(data));
            assertThat(hashWithSeed).isEqualTo(XxHash3Native.hash(data, seed));
            assertThat(hashNoSeed).isNotEqualTo(hashWithSeed);
        }
    }

    @Test
    void testStreamingDigestDoesNotModifyState()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] data = "Test data".getBytes(StandardCharsets.UTF_8);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.update(data);

            // Multiple calls to digest should return the same value
            long hash1 = hasher.digest();
            long hash2 = hasher.digest();
            assertThat(hash1).isEqualTo(hash2);

            // Can continue updating after digest
            hasher.update(data);
            long hash3 = hasher.digest();
            assertThat(hash3).isNotEqualTo(hash1);
        }
    }

    @Test
    void testStreamingEmpty()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            assertThat(hasher.digest()).isEqualTo(EMPTY_64);
        }

        try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128()) {
            assertThat(hasher.digest()).isEqualTo(EMPTY_128);
        }
    }

    @Test
    void testStreamingChunkedMatchesOneShot()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Use a larger buffer to test chunked processing
        byte[] data = createSanityBuffer(2048);
        long expected = XxHash3Native.hash(data);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            // Update in small chunks
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
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        byte[] part1 = "Hello".getBytes(StandardCharsets.UTF_8);
        byte[] part2 = ", ".getBytes(StandardCharsets.UTF_8);
        byte[] part3 = "World!".getBytes(StandardCharsets.UTF_8);

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            long hash = hasher
                    .update(part1)
                    .update(part2)
                    .update(part3)
                    .digest();

            assertThat(hash).isEqualTo(XxHash3Native.hash("Hello, World!".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void testUpdateLELong()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Test that updateLE(long) produces same hash as manually writing bytes in LE order
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}; // LE order

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(XxHash3Native.hash(bytes));
        }

        // Also test 128-bit hasher
        try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(XxHash3Native.hash128(bytes));
        }
    }

    @Test
    void testUpdateLEInt()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Test that updateLE(int) produces same hash as manually writing bytes in LE order
        int value = 0x01020304;
        byte[] bytes = new byte[] {0x04, 0x03, 0x02, 0x01}; // LE order

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(XxHash3Native.hash(bytes));
        }

        // Also test 128-bit hasher
        try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(XxHash3Native.hash128(bytes));
        }
    }

    @Test
    void testUpdateLELengthPrefixed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Simulate hashing length-prefixed data
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        int length = data.length;

        // Build expected hash by manually constructing length prefix + data
        byte[] prefixed = new byte[4 + data.length];
        prefixed[0] = (byte) length;
        prefixed[1] = (byte) (length >> 8);
        prefixed[2] = (byte) (length >> 16);
        prefixed[3] = (byte) (length >> 24);
        System.arraycopy(data, 0, prefixed, 4, data.length);

        // Using updateLE should produce same result
        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.updateLE(length).update(data);
            assertThat(hasher.digest()).isEqualTo(XxHash3Native.hash(prefixed));
        }
    }

    // ========== Single long hash tests ==========

    @Test
    void testHashLong()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // hash(long) should produce the same result as hashing the 8 bytes in LE order
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}; // LE order

        assertThat(XxHash3Native.hash(value)).isEqualTo(XxHash3Native.hash(bytes));
    }

    @Test
    void testHashLongWithSeed()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // hash(long, seed) should produce the same result as hashing the 8 bytes with seed
        long value = 0x0102030405060708L;
        byte[] bytes = new byte[] {0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}; // LE order
        long seed = PRIME64;

        assertThat(XxHash3Native.hash(value, seed)).isEqualTo(XxHash3Native.hash(bytes, seed));
    }

    @Test
    void testHashLongKnownValues()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // Test that different seeds produce different results
        assertThat(XxHash3Native.hash(0L)).isNotEqualTo(XxHash3Native.hash(0L, PRIME64));
        assertThat(XxHash3Native.hash(Long.MAX_VALUE)).isNotEqualTo(XxHash3Native.hash(Long.MAX_VALUE, PRIME64));

        // Test consistency - calling with same input should return same result
        assertThat(XxHash3Native.hash(12345L)).isEqualTo(XxHash3Native.hash(12345L));
        assertThat(XxHash3Native.hash(12345L, PRIME64)).isEqualTo(XxHash3Native.hash(12345L, PRIME64));
    }

    @Test
    void testHashLongMatchesStreaming()
    {
        assumeTrue(XxHash3Native.isEnabled(), "XxHash3 native library not available");

        // hash(long) should produce same result as streaming updateLE
        long value = 0xDEADBEEFCAFEBABEL;

        try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
            hasher.updateLE(value);
            assertThat(hasher.digest()).isEqualTo(XxHash3Native.hash(value));
        }
    }
}
