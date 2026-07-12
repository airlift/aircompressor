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
import java.util.List;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestXxHash32
{
    // lengths around the 16 byte block boundary and the 4 byte tail boundary
    private static final List<Integer> LENGTHS = List.of(0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 100, 200, 1024, 4096, 10000);
    private static final List<Integer> SEEDS = List.of(0, 1, 0x9E3779B1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE);

    private static byte[] data(int length)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) ((i * 31) + 7);
        }
        return data;
    }

    @Test
    void testKnownVectors()
    {
        // reference values from the xxHash specification
        assertThat(XxHash32JavaHasher.hash(new byte[0], 0, 0, 0)).isEqualTo(0x02CC5D05);
        assertThat(XxHash32JavaHasher.hash("abc".getBytes(UTF_8), 0, 3, 0)).isEqualTo(0x32D153FF);
    }

    @Test
    void testJavaMatchesNative()
    {
        if (!XxHash32NativeHasher.isEnabled()) {
            return;
        }

        for (int length : LENGTHS) {
            byte[] data = data(length);
            for (int seed : SEEDS) {
                assertThat(XxHash32NativeHasher.hash(data, 0, length, seed))
                        .describedAs("length=%s seed=%s", length, seed)
                        .isEqualTo(XxHash32JavaHasher.hash(data, 0, length, seed));
            }
        }
    }

    @Test
    void testHeapAndNativeSegmentsMatchByteArray()
    {
        try (Arena arena = Arena.ofConfined()) {
            for (int length : LENGTHS) {
                byte[] data = data(length);
                int expected = XxHash32JavaHasher.hash(data, 0, length, 0);

                MemorySegment heap = MemorySegment.ofArray(data);
                MemorySegment nativeSegment = arena.allocateFrom(JAVA_BYTE, data);

                assertThat(XxHash32JavaHasher.hash(heap, 0)).describedAs("heap, length=%s", length).isEqualTo(expected);
                assertThat(XxHash32JavaHasher.hash(nativeSegment, 0)).describedAs("native, length=%s", length).isEqualTo(expected);
                assertThat(XxHash32Hasher.hash(heap)).describedAs("hasher heap, length=%s", length).isEqualTo(expected);
                assertThat(XxHash32Hasher.hash(nativeSegment)).describedAs("hasher native, length=%s", length).isEqualTo(expected);
            }
        }
    }

    @Test
    void testHashOfSlicedSegmentMatchesRange()
    {
        byte[] data = data(1000);
        MemorySegment segment = MemorySegment.ofArray(data);

        for (int offset : List.of(0, 1, 7, 16, 500)) {
            for (int length : List.of(0, 1, 15, 16, 17, 400)) {
                assertThat(XxHash32Hasher.hash(segment.asSlice(offset, length)))
                        .describedAs("offset=%s length=%s", offset, length)
                        .isEqualTo(XxHash32Hasher.hash(data, offset, length));
            }
        }
    }

    @Test
    void testStreamingMatchesOneShot()
    {
        for (int length : LENGTHS) {
            byte[] data = data(length);
            for (int seed : List.of(0, 42)) {
                int expected = XxHash32JavaHasher.hash(data, 0, length, seed);

                // feed the input in chunks of varying size to exercise the internal buffer
                for (int chunkSize : List.of(1, 3, 8, 16, 17, 64)) {
                    assertThat(digestInChunks(new XxHash32JavaHasher(seed), data, chunkSize))
                            .describedAs("java length=%s seed=%s chunk=%s", length, seed, chunkSize)
                            .isEqualTo(expected);

                    if (XxHash32NativeHasher.isEnabled()) {
                        assertThat(digestInChunks(new XxHash32NativeHasher(seed), data, chunkSize))
                                .describedAs("native length=%s seed=%s chunk=%s", length, seed, chunkSize)
                                .isEqualTo(expected);
                    }
                }
            }
        }
    }

    @Test
    void testStreamingSegmentUpdatesMatchOneShot()
    {
        try (Arena arena = Arena.ofConfined()) {
            for (int length : LENGTHS) {
                byte[] data = data(length);
                for (int seed : List.of(0, 42)) {
                    int expected = XxHash32JavaHasher.hash(data, 0, length, seed);

                    MemorySegment heap = MemorySegment.ofArray(data);
                    MemorySegment nativeSegment = arena.allocateFrom(JAVA_BYTE, data);

                    for (int chunkSize : List.of(1, 3, 8, 16, 17, 64)) {
                        assertThat(digestSegmentInChunks(new XxHash32JavaHasher(seed), heap, chunkSize))
                                .describedAs("java heap length=%s seed=%s chunk=%s", length, seed, chunkSize)
                                .isEqualTo(expected);
                        assertThat(digestSegmentInChunks(new XxHash32JavaHasher(seed), nativeSegment, chunkSize))
                                .describedAs("java native length=%s seed=%s chunk=%s", length, seed, chunkSize)
                                .isEqualTo(expected);

                        if (XxHash32NativeHasher.isEnabled()) {
                            assertThat(digestSegmentInChunks(new XxHash32NativeHasher(seed), heap, chunkSize))
                                    .describedAs("native heap length=%s seed=%s chunk=%s", length, seed, chunkSize)
                                    .isEqualTo(expected);
                            assertThat(digestSegmentInChunks(new XxHash32NativeHasher(seed), nativeSegment, chunkSize))
                                    .describedAs("native native length=%s seed=%s chunk=%s", length, seed, chunkSize)
                                    .isEqualTo(expected);
                        }
                    }
                }
            }
        }
    }

    private static int digestInChunks(XxHash32Hasher hasher, byte[] data, int chunkSize)
    {
        try (hasher) {
            for (int offset = 0; offset < data.length; offset += chunkSize) {
                hasher.update(data, offset, Math.min(chunkSize, data.length - offset));
            }
            return hasher.digest();
        }
    }

    private static int digestSegmentInChunks(XxHash32Hasher hasher, MemorySegment data, int chunkSize)
    {
        try (hasher) {
            for (long offset = 0; offset < data.byteSize(); offset += chunkSize) {
                hasher.update(data.asSlice(offset, Math.min(chunkSize, data.byteSize() - offset)));
            }
            return hasher.digest();
        }
    }

    @Test
    void testReset()
    {
        byte[] data = data(100);
        int expected = XxHash32Hasher.hash(data);

        try (XxHash32Hasher hasher = XxHash32Hasher.create()) {
            hasher.update(data(37));
            hasher.reset();
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(expected);

            hasher.reset(42);
            hasher.update(data);
            assertThat(hasher.digest()).isEqualTo(XxHash32Hasher.hash(data, 42));
        }
    }

    @Test
    void testUpdateLE()
    {
        byte[] expected = new byte[] {0x78, 0x56, 0x34, 0x12};

        try (XxHash32Hasher hasher = XxHash32Hasher.create()) {
            hasher.updateLE(0x12345678);
            assertThat(hasher.digest()).isEqualTo(XxHash32Hasher.hash(expected));
        }
    }
}
