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
import java.util.Random;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TestNativeHashCallSizes
{
    @Test
    void testHeapAndNativeInputsAcrossCriticalCallLimit()
    {
        assumeTrue(XxHash3Native.isEnabled() && XxHash32NativeHasher.isEnabled() && XxHash64NativeHasher.isEnabled());
        for (int size : new int[] {8191, 8192, 8193, 16384, 16385, 131072}) {
            for (long seed : new long[] {0, 987654321}) {
                byte[] data = new byte[size];
                new Random(42).nextBytes(data);
                try (Arena arena = Arena.ofConfined();
                        XxHash32Hasher hash32 = new XxHash32NativeHasher((int) seed);
                        XxHash64Hasher hash64 = new XxHash64NativeHasher(seed);
                        XxHash3Hasher hash3 = XxHash3Native.newHasher(seed);
                        XxHash3Hasher128 hash128 = XxHash3Native.newHasher128(seed)) {
                    MemorySegment nativeInput = arena.allocateFrom(JAVA_BYTE, data);
                    int expected32 = XxHash32JavaHasher.hash(data, 0, data.length, (int) seed);
                    long expected64 = XxHash64JavaHasher.hash(data, 0, data.length, seed);
                    // Short updates use the critical path and provide an independent check of the long-call path.
                    for (int offset = 0; offset < size; offset += 32) {
                        int length = Math.min(32, size - offset);
                        hash3.update(data, offset, length);
                        hash128.update(data, offset, length);
                    }
                    long expected3 = hash3.digest();
                    XxHash128 expected128 = hash128.digest();
                    assertThat(XxHash32NativeHasher.hash(data, 0, size, (int) seed)).isEqualTo(expected32);
                    assertThat(XxHash32NativeHasher.hash(nativeInput, (int) seed)).isEqualTo(expected32);
                    assertThat(XxHash64NativeHasher.hash(data, 0, size, seed)).isEqualTo(expected64);
                    assertThat(XxHash64NativeHasher.hash(nativeInput, seed)).isEqualTo(expected64);
                    assertThat(XxHash3Native.hash(data, seed)).isEqualTo(expected3);
                    assertThat(XxHash3Native.hash(nativeInput, seed)).isEqualTo(expected3);
                    assertThat(XxHash3Native.hash128(data, seed)).isEqualTo(expected128);
                    assertThat(XxHash3Native.hash128(nativeInput, seed)).isEqualTo(expected128);
                    assertThat(XxHash3Native.hash128(data)).isEqualTo(XxHash3Native.hash128(nativeInput));
                    assertThat(XxHash3Native.hash(data)).isEqualTo(XxHash3Native.hash(nativeInput));

                    for (MemorySegment input : new MemorySegment[] {MemorySegment.ofArray(data), nativeInput}) {
                        assertThat(hash32.reset((int) seed).update(input).digest()).isEqualTo(expected32);
                        assertThat(hash64.reset(seed).update(input).digest()).isEqualTo(expected64);
                        assertThat(hash3.reset(seed).update(input).digest()).isEqualTo(expected3);
                        assertThat(hash128.reset(seed).update(input).digest()).isEqualTo(expected128);
                    }
                }
            }
        }
    }

    @Test
    void testLargeHeapUpdatesWithOffsetsAndBufferedState()
    {
        assumeTrue(XxHash3Native.isEnabled() && XxHash32NativeHasher.isEnabled() && XxHash64NativeHasher.isEnabled());
        // With the 40 bytes of padding below, these cases straddle the 8 MiB staging limit.
        for (int size : new int[] {8193, 16385, 131073, 4194304, 8388567, 8388568, 8388569, 16777216}) {
            for (long seed : new long[] {0, 987654321}) {
                byte[] data = new byte[size + 40];
                new Random(1234).nextBytes(data);
                MemorySegment heap = MemorySegment.ofArray(data).asReadOnly();
                try (Arena arena = Arena.ofConfined();
                        XxHash32Hasher hash32 = new XxHash32NativeHasher((int) seed);
                        XxHash64Hasher hash64 = new XxHash64NativeHasher(seed);
                        XxHash3Hasher hash3 = XxHash3Native.newHasher(seed);
                        XxHash3Hasher128 hash128 = XxHash3Native.newHasher128(seed)) {
                    MemorySegment nativeInput = arena.allocateFrom(JAVA_BYTE, data);
                    int expected32 = XxHash32JavaHasher.hash(data, 0, data.length, (int) seed);
                    long expected64 = XxHash64JavaHasher.hash(data, 0, data.length, seed);
                    long expected3 = XxHash3Native.hash(nativeInput, seed);
                    XxHash128 expected128 = XxHash3Native.hash128(nativeInput, seed);
                    assertThat(XxHash32NativeHasher.hash(data, 0, data.length, (int) seed)).isEqualTo(expected32);
                    assertThat(XxHash64NativeHasher.hash(data, 0, data.length, seed)).isEqualTo(expected64);
                    assertThat(XxHash3Native.hash(data, seed)).isEqualTo(expected3);
                    assertThat(XxHash3Native.hash128(data, seed)).isEqualTo(expected128);

                    // A short prefix leaves pending bytes in native state; the large update must preserve them.
                    MemorySegment prefix = nativeInput.asSlice(0, 13);
                    MemorySegment body = heap.asSlice(13, size);
                    MemorySegment suffix = nativeInput.asSlice(13L + size, 27);
                    assertThat(hash32.update(prefix).update(body).update(suffix).digest()).isEqualTo(expected32);
                    assertThat(hash64.update(prefix).update(body).update(suffix).digest()).isEqualTo(expected64);
                    assertThat(hash3.update(prefix).update(body).update(suffix).digest()).isEqualTo(expected3);
                    assertThat(hash128.update(prefix).update(body).update(suffix).digest()).isEqualTo(expected128);
                    if (seed == 0) {
                        assertThat(XxHash3Native.hash(data)).isEqualTo(expected3);
                        assertThat(XxHash3Native.hash128(data)).isEqualTo(expected128);
                    }
                }
            }
        }
    }
}
