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

import java.lang.foreign.ValueLayout;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Little-endian, unaligned value layouts used for direct {@link java.lang.foreign.MemorySegment}
 * access. Zstd encodes multi-byte fields little-endian and the bit/SWAR logic relies on
 * little-endian interpretation, so these are used regardless of the platform byte order.
 */
final class MemoryAccess
{
    private MemoryAccess() {}

    /**
     * Base offset for segment-relative addressing. Memory is addressed relative to the start of a
     * {@link java.lang.foreign.MemorySegment}, so the base offset is zero. Retained as a named
     * constant so the (previously {@code Unsafe}-based) address arithmetic reads unchanged.
     */
    public static final int ARRAY_BYTE_BASE_OFFSET = 0;

    public static final ValueLayout.OfShort SHORT_LE = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfInt INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    public static final ValueLayout.OfLong LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);
}
