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
package io.airlift.compress.v3.zstdFFM;

import io.airlift.compress.v3.IncompatibleJvmException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static java.lang.String.format;

final class FfmUtil
{
    public static final ValueLayout.OfByte JAVA_BYTE = ValueLayout.JAVA_BYTE;
    public static final ValueLayout.OfShort JAVA_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED;
    public static final ValueLayout.OfInt JAVA_INT = ValueLayout.JAVA_INT_UNALIGNED;
    public static final ValueLayout.OfLong JAVA_LONG = ValueLayout.JAVA_LONG_UNALIGNED;

    private FfmUtil() {}

    static {
        ByteOrder order = ByteOrder.nativeOrder();
        if (!order.equals(ByteOrder.LITTLE_ENDIAN)) {
            throw new IncompatibleJvmException(format("Zstandard requires a little endian platform (found %s)", order));
        }
    }

    public static MemorySegment ofArray(byte[] array)
    {
        return MemorySegment.ofArray(array);
    }
}
