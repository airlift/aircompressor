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
package io.airlift.compress.lz4;

import io.airlift.compress.IncompatibleJvmException;
import sun.misc.Unsafe;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;

import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

final class UnsafeUtil
{
    public static final Unsafe UNSAFE;

    private UnsafeUtil() {}

    static {
        ByteOrder order = ByteOrder.nativeOrder();
        if (!order.equals(ByteOrder.LITTLE_ENDIAN)) {
            throw new IncompatibleJvmException(format("LZ4 requires a little endian platform (found %s)", order));
        }

        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
        }
        catch (Exception e) {
            throw new IncompatibleJvmException("LZ4 requires access to sun.misc.Unsafe");
        }
    }

    public static byte[] getBase(MemorySegment segment)
    {
        if (segment.isNative()) {
            return null;
        }
        if (segment.isReadOnly()) {
            throw new IllegalArgumentException("MemorySegment is read-only");
        }
        Object inputBase = segment.heapBase().orElse(null);
        if (!(inputBase instanceof byte[] byteArray)) {
            throw new IllegalArgumentException("MemorySegment is not backed by a byte array");
        }
        return byteArray;
    }

    public static long getAddress(MemorySegment segment)
    {
        if (segment.isNative()) {
            return segment.address();
        }
        return segment.address() + ARRAY_BYTE_BASE_OFFSET;
    }
}
