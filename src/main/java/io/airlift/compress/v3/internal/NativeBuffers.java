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
package io.airlift.compress.v3.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Stages heap buffers for noncritical native calls, which must not receive heap addresses.
 * Native buffers are passed through without copying. The arena is allocated only when needed.
 */
public final class NativeBuffers
        implements AutoCloseable
{
    private Arena arena;

    public MemorySegment input(MemorySegment input, long length)
    {
        MemorySegment source = input.asSlice(0, length);
        if (source.isNative()) {
            return source;
        }
        MemorySegment staged = arena().allocate(length, Long.BYTES);
        staged.copyFrom(source);
        return staged;
    }

    public MemorySegment output(MemorySegment output, long length)
    {
        MemorySegment destination = output.asSlice(0, length);
        if (destination.isReadOnly()) {
            throw new IllegalArgumentException("Output segment is read-only");
        }
        if (destination.isNative()) {
            return destination;
        }
        return arena().allocate(length, Long.BYTES);
    }

    public static void copyOutput(MemorySegment staged, MemorySegment output, long written)
    {
        if (!output.isNative()) {
            MemorySegment.copy(staged, 0, output, 0, written);
        }
    }

    public Arena arena()
    {
        if (arena == null) {
            arena = Arena.ofConfined();
        }
        return arena;
    }

    @Override
    public void close()
    {
        if (arena != null) {
            arena.close();
        }
    }
}
