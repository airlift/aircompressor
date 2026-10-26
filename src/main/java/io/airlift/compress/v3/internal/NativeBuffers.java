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
import java.util.ArrayList;
import java.util.List;

import static java.lang.ref.Reference.reachabilityFence;

/**
 * Stages heap buffers for noncritical native calls, which must not receive heap addresses.
 * Native buffers are passed through without copying. Staging allocations are reused through a
 * bounded shared pool; returned segments are borrowed only until this instance is closed.
 */
public final class NativeBuffers
        implements AutoCloseable
{
    // At most 32 MiB is retained for the entire class loader, rather than per codec or thread.
    private static final NativeBufferPool POOL = new NativeBufferPool(32L * 1024 * 1024, 8, 8L * 1024 * 1024);

    private Arena arena;
    private List<NativeBufferPool.Buffer> borrowed;
    private boolean closed;

    public MemorySegment input(MemorySegment input, long length)
    {
        checkOpen();
        MemorySegment source = input.asSlice(0, length);
        if (source.isNative()) {
            return source;
        }
        MemorySegment staged = allocate(length);
        staged.copyFrom(source);
        return staged;
    }

    public MemorySegment output(MemorySegment output, long length)
    {
        checkOpen();
        MemorySegment destination = output.asSlice(0, length);
        if (destination.isReadOnly()) {
            throw new IllegalArgumentException("Output segment is read-only");
        }
        if (destination.isNative()) {
            return destination;
        }
        return allocate(length);
    }

    public static void copyOutput(MemorySegment staged, MemorySegment output, long written)
    {
        if (!output.isNative()) {
            MemorySegment.copy(staged, 0, output, 0, written);
        }
    }

    private MemorySegment allocate(long length)
    {
        if (borrowed == null) {
            borrowed = new ArrayList<>(2);
        }
        NativeBufferPool.Buffer buffer = POOL.borrow(length);
        if (buffer == null) {
            return arena().allocate(length, Long.BYTES);
        }
        borrowed.add(buffer);
        return buffer.segment().asSlice(0, length);
    }

    public Arena arena()
    {
        checkOpen();
        if (arena == null) {
            arena = Arena.ofConfined();
        }
        return arena;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (borrowed != null) {
                borrowed.forEach(POOL::release);
            }
        }
        finally {
            try {
                if (arena != null) {
                    arena.close();
                }
            }
            finally {
                // Pooled allocations have their own cleaners and must remain owned through the call.
                reachabilityFence(this);
            }
        }
    }

    private void checkOpen()
    {
        if (closed) {
            throw new IllegalStateException("Native buffers are closed");
        }
    }
}
