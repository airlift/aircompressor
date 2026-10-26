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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.ref.Cleaner;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A bounded cache of native staging allocations, shared across codecs and threads in a class loader.
 */
final class NativeBufferPool
        implements AutoCloseable
{
    private static final Cleaner CLEANER = Cleaner.create();
    private static final MethodHandle CLOSE_ARENA;

    static {
        try {
            CLOSE_ARENA = MethodHandles.publicLookup().findVirtual(Arena.class, "close", MethodType.methodType(void.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long maxCachedBytes;
    private final int maxCachedBuffers;
    private final long maxBufferSize;
    private final Deque<Buffer> cached = new ArrayDeque<>();
    private long cachedBytes;
    private boolean closed;

    NativeBufferPool(long maxCachedBytes, int maxCachedBuffers, long maxBufferSize)
    {
        this.maxCachedBytes = maxCachedBytes;
        this.maxCachedBuffers = maxCachedBuffers;
        this.maxBufferSize = maxBufferSize;
    }

    Buffer borrow(long size)
    {
        if (size < 8192 || size > maxBufferSize || size > maxCachedBytes || maxCachedBuffers == 0) {
            return null;
        }
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("Native buffer pool is closed");
            }
            Buffer best = null;
            for (Buffer buffer : cached) {
                long capacity = buffer.segment().byteSize();
                if (capacity >= size && capacity <= size * 2 && (best == null || capacity < best.segment().byteSize())) {
                    best = buffer;
                }
            }
            if (best != null) {
                cached.remove(best);
                cachedBytes -= best.segment().byteSize();
                return best;
            }
        }
        return new Buffer(size);
    }

    void release(Buffer buffer)
    {
        List<Buffer> discarded = null;
        synchronized (this) {
            long size = buffer.segment().byteSize();
            if (closed || size > maxBufferSize || size > maxCachedBytes || maxCachedBuffers == 0) {
                discarded = List.of(buffer);
            }
            else {
                while (cached.size() >= maxCachedBuffers || cachedBytes + size > maxCachedBytes) {
                    if (discarded == null) {
                        discarded = new ArrayList<>();
                    }
                    Buffer oldest = cached.removeLast();
                    cachedBytes -= oldest.segment().byteSize();
                    discarded.add(oldest);
                }
                cached.addFirst(buffer);
                cachedBytes += size;
            }
        }
        if (discarded != null) {
            discarded.forEach(Buffer::free);
        }
    }

    synchronized long cachedBytes()
    {
        return cachedBytes;
    }

    @Override
    public void close()
    {
        List<Buffer> discarded;
        synchronized (this) {
            closed = true;
            discarded = List.copyOf(cached);
            cached.clear();
            cachedBytes = 0;
        }
        discarded.forEach(Buffer::free);
    }

    static final class Buffer
    {
        private final MemorySegment segment;
        private final Cleaner.Cleanable cleanable;

        private Buffer(long size)
        {
            Arena arena = Arena.ofShared();
            try {
                segment = arena.allocate(size, Long.BYTES);
                // A lambda defined here would retain this class loader through the Cleaner,
                // keeping its static pool and buffers alive. This adapter only references JDK classes.
                Runnable cleanup = MethodHandleProxies.asInterfaceInstance(Runnable.class, CLOSE_ARENA.bindTo(arena));
                cleanable = CLEANER.register(this, cleanup);
            }
            catch (Throwable e) {
                arena.close();
                throw e;
            }
        }

        MemorySegment segment()
        {
            return segment;
        }

        private void free()
        {
            cleanable.clean();
        }
    }
}
