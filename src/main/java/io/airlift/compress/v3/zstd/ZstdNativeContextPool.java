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

import java.lang.foreign.MemorySegment;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * A bounded, process-wide pool of native zstd contexts. Pooling avoids allocating and
 * initializing a context on every call, while keeping native memory independent of the
 * number of compressor or decompressor instances: instances own no native memory, and the
 * pool never retains more than {@code maxSize} idle contexts. Contexts that do not fit
 * in the pool are freed immediately.
 */
final class ZstdNativeContextPool
{
    private static final int MAX_POOL_SIZE = Math.max(2, Runtime.getRuntime().availableProcessors());

    static final ZstdNativeContextPool COMPRESSION = new ZstdNativeContextPool(ZstdNative::createCompressionContext, ZstdNative::freeCompressionContext, MAX_POOL_SIZE);
    static final ZstdNativeContextPool DECOMPRESSION = new ZstdNativeContextPool(ZstdNative::createDecompressionContext, ZstdNative::freeDecompressionContext, MAX_POOL_SIZE);

    private final Supplier<MemorySegment> allocator;
    private final Consumer<MemorySegment> deallocator;
    private final int maxSize;
    private final Queue<MemorySegment> idle = new ConcurrentLinkedQueue<>();
    private final AtomicInteger idleCount = new AtomicInteger();

    ZstdNativeContextPool(Supplier<MemorySegment> allocator, Consumer<MemorySegment> deallocator, int maxSize)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.deallocator = requireNonNull(deallocator, "deallocator is null");
        this.maxSize = maxSize;
    }

    MemorySegment acquire()
    {
        MemorySegment context = idle.poll();
        if (context != null) {
            idleCount.decrementAndGet();
            return context;
        }
        return allocator.get();
    }

    void release(MemorySegment context)
    {
        if (idleCount.incrementAndGet() > maxSize) {
            idleCount.decrementAndGet();
            deallocator.accept(context);
            return;
        }
        idle.add(context);
    }

    int idleCount()
    {
        return idleCount.get();
    }
}
