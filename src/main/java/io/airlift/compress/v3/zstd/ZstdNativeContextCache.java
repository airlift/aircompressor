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
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.ref.Reference.reachabilityFence;
import static java.util.Objects.requireNonNull;

/**
 * Caches a single native zstd context so that repeated calls do not pay for
 * allocating and initializing a context each time. Concurrent callers that
 * find the cached context in use fall back to a temporary context.
 */
final class ZstdNativeContextCache
{
    private static final Cleaner CLEANER = Cleaner.create();

    private final Supplier<MemorySegment> allocator;
    private final Consumer<MemorySegment> deallocator;
    private final AtomicReference<MemorySegment> cached;

    ZstdNativeContextCache(Supplier<MemorySegment> allocator, Consumer<MemorySegment> deallocator)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.deallocator = requireNonNull(deallocator, "deallocator is null");
        // allocated on first use, so an instance that is never used holds no native memory
        this.cached = new AtomicReference<>();
        CLEANER.register(this, new ContextCleaner(cached, deallocator));
    }

    MemorySegment acquire()
    {
        MemorySegment context = cached.getAndSet(null);
        if (context != null) {
            return context;
        }
        return allocator.get();
    }

    void release(MemorySegment context)
    {
        if (!cached.compareAndSet(null, context)) {
            deallocator.accept(context);
        }
        // keep the cache reachable until the context is back in the slot, so the Cleaner cannot run first and miss it
        reachabilityFence(this);
    }

    private record ContextCleaner(AtomicReference<MemorySegment> cached, Consumer<MemorySegment> deallocator)
            implements Runnable
    {
        @Override
        public void run()
        {
            MemorySegment context = cached.getAndSet(null);
            if (context != null) {
                deallocator.accept(context);
            }
        }
    }
}
