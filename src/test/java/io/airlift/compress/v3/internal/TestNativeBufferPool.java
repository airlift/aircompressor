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

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

class TestNativeBufferPool
{
    @Test
    void testPoolDoesNotPreventClassLoaderUnloading()
            throws Exception
    {
        WeakReference<ClassLoader> loader = populateIsolatedPool();
        for (int i = 0; i < 10 && loader.get() != null; i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertThat(loader.get()).isNull();
    }

    private static WeakReference<ClassLoader> populateIsolatedPool()
            throws Exception
    {
        URL classes = NativeBuffers.class.getProtectionDomain().getCodeSource().getLocation();
        try (URLClassLoader loader = new URLClassLoader(new URL[] {classes}, ClassLoader.getPlatformClassLoader())) {
            Class<?> type = Class.forName(NativeBuffers.class.getName(), true, loader);
            Object buffers = type.getConstructor().newInstance();
            type.getMethod("input", MemorySegment.class, long.class)
                    .invoke(buffers, MemorySegment.ofArray(new byte[8192]), 8192L);
            type.getMethod("close").invoke(buffers);
            return new WeakReference<>(loader);
        }
    }

    @Test
    void testExclusiveBorrowAndReuse()
    {
        try (NativeBufferPool pool = new NativeBufferPool(32768, 2, 32768)) {
            var first = pool.borrow(8192);
            var second = pool.borrow(8192);
            assertThat(first.segment().address()).isNotEqualTo(second.segment().address());
            first.segment().fill((byte) 7);
            pool.release(first);
            var reused = pool.borrow(8192);
            assertThat(reused.segment().address()).isEqualTo(first.segment().address());
            assertThat(reused.segment().get(JAVA_BYTE, 0)).isEqualTo((byte) 7);
            pool.release(reused);
            pool.release(second);
        }
    }

    @Test
    void testBudgetEvictsAndFreesNativeMemory()
    {
        NativeBufferPool pool = new NativeBufferPool(32768, 2, 32768);
        var first = pool.borrow(16384);
        var second = pool.borrow(16384);
        var larger = pool.borrow(32768);
        pool.release(first);
        pool.release(second);
        pool.release(larger);
        assertThat(pool.cachedBytes()).isEqualTo(32768);
        assertThat(first.segment().scope().isAlive()).isFalse();
        assertThat(second.segment().scope().isAlive()).isFalse();
        assertThat(larger.segment().scope().isAlive()).isTrue();
        assertThat(pool.borrow(65536)).isNull();
        assertThat(pool.borrow(8)).isNull();
        pool.close();
        assertThat(larger.segment().scope().isAlive()).isFalse();
    }

    @Test
    void testReturnAfterPoolClosed()
    {
        NativeBufferPool pool = new NativeBufferPool(32768, 2, 32768);
        var buffer = pool.borrow(8192);
        pool.close();
        pool.release(buffer);
        assertThat(buffer.segment().scope().isAlive()).isFalse();
        assertThat(pool.cachedBytes()).isZero();
    }

    @Test
    void testConcurrentBorrowAcrossThreads()
            throws Exception
    {
        try (NativeBufferPool pool = new NativeBufferPool(32768, 2, 32768);
                var executor = Executors.newFixedThreadPool(4)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < 4; thread++) {
                byte value = (byte) thread;
                futures.add(executor.submit(() -> {
                    for (int iteration = 0; iteration < 100; iteration++) {
                        var buffer = pool.borrow(8192);
                        try {
                            buffer.segment().fill(value);
                            Thread.yield();
                            assertThat(buffer.segment().get(JAVA_BYTE, 0)).isEqualTo(value);
                            assertThat(buffer.segment().get(JAVA_BYTE, 8191)).isEqualTo(value);
                        }
                        finally {
                            pool.release(buffer);
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            assertThat(pool.cachedBytes()).isLessThanOrEqualTo(32768);
        }
    }
}
