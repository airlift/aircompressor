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

import java.lang.foreign.Arena;
import java.lang.ref.Cleaner;

/**
 * Holds the arena that owns the native memory of a stream, and frees it when the stream is closed or becomes unreachable.
 * Must not reference the stream it belongs to, or the stream would never become unreachable.
 */
final class ZstdNativeResources
        implements Runnable
{
    private static final Cleaner CLEANER = Cleaner.create();

    // a shared arena allows sequential use from different threads
    private final Arena arena = Arena.ofShared();
    private final Cleaner.Cleanable cleanable;

    ZstdNativeResources(Object owner)
    {
        this.cleanable = CLEANER.register(owner, this);
    }

    Arena arena()
    {
        return arena;
    }

    // frees the arena deterministically; idempotent
    void clean()
    {
        cleanable.clean();
    }

    @Override
    public void run()
    {
        arena.close();
    }
}
