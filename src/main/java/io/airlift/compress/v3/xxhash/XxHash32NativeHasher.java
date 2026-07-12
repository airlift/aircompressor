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
package io.airlift.compress.v3.xxhash;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Cleaner;
import java.nio.ByteOrder;

import static java.util.Objects.checkFromIndexSize;

public final class XxHash32NativeHasher
        implements XxHash32Hasher
{
    private static final Cleaner CLEANER = Cleaner.create();
    private static final ValueLayout.OfInt JAVA_INT_LE_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final MemorySegment state;
    private final Cleaner.Cleanable cleanable;
    private final byte[] scratch = new byte[4];
    private boolean closed;

    public XxHash32NativeHasher(int seed)
    {
        XxHash32Bindings.verifyEnabled();

        NativeResources resources = new NativeResources();
        this.cleanable = CLEANER.register(this, resources);
        Arena arena = resources.arena();

        MemorySegment rawState = XxHash32Bindings.createState();
        if (rawState.equals(MemorySegment.NULL)) {
            cleanable.clean();
            throw new IllegalStateException("Failed to create XXH32 state");
        }
        this.state = rawState.reinterpret(arena, XxHash32Bindings::freeState);
        XxHash32Bindings.reset(state, seed);
    }

    // ========== Static methods ==========

    public static boolean isEnabled()
    {
        return XxHash32Bindings.isEnabled();
    }

    public static int hash(byte[] input, int offset, int length, int seed)
    {
        checkFromIndexSize(offset, length, input.length);
        XxHash32Bindings.verifyEnabled();
        MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
        return XxHash32Bindings.hash(segment, length, seed);
    }

    public static int hash(MemorySegment input, int seed)
    {
        XxHash32Bindings.verifyEnabled();
        return XxHash32Bindings.hash(input, input.byteSize(), seed);
    }

    // ========== Instance streaming methods ==========

    @Override
    public XxHash32Hasher update(byte[] input)
    {
        return update(input, 0, input.length);
    }

    @Override
    public XxHash32Hasher update(byte[] input, int offset, int length)
    {
        checkFromIndexSize(offset, length, input.length);
        checkNotClosed();
        MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
        XxHash32Bindings.update(state, segment, length);
        return this;
    }

    @Override
    public XxHash32Hasher update(MemorySegment input)
    {
        checkNotClosed();
        XxHash32Bindings.update(state, input, input.byteSize());
        return this;
    }

    @Override
    public XxHash32Hasher updateLE(int value)
    {
        checkNotClosed();
        MemorySegment segment = MemorySegment.ofArray(scratch);
        segment.set(JAVA_INT_LE_UNALIGNED, 0, value);
        XxHash32Bindings.update(state, segment, 4);
        return this;
    }

    @Override
    public int digest()
    {
        checkNotClosed();
        return XxHash32Bindings.digest(state);
    }

    @Override
    public XxHash32Hasher reset()
    {
        return reset(DEFAULT_SEED);
    }

    @Override
    public XxHash32Hasher reset(int seed)
    {
        checkNotClosed();
        XxHash32Bindings.reset(state, seed);
        return this;
    }

    private void checkNotClosed()
    {
        if (closed) {
            throw new IllegalStateException("Hasher has been closed");
        }
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            cleanable.clean();
        }
    }

    /**
     * Holds native resources that must be freed.
     * Implements Runnable so it can be used with Cleaner.
     */
    private record NativeResources(Arena arena)
            implements Runnable
    {
        NativeResources()
        {
            this(Arena.ofShared());
        }

        @Override
        public void run()
        {
            arena.close();
        }
    }
}
