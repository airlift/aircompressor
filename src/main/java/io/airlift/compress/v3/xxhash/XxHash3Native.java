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

/**
 * XXHash3 hash function with support for both one-shot and streaming hashing.
 * <p>
 * For one-shot hashing, use the static methods:
 * <pre>
 * long hash = XxHash3Native.hash(data);
 * XxHash128 hash128 = XxHash3Native.hash128(data);
 * </pre>
 * <p>
 * For streaming (incremental) hashing, use the factory methods:
 * <pre>
 * try (XxHash3Hasher hasher = XxHash3Native.newHasher()) {
 *     hasher.update(chunk1);
 *     hasher.update(chunk2);
 *     long hash = hasher.digest();
 * }
 *
 * try (XxHash3Hasher128 hasher = XxHash3Native.newHasher128()) {
 *     hasher.update(chunk1);
 *     hasher.update(chunk2);
 *     XxHash128 hash = hasher.digest();
 * }
 * </pre>
 *
 * <h2>Performance</h2>
 * <p>
 * XXHash3 is significantly faster than XXHash64 for large inputs (~2.5x at 16KB+).
 * <p>
 * The 64-bit hash ({@link #hash}) has minimal overhead and is fast at all input sizes.
 * <p>
 * The 128-bit hash ({@link #hash128}) has additional overhead due to FFM struct-return
 * handling. For small inputs (&lt;512 bytes), this overhead is noticeable. At larger sizes
 * (8KB+), the 64-bit and 128-bit variants have similar throughput as hash computation
 * dominates.
 */
public final class XxHash3Native
{
    private static final Cleaner CLEANER = Cleaner.create();
    private static final ValueLayout.OfLong JAVA_LONG_LE_UNALIGNED = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt JAVA_INT_LE_UNALIGNED = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private XxHash3Native() {}

    // ========== Factory methods for streaming ==========

    /**
     * Creates a new streaming hasher for 64-bit hashes with no seed.
     */
    public static XxHash3Hasher newHasher()
    {
        return new Hasher64Impl();
    }

    /**
     * Creates a new streaming hasher for 64-bit hashes with the specified seed.
     */
    public static XxHash3Hasher newHasher(long seed)
    {
        return new Hasher64Impl(seed);
    }

    /**
     * Creates a new streaming hasher for 128-bit hashes with no seed.
     */
    public static XxHash3Hasher128 newHasher128()
    {
        return new Hasher128Impl();
    }

    /**
     * Creates a new streaming hasher for 128-bit hashes with the specified seed.
     */
    public static XxHash3Hasher128 newHasher128(long seed)
    {
        return new Hasher128Impl(seed);
    }

    // ========== Static one-shot methods ==========

    public static boolean isEnabled()
    {
        return XxHash3Bindings.isEnabled();
    }

    // 64-bit hash variants

    public static long hash(long value)
    {
        return hash(value, 0);
    }

    public static long hash(long value, long seed)
    {
        XxHash3Bindings.verifyEnabled();
        byte[] scratch = new byte[8];
        MemorySegment segment = MemorySegment.ofArray(scratch);
        segment.set(JAVA_LONG_LE_UNALIGNED, 0, value);
        return XxHash3Bindings.hash64(segment, 8, seed);
    }

    public static long hash(byte[] input)
    {
        return hash(input, 0, input.length);
    }

    public static long hash(byte[] input, int offset, int length)
    {
        checkFromIndexSize(offset, length, input.length);
        XxHash3Bindings.verifyEnabled();
        MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
        return XxHash3Bindings.hash64(segment, length);
    }

    public static long hash(MemorySegment input)
    {
        XxHash3Bindings.verifyEnabled();
        return XxHash3Bindings.hash64(input, input.byteSize());
    }

    public static long hash(byte[] input, long seed)
    {
        return hash(input, 0, input.length, seed);
    }

    public static long hash(byte[] input, int offset, int length, long seed)
    {
        checkFromIndexSize(offset, length, input.length);
        XxHash3Bindings.verifyEnabled();
        MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
        return XxHash3Bindings.hash64(segment, length, seed);
    }

    public static long hash(MemorySegment input, long seed)
    {
        XxHash3Bindings.verifyEnabled();
        return XxHash3Bindings.hash64(input, input.byteSize(), seed);
    }

    // 128-bit hash variants

    public static XxHash128 hash128(byte[] input)
    {
        return hash128(input, 0, input.length);
    }

    public static XxHash128 hash128(byte[] input, int offset, int length)
    {
        checkFromIndexSize(offset, length, input.length);
        XxHash3Bindings.verifyEnabled();
        MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
        return XxHash3Bindings.hash128(segment, length);
    }

    public static XxHash128 hash128(MemorySegment input)
    {
        XxHash3Bindings.verifyEnabled();
        return XxHash3Bindings.hash128(input, input.byteSize());
    }

    public static XxHash128 hash128(byte[] input, long seed)
    {
        return hash128(input, 0, input.length, seed);
    }

    public static XxHash128 hash128(byte[] input, int offset, int length, long seed)
    {
        checkFromIndexSize(offset, length, input.length);
        XxHash3Bindings.verifyEnabled();
        MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
        return XxHash3Bindings.hash128(segment, length, seed);
    }

    public static XxHash128 hash128(MemorySegment input, long seed)
    {
        XxHash3Bindings.verifyEnabled();
        return XxHash3Bindings.hash128(input, input.byteSize(), seed);
    }

    // ========== Implementation classes ==========

    /**
     * 64-bit streaming hasher implementation.
     */
    private static final class Hasher64Impl
            implements XxHash3Hasher
    {
        private final MemorySegment state;
        private final Cleaner.Cleanable cleanable;
        private final byte[] scratch = new byte[8];
        private boolean closed;

        Hasher64Impl()
        {
            XxHash3Bindings.verifyEnabled();

            NativeResources resources = new NativeResources();
            this.cleanable = CLEANER.register(this, resources);
            Arena arena = resources.arena();

            MemorySegment rawState = XxHash3Bindings.createState();
            if (rawState.equals(MemorySegment.NULL)) {
                cleanable.clean();
                throw new IllegalStateException("Failed to create XXH3 state");
            }
            this.state = rawState.reinterpret(arena, XxHash3Bindings::freeState);
            XxHash3Bindings.reset64(state);
        }

        Hasher64Impl(long seed)
        {
            XxHash3Bindings.verifyEnabled();

            NativeResources resources = new NativeResources();
            this.cleanable = CLEANER.register(this, resources);
            Arena arena = resources.arena();

            MemorySegment rawState = XxHash3Bindings.createState();
            if (rawState.equals(MemorySegment.NULL)) {
                cleanable.clean();
                throw new IllegalStateException("Failed to create XXH3 state");
            }
            this.state = rawState.reinterpret(arena, XxHash3Bindings::freeState);
            XxHash3Bindings.reset64(state, seed);
        }

        @Override
        public XxHash3Hasher update(byte[] input)
        {
            return update(input, 0, input.length);
        }

        @Override
        public XxHash3Hasher update(byte[] input, int offset, int length)
        {
            checkFromIndexSize(offset, length, input.length);
            checkNotClosed();
            MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
            XxHash3Bindings.update64(state, segment, length);
            return this;
        }

        @Override
        public XxHash3Hasher update(MemorySegment input)
        {
            checkNotClosed();
            XxHash3Bindings.update64(state, input, input.byteSize());
            return this;
        }

        @Override
        public XxHash3Hasher updateLE(long value)
        {
            checkNotClosed();
            MemorySegment segment = MemorySegment.ofArray(scratch);
            segment.set(JAVA_LONG_LE_UNALIGNED, 0, value);
            XxHash3Bindings.update64(state, segment, 8);
            return this;
        }

        @Override
        public XxHash3Hasher updateLE(int value)
        {
            checkNotClosed();
            MemorySegment segment = MemorySegment.ofArray(scratch);
            segment.set(JAVA_INT_LE_UNALIGNED, 0, value);
            XxHash3Bindings.update64(state, segment, 4);
            return this;
        }

        @Override
        public long digest()
        {
            checkNotClosed();
            return XxHash3Bindings.digest64(state);
        }

        @Override
        public XxHash3Hasher reset()
        {
            checkNotClosed();
            XxHash3Bindings.reset64(state);
            return this;
        }

        @Override
        public XxHash3Hasher reset(long seed)
        {
            checkNotClosed();
            XxHash3Bindings.reset64(state, seed);
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
    }

    /**
     * 128-bit streaming hasher implementation.
     */
    private static final class Hasher128Impl
            implements XxHash3Hasher128
    {
        private final MemorySegment state;
        private final Cleaner.Cleanable cleanable;
        private final byte[] scratch = new byte[8];
        private boolean closed;

        Hasher128Impl()
        {
            XxHash3Bindings.verifyEnabled();

            NativeResources resources = new NativeResources();
            this.cleanable = CLEANER.register(this, resources);
            Arena arena = resources.arena();

            MemorySegment rawState = XxHash3Bindings.createState();
            if (rawState.equals(MemorySegment.NULL)) {
                cleanable.clean();
                throw new IllegalStateException("Failed to create XXH3 state");
            }
            this.state = rawState.reinterpret(arena, XxHash3Bindings::freeState);
            XxHash3Bindings.reset128(state);
        }

        Hasher128Impl(long seed)
        {
            XxHash3Bindings.verifyEnabled();

            NativeResources resources = new NativeResources();
            this.cleanable = CLEANER.register(this, resources);
            Arena arena = resources.arena();

            MemorySegment rawState = XxHash3Bindings.createState();
            if (rawState.equals(MemorySegment.NULL)) {
                cleanable.clean();
                throw new IllegalStateException("Failed to create XXH3 state");
            }
            this.state = rawState.reinterpret(arena, XxHash3Bindings::freeState);
            XxHash3Bindings.reset128(state, seed);
        }

        @Override
        public XxHash3Hasher128 update(byte[] input)
        {
            return update(input, 0, input.length);
        }

        @Override
        public XxHash3Hasher128 update(byte[] input, int offset, int length)
        {
            checkFromIndexSize(offset, length, input.length);
            checkNotClosed();
            MemorySegment segment = MemorySegment.ofArray(input).asSlice(offset, length);
            XxHash3Bindings.update128(state, segment, length);
            return this;
        }

        @Override
        public XxHash3Hasher128 update(MemorySegment input)
        {
            checkNotClosed();
            XxHash3Bindings.update128(state, input, input.byteSize());
            return this;
        }

        @Override
        public XxHash3Hasher128 updateLE(long value)
        {
            checkNotClosed();
            MemorySegment segment = MemorySegment.ofArray(scratch);
            segment.set(JAVA_LONG_LE_UNALIGNED, 0, value);
            XxHash3Bindings.update128(state, segment, 8);
            return this;
        }

        @Override
        public XxHash3Hasher128 updateLE(int value)
        {
            checkNotClosed();
            MemorySegment segment = MemorySegment.ofArray(scratch);
            segment.set(JAVA_INT_LE_UNALIGNED, 0, value);
            XxHash3Bindings.update128(state, segment, 4);
            return this;
        }

        @Override
        public XxHash128 digest()
        {
            checkNotClosed();
            return XxHash3Bindings.digest128(state);
        }

        @Override
        public XxHash3Hasher128 reset()
        {
            checkNotClosed();
            XxHash3Bindings.reset128(state);
            return this;
        }

        @Override
        public XxHash3Hasher128 reset(long seed)
        {
            checkNotClosed();
            XxHash3Bindings.reset128(state, seed);
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
