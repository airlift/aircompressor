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

import java.lang.foreign.MemorySegment;

/**
 * XXHash64 hash function with support for both one-shot and streaming hashing.
 * <p>
 * For one-shot hashing, use the static methods:
 * <pre>
 * long hash = XxHash64Hasher.hash(data);
 * long hash = XxHash64Hasher.hash(data, seed);
 * </pre>
 * <p>
 * For streaming (incremental) hashing, use the factory methods:
 * <pre>
 * try (XxHash64Hasher hasher = XxHash64Hasher.create()) {
 *     hasher.update(chunk1);
 *     hasher.update(chunk2);
 *     long hash = hasher.digest();
 * }
 * </pre>
 */
public sealed interface XxHash64Hasher
        extends AutoCloseable
        permits XxHash64JavaHasher, XxHash64NativeHasher
{
    long DEFAULT_SEED = 0;

    // ========== Static one-shot methods ==========

    static long hash(byte[] input)
    {
        return hash(input, 0, input.length, DEFAULT_SEED);
    }

    static long hash(byte[] input, int offset, int length)
    {
        return hash(input, offset, length, DEFAULT_SEED);
    }

    static long hash(byte[] input, long seed)
    {
        return hash(input, 0, input.length, seed);
    }

    static long hash(byte[] input, int offset, int length, long seed)
    {
        if (XxHash64NativeHasher.isEnabled()) {
            return XxHash64NativeHasher.hash(input, offset, length, seed);
        }
        return XxHash64JavaHasher.hash(input, offset, length, seed);
    }

    static long hash(MemorySegment input)
    {
        return hash(input, DEFAULT_SEED);
    }

    static long hash(MemorySegment input, long seed)
    {
        if (XxHash64NativeHasher.isEnabled()) {
            return XxHash64NativeHasher.hash(input, seed);
        }
        return XxHash64JavaHasher.hash(input, seed);
    }

    // ========== Factory methods for streaming ==========

    static XxHash64Hasher create()
    {
        return create(DEFAULT_SEED);
    }

    static XxHash64Hasher create(long seed)
    {
        if (XxHash64NativeHasher.isEnabled()) {
            return new XxHash64NativeHasher(seed);
        }
        return new XxHash64JavaHasher(seed);
    }

    // ========== Instance methods for streaming ==========

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher update(byte[] input);

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher update(byte[] input, int offset, int length);

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher update(MemorySegment input);

    /**
     * Updates the hash state with the given long value in little-endian byte order.
     * This is useful for hashing length-prefixed data without manual byte conversion.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher updateLE(long value);

    /**
     * Updates the hash state with the given int value in little-endian byte order.
     * This is useful for hashing length-prefixed data without manual byte conversion.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher updateLE(int value);

    /**
     * Computes and returns the 64-bit hash of all data passed to update().
     * The state is not modified, so you can continue to call update() and digest().
     */
    long digest();

    /**
     * Resets the hasher to its initial state with the default seed.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher reset();

    /**
     * Resets the hasher with the given seed.
     *
     * @return this hasher for fluent chaining
     */
    XxHash64Hasher reset(long seed);

    /**
     * Closes this hasher and releases any resources.
     */
    @Override
    void close();
}
