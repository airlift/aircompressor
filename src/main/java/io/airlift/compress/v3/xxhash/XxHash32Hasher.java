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
 * XXHash32 hash function with support for both one-shot and streaming hashing.
 * <p>
 * For one-shot hashing, use the static methods:
 * <pre>
 * int hash = XxHash32Hasher.hash(data);
 * int hash = XxHash32Hasher.hash(data, seed);
 * </pre>
 * <p>
 * For streaming (incremental) hashing, use the factory methods:
 * <pre>
 * try (XxHash32Hasher hasher = XxHash32Hasher.create()) {
 *     hasher.update(chunk1);
 *     hasher.update(chunk2);
 *     int hash = hasher.digest();
 * }
 * </pre>
 * <p>
 * Streaming hasher instances are not thread-safe and must not be used concurrently
 * from multiple threads without external synchronization.
 */
public sealed interface XxHash32Hasher
        extends AutoCloseable
        permits XxHash32JavaHasher, XxHash32NativeHasher
{
    int DEFAULT_SEED = 0;

    // ========== Static one-shot methods ==========

    static int hash(byte[] input)
    {
        return hash(input, 0, input.length, DEFAULT_SEED);
    }

    static int hash(byte[] input, int offset, int length)
    {
        return hash(input, offset, length, DEFAULT_SEED);
    }

    static int hash(byte[] input, int seed)
    {
        return hash(input, 0, input.length, seed);
    }

    static int hash(byte[] input, int offset, int length, int seed)
    {
        if (XxHash32NativeHasher.isEnabled()) {
            return XxHash32NativeHasher.hash(input, offset, length, seed);
        }
        return XxHash32JavaHasher.hash(input, offset, length, seed);
    }

    static int hash(MemorySegment input)
    {
        return hash(input, DEFAULT_SEED);
    }

    static int hash(MemorySegment input, int seed)
    {
        if (XxHash32NativeHasher.isEnabled()) {
            return XxHash32NativeHasher.hash(input, seed);
        }
        return XxHash32JavaHasher.hash(input, seed);
    }

    // ========== Factory methods for streaming ==========

    static XxHash32Hasher create()
    {
        return create(DEFAULT_SEED);
    }

    static XxHash32Hasher create(int seed)
    {
        if (XxHash32NativeHasher.isEnabled()) {
            return new XxHash32NativeHasher(seed);
        }
        return new XxHash32JavaHasher(seed);
    }

    // ========== Instance methods for streaming ==========

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash32Hasher update(byte[] input);

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash32Hasher update(byte[] input, int offset, int length);

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash32Hasher update(MemorySegment input);

    /**
     * Updates the hash state with the given int value in little-endian byte order.
     * This is useful for hashing length-prefixed data without manual byte conversion.
     *
     * @return this hasher for fluent chaining
     */
    XxHash32Hasher updateLE(int value);

    /**
     * Computes and returns the 32-bit hash of all data passed to update().
     * The state is not modified, so you can continue to call update() and digest().
     */
    int digest();

    /**
     * Resets the hasher to its initial state with the default seed.
     *
     * @return this hasher for fluent chaining
     */
    XxHash32Hasher reset();

    /**
     * Resets the hasher with the given seed.
     *
     * @return this hasher for fluent chaining
     */
    XxHash32Hasher reset(int seed);

    /**
     * Closes this hasher and releases any resources.
     */
    @Override
    void close();
}
