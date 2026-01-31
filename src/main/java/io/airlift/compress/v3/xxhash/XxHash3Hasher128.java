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
 * Streaming hasher for computing 128-bit XXHash3 hashes incrementally.
 * <p>
 * Example usage:
 * <pre>
 * try (XxHash3Hasher128 hasher = XxHash3.newHasher128()) {
 *     hasher.update(chunk1);
 *     hasher.update(chunk2);
 *     XxHash128 hash = hasher.digest();
 * }
 * </pre>
 */
public interface XxHash3Hasher128
        extends AutoCloseable
{
    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 update(byte[] input);

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 update(byte[] input, int offset, int length);

    /**
     * Updates the hash state with the given input data.
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 update(MemorySegment input);

    /**
     * Updates the hash state with the given long value in little-endian byte order.
     * This is useful for hashing length-prefixed data without manual byte conversion.
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 updateLE(long value);

    /**
     * Updates the hash state with the given int value in little-endian byte order.
     * This is useful for hashing length-prefixed data without manual byte conversion.
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 updateLE(int value);

    /**
     * Computes and returns the 128-bit hash of all data passed to update().
     * The state is not modified, so you can continue to call update() and digest().
     */
    XxHash128 digest();

    /**
     * Resets the hasher to its initial state (no seed).
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 reset();

    /**
     * Resets the hasher with the given seed.
     *
     * @return this hasher for fluent chaining
     */
    XxHash3Hasher128 reset(long seed);

    /**
     * Closes this hasher and releases native resources.
     */
    @Override
    void close();
}
