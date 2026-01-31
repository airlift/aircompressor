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

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.assertj.core.api.Assertions.assertThat;

class TestXxHash64
        extends AbstractTestXxHash64
{
    @Override
    protected XxHash64Hasher createHasher()
    {
        return XxHash64Hasher.create();
    }

    @Override
    protected XxHash64Hasher createHasher(long seed)
    {
        return XxHash64Hasher.create(seed);
    }

    @Override
    protected long hash(byte[] input)
    {
        return XxHash64Hasher.hash(input);
    }

    @Override
    protected long hash(byte[] input, long seed)
    {
        return XxHash64Hasher.hash(input, seed);
    }

    @Override
    protected long hash(byte[] input, int offset, int length)
    {
        return XxHash64Hasher.hash(input, offset, length);
    }

    @Override
    protected long hash(MemorySegment input)
    {
        return XxHash64Hasher.hash(input);
    }

    // ========== Java vs Native consistency tests ==========

    @Test
    void testJavaAndNativeProduceSameOneShot()
    {
        byte[] data = createSanityBuffer(1024);
        long javaHash = XxHash64JavaHasher.hash(data, 0, data.length, 0);

        if (XxHash64NativeHasher.isEnabled()) {
            long nativeHash = XxHash64NativeHasher.hash(data, 0, data.length, 0);
            assertThat(nativeHash).isEqualTo(javaHash);
        }
    }

    @Test
    void testJavaAndNativeProduceSameStreaming()
    {
        byte[] data = createSanityBuffer(1024);

        try (XxHash64JavaHasher javaHasher = new XxHash64JavaHasher(0)) {
            javaHasher.update(data);
            long javaHash = javaHasher.digest();

            if (XxHash64NativeHasher.isEnabled()) {
                try (XxHash64NativeHasher nativeHasher = new XxHash64NativeHasher(0)) {
                    nativeHasher.update(data);
                    long nativeHash = nativeHasher.digest();
                    assertThat(nativeHash).isEqualTo(javaHash);
                }
            }
        }
    }
}
