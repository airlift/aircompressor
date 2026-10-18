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

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniCompressor;
import io.airlift.compress.v3.thirdparty.ZstdJniDecompressor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

public class TestZstdNative
        extends AbstractTestZstd
{
    @Override
    protected ZstdCompressor getCompressor()
    {
        return new ZstdNativeCompressor();
    }

    @Override
    protected ZstdDecompressor getDecompressor()
    {
        return new ZstdNativeDecompressor();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJniDecompressor();
    }

    @Test
    public void testConcurrentUseOfSharedInstances()
            throws Exception
    {
        ZstdCompressor compressor = getCompressor();
        ZstdDecompressor decompressor = getDecompressor();

        int threads = 8;
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                long seed = thread;
                futures.add(executor.submit(() -> {
                    Random random = new Random(seed);
                    for (int iteration = 0; iteration < 200; iteration++) {
                        byte[] original = new byte[random.nextInt(1, 64 * 1024)];
                        for (int i = 0; i < original.length; i++) {
                            original[i] = (byte) random.nextInt(16);
                        }
                        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
                        int compressedLength = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

                        byte[] decompressed = new byte[original.length];
                        int decompressedLength = decompressor.decompress(compressed, 0, compressedLength, decompressed, 0, decompressed.length);

                        assertThat(decompressedLength).isEqualTo(original.length);
                        assertThat(decompressed).isEqualTo(original);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }
}
