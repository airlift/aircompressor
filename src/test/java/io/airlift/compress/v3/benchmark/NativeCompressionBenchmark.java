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
package io.airlift.compress.v3.benchmark;

import io.airlift.compress.v3.Compressor;
import io.airlift.compress.v3.Decompressor;
import io.airlift.compress.v3.lz4.Lz4NativeCompressor;
import io.airlift.compress.v3.lz4.Lz4NativeDecompressor;
import io.airlift.compress.v3.snappy.SnappyNativeCompressor;
import io.airlift.compress.v3.snappy.SnappyNativeDecompressor;
import io.airlift.compress.v3.zstd.ZstdNativeCompressor;
import io.airlift.compress.v3.zstd.ZstdNativeDecompressor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class NativeCompressionBenchmark
{
    @Param({"lz4", "snappy", "zstd"})
    private String algorithm;

    @Param({"65536", "65537", "4194304"})
    private int blockSize;

    @Param({"false", "true"})
    private boolean nativeBuffers;

    private Compressor compressor;
    private Decompressor decompressor;
    private byte[] input;
    private byte[] compressed;
    private byte[] compressTarget;
    private byte[] decompressTarget;
    private Arena arena;
    private MemorySegment nativeInput;
    private MemorySegment nativeCompressed;
    private MemorySegment nativeCompressTarget;
    private MemorySegment nativeDecompressTarget;

    @Setup
    public void setup()
    {
        compressor = switch (algorithm) {
            case "lz4" -> new Lz4NativeCompressor();
            case "snappy" -> new SnappyNativeCompressor();
            case "zstd" -> new ZstdNativeCompressor();
            default -> throw new IllegalArgumentException(algorithm);
        };
        decompressor = switch (algorithm) {
            case "lz4" -> new Lz4NativeDecompressor();
            case "snappy" -> new SnappyNativeDecompressor();
            case "zstd" -> new ZstdNativeDecompressor();
            default -> throw new IllegalArgumentException(algorithm);
        };
        input = new byte[blockSize];
        new Random(42).nextBytes(input);
        compressTarget = new byte[compressor.maxCompressedLength(blockSize)];
        int compressedSize = compressor.compress(input, 0, input.length, compressTarget, 0, compressTarget.length);
        compressed = Arrays.copyOf(compressTarget, compressedSize);
        decompressTarget = new byte[blockSize];
        if (nativeBuffers) {
            arena = Arena.ofConfined();
            nativeInput = arena.allocateFrom(JAVA_BYTE, input);
            nativeCompressed = arena.allocateFrom(JAVA_BYTE, compressed);
            nativeCompressTarget = arena.allocate(compressTarget.length);
            nativeDecompressTarget = arena.allocate(blockSize);
        }
    }

    @Benchmark
    public int compress(BytesCounter counter)
    {
        counter.bytes += blockSize;
        if (nativeBuffers) {
            return compressor.compress(nativeInput, nativeCompressTarget);
        }
        return compressor.compress(input, 0, input.length, compressTarget, 0, compressTarget.length);
    }

    @Benchmark
    public int decompress(BytesCounter counter)
    {
        counter.bytes += blockSize;
        if (nativeBuffers) {
            return decompressor.decompress(nativeCompressed, nativeDecompressTarget);
        }
        return decompressor.decompress(compressed, 0, compressed.length, decompressTarget, 0, decompressTarget.length);
    }

    @TearDown
    public void tearDown()
    {
        if (arena != null) {
            arena.close();
        }
    }
}
