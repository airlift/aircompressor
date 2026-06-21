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

import io.airlift.compress.v3.lz4.Lz4Decompressor;
import io.airlift.compress.v3.lz4.Lz4JavaCompressor;
import io.airlift.compress.v3.lz4.Lz4JavaDecompressor;
import io.airlift.compress.v3.lz4.Lz4JavaSafeDecompressor;
import io.airlift.compress.v3.lz4.Lz4NativeDecompressor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the LZ4 decompressor on the {@code MemorySegment} overload with native (off-heap) input and output, as
 * used by mmap-style readers. Each parameter runs in its own fork so the JIT profile is not contaminated across
 * datasets.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 6, time = 1)
@Fork(3)
public class NativeLz4DecompressBenchmark
{
    @Param({"silesia/dickens", "silesia/mozilla"})
    private String name;

    @Param({"java", "java-safe", "native"})
    private String impl;

    private final Arena arena = Arena.ofShared();
    private MemorySegment input;
    private MemorySegment output;
    private int uncompressedLength;
    private Lz4Decompressor decompressor;

    @Setup
    public void setup()
            throws IOException
    {
        decompressor = switch (impl) {
            case "java" -> new Lz4JavaDecompressor();
            case "java-safe" -> new Lz4JavaSafeDecompressor();
            case "native" -> new Lz4NativeDecompressor();
            default -> throw new IllegalArgumentException(impl);
        };

        byte[] raw = Files.readAllBytes(new File("testdata", name).toPath());
        uncompressedLength = raw.length;

        Lz4JavaCompressor compressor = new Lz4JavaCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(raw.length)];
        int compressedLength = compressor.compress(raw, 0, raw.length, compressed, 0, compressed.length);

        // Memory-map the compressed bytes from a file: the decompressor reads input straight off the mapped pages,
        // as a reader over an mmap'd file would.
        Path compressedFile = Files.createTempFile("lz4-bench", ".lz4");
        compressedFile.toFile().deleteOnExit();
        Files.write(compressedFile, Arrays.copyOf(compressed, compressedLength));
        try (FileChannel channel = FileChannel.open(compressedFile, StandardOpenOption.READ)) {
            input = channel.map(FileChannel.MapMode.READ_ONLY, 0, compressedLength, arena);
        }

        output = arena.allocate(raw.length);
    }

    @Benchmark
    public void decompress(BytesCounter counter)
    {
        decompressor.decompress(input, output);
        counter.bytes += uncompressedLength;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        new Runner(new OptionsBuilder()
                .include(".*" + NativeLz4DecompressBenchmark.class.getSimpleName() + ".*")
                .jvmArgsAppend("--enable-native-access=ALL-UNNAMED")
                .build())
                .run();
    }
}
