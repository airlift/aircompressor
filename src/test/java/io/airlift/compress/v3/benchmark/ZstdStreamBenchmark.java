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

import io.airlift.compress.v3.zstd.ZstdJavaInputStream;
import io.airlift.compress.v3.zstd.ZstdJavaOutputStream;
import io.airlift.compress.v3.zstd.ZstdNativeInputStream;
import io.airlift.compress.v3.zstd.ZstdNativeOutputStream;
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
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.min;

/**
 * Compares the pure Java and native Zstd streams. Both decompress the same bytes,
 * which the native stream compressed at the default level.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class ZstdStreamBenchmark
{
    public enum Implementation
    {
        java,
        nativeLibrary
    }

    @Param({"java", "nativeLibrary"})
    private Implementation implementation;

    // bytes per write and per read
    @Param({"4096", "65536"})
    private int chunkSize;

    private byte[] uncompressed;
    private byte[] compressed;
    private byte[] readBuffer;
    private final ReusableOutputStream sink = new ReusableOutputStream();

    @Setup
    public void setup(DataSet data)
            throws IOException
    {
        uncompressed = data.getUncompressed();
        readBuffer = new byte[chunkSize];

        ReusableOutputStream compressedOutput = new ReusableOutputStream();
        try (OutputStream out = new ZstdNativeOutputStream(compressedOutput)) {
            out.write(uncompressed);
        }
        compressed = compressedOutput.toByteArray();
    }

    @Benchmark
    public int compress(BytesCounter counter)
            throws IOException
    {
        sink.reset();
        try (OutputStream out = implementation == Implementation.java ? new ZstdJavaOutputStream(sink) : new ZstdNativeOutputStream(sink)) {
            for (int offset = 0; offset < uncompressed.length; offset += chunkSize) {
                out.write(uncompressed, offset, min(chunkSize, uncompressed.length - offset));
            }
        }
        counter.bytes += uncompressed.length;
        return sink.size();
    }

    @Benchmark
    public int decompress(BytesCounter counter)
            throws IOException
    {
        int total = 0;
        try (InputStream in = implementation == Implementation.java ? new ZstdJavaInputStream(new ByteArrayInputStream(compressed)) : new ZstdNativeInputStream(new ByteArrayInputStream(compressed))) {
            int read;
            while ((read = in.read(readBuffer)) != -1) {
                total += read;
            }
        }
        if (total != uncompressed.length) {
            throw new IllegalStateException("Read " + total + " bytes instead of " + uncompressed.length);
        }
        counter.bytes += total;
        return total;
    }

    private static class ReusableOutputStream
            extends ByteArrayOutputStream
    {
        ReusableOutputStream()
        {
            super(1 << 20);
        }
    }

    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        CommandLineOptions parsedOptions = new CommandLineOptions(args);
        ChainedOptionsBuilder options = new OptionsBuilder()
                .parent(parsedOptions);

        if (parsedOptions.getIncludes().isEmpty()) {
            options = options.include(".*\\." + ZstdStreamBenchmark.class.getSimpleName() + ".*");
        }

        new Runner(options.build()).run();
    }
}
