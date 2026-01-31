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

import io.airlift.compress.v3.xxhash.XxHash128;
import io.airlift.compress.v3.xxhash.XxHash3Native;
import io.airlift.compress.v3.xxhash.XxHash64JavaHasher;
import io.airlift.compress.v3.xxhash.XxHash64NativeHasher;
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
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing XxHash64 and XxHash3 implementations.
 *
 * <p>Run from Maven:
 * <pre>
 * mvn exec:java -Dexec.mainClass="io.airlift.compress.v3.benchmark.HashBenchmark" -Dexec.classpathScope="test"
 * </pre>
 *
 * <p>To run with specific sizes:
 * <pre>
 * mvn exec:java -Dexec.mainClass="io.airlift.compress.v3.benchmark.HashBenchmark" -Dexec.classpathScope="test" -Dexec.args="-p size=64,1024,65536"
 * </pre>
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
public class HashBenchmark
{
    @Param({"8", "64", "1024", "65536"})
    private int size;

    private byte[] data;
    private MemorySegment segment;

    @Setup
    public void setup()
    {
        data = new byte[size];
        ThreadLocalRandom.current().nextBytes(data);
        segment = MemorySegment.ofArray(data);
    }

    // ========== XxHash64 Java (VarHandle-based) ==========

    @Benchmark
    public long xxhash64_java()
    {
        return XxHash64JavaHasher.hash(data, 0, data.length, 0);
    }

    @Benchmark
    public long xxhash64_java_segment()
    {
        return XxHash64JavaHasher.hash(segment, 0);
    }

    // ========== XxHash64 Native ==========

    @Benchmark
    public long xxhash64_native()
    {
        return XxHash64NativeHasher.hash(data, 0, data.length, 0);
    }

    @Benchmark
    public long xxhash64_native_segment()
    {
        return XxHash64NativeHasher.hash(segment, 0);
    }

    // ========== XxHash3 64-bit Native ==========

    @Benchmark
    public long xxhash3_64_native()
    {
        return XxHash3Native.hash(data);
    }

    @Benchmark
    public long xxhash3_64_native_segment()
    {
        return XxHash3Native.hash(segment);
    }

    // ========== XxHash3 128-bit Native ==========

    @Benchmark
    public XxHash128 xxhash3_128_native()
    {
        return XxHash3Native.hash128(data);
    }

    @Benchmark
    public XxHash128 xxhash3_128_native_segment()
    {
        return XxHash3Native.hash128(segment);
    }

    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        CommandLineOptions parsedOptions = new CommandLineOptions(args);
        new Runner(new OptionsBuilder()
                .parent(parsedOptions)
                .include(".*\\." + HashBenchmark.class.getSimpleName() + ".*")
                .build())
                .run();
    }
}
