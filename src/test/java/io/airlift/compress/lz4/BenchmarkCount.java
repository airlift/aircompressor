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
package io.airlift.compress.lz4;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class BenchmarkCount
{
    @Param({"1", "3", "7", "15", "127", "511"})
    private int matchLength;

    @Param({"0", "1", "3", "7", "50"})
    private int padding;

    private byte[] data;

    @Setup
    public void setup()
    {
        int size = (matchLength + 1) * 2 + padding;
        data = new byte[size];

        byte[] pattern = new byte[matchLength];
        ThreadLocalRandom.current().nextBytes(pattern);

        System.arraycopy(pattern, 0, data, 0, matchLength);
        data[matchLength] = 1;

        System.arraycopy(pattern, 0, data, matchLength + 1, matchLength);
        data[matchLength + 1 + matchLength] = 2;
    }

    @Benchmark
    public long count()
    {
        return Lz4RawCompressor.count(data, ARRAY_BYTE_BASE_OFFSET + matchLength + 1, ARRAY_BYTE_BASE_OFFSET + data.length, ARRAY_BYTE_BASE_OFFSET);
    }

    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        CommandLineOptions parsedOptions = new CommandLineOptions(args);
        ChainedOptionsBuilder options = new OptionsBuilder()
                .parent(parsedOptions);

        if (parsedOptions.getIncludes().isEmpty()) {
            options = options.include(".*\\." + BenchmarkCount.class.getSimpleName() + ".*");
        }

        new Runner(options.build()).run();
    }
}
