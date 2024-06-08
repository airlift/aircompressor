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
package io.airlift.compress.benchmark;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.Util;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.util.Statistics;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class CompressionBenchmark
{
    private Compressor compressor;
    private Decompressor decompressor;

    private byte[] uncompressed;
    private byte[] compressed;

    private byte[] compressTarget;
    private byte[] uncompressTarget;

    @Param({
            "airlift_lz4",
            "airlift_lzo",
            "airlift_snappy",
            "airlift_zstd",

            "xerial_snappy",
            "jpountz_lz4_jni",
            "jpountz_lz4_safe",
            "jpountz_lz4_unsafe",
            "hadoop_lzo",
            "zstd_jni",

            "airlift_lz4_stream",
            "airlift_lzo_stream",
            "airlift_snappy_stream",

            "hadoop_lz4_stream",
            "hadoop_lzo_stream",
            "hadoop_snappy_stream",
            "java_zip_stream",
            "hadoop_gzip_stream",
    })
    private Algorithm algorithm;

    @Setup
    public void setup(DataSet data)
            throws IOException
    {
        uncompressed = data.getUncompressed();

        compressor = algorithm.getCompressor();
        compressTarget = new byte[compressor.maxCompressedLength(uncompressed.length)];

        decompressor = algorithm.getDecompressor();
        Compressor compressor = algorithm.getCompressor();
        compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];
        int compressedLength = compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0, compressed.length);
        compressed = Arrays.copyOf(compressed, compressedLength);
        uncompressTarget = new byte[uncompressed.length];
    }

    @Benchmark
    public int compress(BytesCounter counter)
    {
        int written = compressor.compress(uncompressed, 0, uncompressed.length, compressTarget, 0, compressTarget.length);
        counter.bytes += uncompressed.length;
        return written;
    }

    @Benchmark
    public int decompress(BytesCounter counter)
    {
        int written = decompressor.decompress(compressed, 0, compressed.length, uncompressTarget, 0, uncompressTarget.length);
        counter.bytes += uncompressed.length;
        return written;
    }

    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        CommandLineOptions parsedOptions = new CommandLineOptions(args);
        ChainedOptionsBuilder options = new OptionsBuilder()
                .parent(parsedOptions);

        if (parsedOptions.getIncludes().isEmpty()) {
            options = options.include(".*\\." + CompressionBenchmark.class.getSimpleName() + ".*");
        }

        Collection<RunResult> results = new Runner(options.build()).run();

        int count = 0;
        double sum = 0;
        for (RunResult result : results) {
            Statistics stats = result.getSecondaryResults().get("bytes").getStatistics();
            String algorithm = result.getParams().getParam("algorithm");
            String name = result.getParams().getParam("name");

            count++;
            sum += 1 / stats.getMean();

            int compressSize = compressSize(algorithm, name);
            System.out.printf("  %-10s  %-22s  %-25s  %,11d  %10s ± %11s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    algorithm,
                    name,
                    compressSize,
                    Util.toHumanReadableSpeed((long) stats.getMean()),
                    Util.toHumanReadableSpeed((long) stats.getMeanErrorAt(0.999)),
                    stats.getMeanErrorAt(0.999) * 100 / stats.getMean(),
                    stats.getN());
        }
        System.out.println();
        System.out.println("Overall: " + Util.toHumanReadableSpeed((long) (count / sum)));
        System.out.println();
    }

    private static int compressSize(String algorithmName, String name)
    {
        try {
            Compressor compressor = Algorithm.valueOf(algorithmName).getCompressor();
            DataSet dataSet = new DataSet(name);
            dataSet.loadFile();
            byte[] uncompressed = dataSet.getUncompressed();
            byte[] compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];
            return compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0, compressed.length);
        }
        catch (Exception e) {
            return -1;
        }
    }
}
