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

import io.airlift.compress.Algorithm;
import io.airlift.compress.Compressor;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.util.Statistics;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class BlockCompressBenchmark
{
    @Param({
            "airlift_lz4",
            "airlift_snappy",
            "xerial_snappy",
            "jpountz_lz4_jni"
    })
    private Algorithm algorithm;

    private Compressor compressor;

    private byte[] compressed;
    private byte[] uncompressed;

    @Setup
    public void setup(DataSet data)
            throws IOException
    {
        uncompressed = data.getUncompressed();
        compressor = algorithm.getCompressor();
        compressed = new byte[compressor.maxCompressedLength(uncompressed.length)];
    }

    @Benchmark
    public int blockCompress(BytesCounter counter)
    {
        int written = compressor.compress(uncompressed, 0, uncompressed.length, compressed, 0, compressed.length);
        counter.add(uncompressed.length);
        return written;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(".*\\." + BlockCompressBenchmark.class.getSimpleName() + ".*")
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Statistics stats = result.getSecondaryResults().get("getBytes").getStatistics();
            System.out.printf("  %-15s  %-10s  %10s ± %10s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getParams().getParam("algorithm"),
                    result.getParams().getParam("name"),
                    Util.toHumanReadableSpeed((long) stats.getMean()),
                    Util.toHumanReadableSpeed((long) stats.getMeanErrorAt(0.999)),
                    stats.getMeanErrorAt(0.999) * 100 / stats.getMean(),
                    stats.getN());
        }
        System.out.println();
    }
}
