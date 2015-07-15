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
package io.airlift.compress;

import com.facebook.presto.hadoop.HadoopNative;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.util.Statistics;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class DecompressBenchmark
{
    static {
        PrintStream err = System.err;
        try {
            System.setErr(new PrintStream(ByteStreams.nullOutputStream()));
            HadoopNative.requireHadoopNative();
        }
        finally {
            System.setErr(err);
        }
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    private byte[] data;

    private byte[] uncompressedBytes;

    private HadoopSnappyCodec airliftSnappyCodec;
    private SnappyCodec hadoopSnappyCodec;
    private byte[] blockCompressedSnappy;
    private byte[] streamCompressedAirliftSnappy;
    private byte[] streamCompressedHadoopSnappy;

    @Setup
    public void prepare()
            throws IOException
    {
        data = getUncompressedData();
        uncompressedBytes = new byte[data.length];

        blockCompressedSnappy = compressBlockSnappy(data);

        Arrays.fill(uncompressedBytes, (byte) 0);
        blockAirliftSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        blockXerialSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor");
        }

        airliftSnappyCodec = new HadoopSnappyCodec();
        streamCompressedAirliftSnappy = compressHadoopStream(airliftSnappyCodec, data, 0, data.length);
        Arrays.fill(uncompressedBytes, (byte) 0);
        streamAirliftSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor");
        }

        hadoopSnappyCodec = new SnappyCodec();
        hadoopSnappyCodec.setConf(HADOOP_CONF);
        streamCompressedHadoopSnappy = compressHadoopStream(hadoopSnappyCodec, data, 0, data.length);
        Arrays.fill(uncompressedBytes, (byte) 0);
        streamHadoopSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor");
        }
    }

    private static byte[] getUncompressedData()
            throws IOException
    {
        return Files.toByteArray(new File("testdata/html"));
    }

    @Benchmark
    public int blockAirliftSnappy(BytesCounter counter)
    {
        int read = Snappy.uncompress(blockCompressedSnappy, 0, blockCompressedSnappy.length, uncompressedBytes, 0);
        counter.add(uncompressedBytes.length);
        return read;
    }

    @Benchmark
    public int blockXerialSnappy(BytesCounter counter)
            throws IOException
    {
        int read = org.xerial.snappy.Snappy.uncompress(blockCompressedSnappy, 0, blockCompressedSnappy.length, uncompressedBytes, 0);
        counter.add(uncompressedBytes.length);
        return read;
    }

    @Benchmark
    public int streamAirliftSnappy(BytesCounter counter)
            throws IOException
    {
        InputStream in = airliftSnappyCodec.createInputStream(new ByteArrayInputStream(streamCompressedAirliftSnappy));

        int decompressedOffset = 0;
        while (decompressedOffset < uncompressedBytes.length) {
            decompressedOffset += in.read(uncompressedBytes, decompressedOffset, uncompressedBytes.length - decompressedOffset);
        }

        counter.add(uncompressedBytes.length);
        return decompressedOffset;
    }

    @Benchmark
    public int streamHadoopSnappy(BytesCounter counter)
            throws IOException
    {
        InputStream in = hadoopSnappyCodec.createInputStream(new ByteArrayInputStream(streamCompressedHadoopSnappy));

        int decompressedOffset = 0;
        while (decompressedOffset < uncompressedBytes.length) {
            decompressedOffset += in.read(uncompressedBytes, decompressedOffset, uncompressedBytes.length - decompressedOffset);
        }

        counter.add(uncompressedBytes.length);
        return decompressedOffset;
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class BytesCounter
    {
        private long bytes;

        @Setup(Level.Iteration)
        public void reset()
        {
            bytes = 0;
        }

        public void add(long bytes)
        {
            this.bytes += bytes;
        }

        public long getBytes()
        {
            return bytes;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        new DecompressBenchmark().prepare();

        Options opt = new OptionsBuilder()
//                .outputFormat(OutputFormatType.Silent)
                .include(".*" + DecompressBenchmark.class.getSimpleName() + ".*")
//                .forks(1)
//                .warmupIterations(5)
//                .measurementIterations(10)
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Statistics stats = result.getSecondaryResults().get("getBytes").getStatistics();
            System.out.printf("  %-14s %10s ± %10s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    Util.toHumanReadableSpeed((long) stats.getMean()),
                    Util.toHumanReadableSpeed((long) stats.getMeanErrorAt(0.999)),
                    stats.getMeanErrorAt(0.999) * 100 / stats.getMean(),
                    stats.getN());
        }
        System.out.println();
    }

    private static byte[] compressBlockSnappy(byte[] uncompressed)
            throws IOException
    {
        return Snappy.compress(uncompressed);
    }

    private static byte[] compressHadoopStream(CompressionCodec codec, byte[] uncompressed, int offset, int length)
            throws IOException
    {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        CompressionOutputStream out = codec.createOutputStream(buffer);
        ByteStreams.copy(new ByteArrayInputStream(uncompressed, offset, length), out);
        out.close();
        return buffer.toByteArray();
    }
}
