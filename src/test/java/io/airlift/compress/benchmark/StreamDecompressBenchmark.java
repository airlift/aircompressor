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

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.airlift.compress.HadoopNative;
import io.airlift.compress.Util;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.snappy.SnappyCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class StreamDecompressBenchmark
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    private byte[] data;

    private byte[] uncompressedBytes;

    private Lz4Codec airliftLz4Codec = new Lz4Codec();
    private org.apache.hadoop.io.compress.Lz4Codec hadoopLz4Codec;
    private byte[] streamCompressedLz4;

    private SnappyCodec airliftSnappyCodec;
    private org.apache.hadoop.io.compress.SnappyCodec hadoopSnappyCodec;

    private byte[] streamCompressedAirliftSnappy;
    private byte[] streamCompressedHadoopSnappy;

    private LzoCodec airliftLzoCodec = new LzoCodec();
    private com.hadoop.compression.lzo.LzoCodec hadoopLzoCodec;
    private byte[] streamCompressedLzo;

    @Setup
    public void prepare()
            throws IOException
    {
        data = getUncompressedData();
        uncompressedBytes = new byte[data.length];

        airliftSnappyCodec = new SnappyCodec();
        streamCompressedAirliftSnappy = compressHadoopStream(airliftSnappyCodec, data, 0, data.length);

        hadoopSnappyCodec = new org.apache.hadoop.io.compress.SnappyCodec();
        hadoopSnappyCodec.setConf(HADOOP_CONF);
        streamCompressedHadoopSnappy = compressHadoopStream(hadoopSnappyCodec, data, 0, data.length);

        hadoopLz4Codec = new org.apache.hadoop.io.compress.Lz4Codec();
        hadoopLz4Codec.setConf(HADOOP_CONF);
        streamCompressedLz4 = compressStreamLz4(data);

        hadoopLzoCodec = new com.hadoop.compression.lzo.LzoCodec();
        hadoopLzoCodec.setConf(HADOOP_CONF);
        streamCompressedLzo = compressStreamLzo(data);
    }

    /**
     * DO NOT call this from prepare!
     * Verify the implementations are working.  This executes all implementation code,
     * which can cause some implementations (e.g. Hadoop) to become highly virtualized
     * and thus slow.
     */
    public void verify()
            throws IOException
    {
        Arrays.fill(uncompressedBytes, (byte) 0);
        streamAirliftSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream airlift snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        streamHadoopSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream hadoop snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        streamAirliftLz4(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block airlift lz4 stream");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        streamHadoopLz4(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block hadoop lz4 stream");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        streamAirliftLzo(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream airlift lzo");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        streamHadoopLzo(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream hadoop lzo");
        }
    }

    private static byte[] getUncompressedData()
            throws IOException
    {
        return Files.toByteArray(new File("testdata/html"));
    }

    @Benchmark
    public int streamAirliftLz4(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, airliftLz4Codec, streamCompressedLz4);
    }

    @Benchmark
    public int streamHadoopLz4(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopLz4Codec, streamCompressedLz4);
    }

    @Benchmark
    public int streamAirliftSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, airliftSnappyCodec, streamCompressedAirliftSnappy);
    }

    @Benchmark
    public int streamHadoopSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopSnappyCodec, streamCompressedHadoopSnappy);
    }

    @Benchmark
    public int streamAirliftLzo(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, airliftLzoCodec, streamCompressedLzo);
    }

    @Benchmark
    public int streamHadoopLzo(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopLzoCodec, streamCompressedLzo);
    }

    private int streamHadoop(BytesCounter counter, CompressionCodec codec, byte[] compressed)
            throws IOException
    {
        InputStream in = codec.createInputStream(new ByteArrayInputStream(compressed));

        int decompressedOffset = 0;
        while (decompressedOffset < uncompressedBytes.length) {
            decompressedOffset += in.read(uncompressedBytes, decompressedOffset, uncompressedBytes.length - decompressedOffset);
        }

        counter.add(uncompressedBytes.length);
        return decompressedOffset;
    }

    public static void main(String[] args)
            throws Exception
    {
        StreamDecompressBenchmark verifyDecompressor = new StreamDecompressBenchmark();
        verifyDecompressor.prepare();
        verifyDecompressor.verify();

        Options opt = new OptionsBuilder()
//                .outputFormat(OutputFormatType.Silent)
                .include(".*\\." + StreamDecompressBenchmark.class.getSimpleName() + ".*")
//                .forks(1)
//                .warmupIterations(5)
//                .measurementIterations(10)
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Statistics stats = result.getSecondaryResults().get("getBytes").getStatistics();
            System.out.printf("  %-19s %10s ± %10s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    Util.toHumanReadableSpeed((long) stats.getMean()),
                    Util.toHumanReadableSpeed((long) stats.getMeanErrorAt(0.999)),
                    stats.getMeanErrorAt(0.999) * 100 / stats.getMean(),
                    stats.getN());
        }
        System.out.println();
    }

    private byte[] compressStreamLz4(byte[] uncompressed)
            throws IOException
    {
        return compressHadoopStream(hadoopLz4Codec, uncompressed, 0, uncompressed.length);
    }

    private static byte[] compressStreamLzo(byte[] uncompressed)
            throws IOException
    {
        com.hadoop.compression.lzo.LzoCodec codec = new com.hadoop.compression.lzo.LzoCodec();
        codec.setConf(HADOOP_CONF);

        return compressHadoopStream(codec, uncompressed, 0, uncompressed.length);
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
