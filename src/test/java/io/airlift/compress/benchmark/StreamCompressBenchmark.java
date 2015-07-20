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

import com.google.common.io.Files;
import io.airlift.compress.HadoopNative;
import io.airlift.compress.Util;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.snappy.ByteArrayOutputStream;
import io.airlift.compress.snappy.SnappyCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 10)
@Warmup(iterations = 5)
@Fork(3)
public class StreamCompressBenchmark
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private static final Configuration HADOOP_CONF = new Configuration();

    private byte[] data;

    private byte[] uncompressedBytes;

    private byte[] streamCompressSnappy;
    private final SnappyCodec airliftSnappyCodec = new SnappyCodec();
    private org.apache.hadoop.io.compress.SnappyCodec hadoopSnappyCodec;

    private byte[] streamCompressLz4;
    private final Lz4Codec airliftLz4Codec = new Lz4Codec();
    private org.apache.hadoop.io.compress.Lz4Codec hadoopLz4Codec;

    private byte[] streamCompressLzo;
    private final LzoCodec airliftLzoCodec = new LzoCodec();
    private com.hadoop.compression.lzo.LzoCodec hadoopLzoCodec;

    @Setup
    public void prepare()
            throws IOException
    {
        data = getUncompressedData();
        uncompressedBytes = new byte[data.length];

        // assume stream code will not add more that 10% overhead
        streamCompressSnappy = new byte[(int) (data.length * 1.1) + 8];
        hadoopSnappyCodec = new org.apache.hadoop.io.compress.SnappyCodec();
        hadoopSnappyCodec.setConf(HADOOP_CONF);

        // assume stream code will not add more that 10% overhead
        streamCompressLz4 = new byte[(int) (data.length * 1.1) + 8];
        hadoopLz4Codec = new org.apache.hadoop.io.compress.Lz4Codec();
        hadoopLz4Codec.setConf(HADOOP_CONF);

        // assume stream code will not add more that 10% overhead
        streamCompressLzo = new byte[(int) (data.length * 1.1) + 8];
        hadoopLzoCodec = new com.hadoop.compression.lzo.LzoCodec();
        hadoopLzoCodec.setConf(HADOOP_CONF);
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
        int written;

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamAirliftSnappy(new BytesCounter());
        hadoopDecompress(airliftSnappyCodec, streamCompressSnappy, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream airlift snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamHadoopSnappy(new BytesCounter());
        hadoopDecompress(hadoopSnappyCodec, streamCompressSnappy, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream hadoop snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamAirliftLz4(new BytesCounter());
        hadoopDecompress(airliftLz4Codec, streamCompressLz4, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream airlift lz4");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamHadoopLz4(new BytesCounter());
        hadoopDecompress(hadoopLz4Codec, streamCompressLz4, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream hadoop lz4");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamAirliftLzo(new BytesCounter());
        hadoopDecompress(airliftLzoCodec, streamCompressLzo, 0, written);
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: stream airlift lzo");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        written = streamHadoopLzo(new BytesCounter());
        hadoopDecompress(hadoopLzoCodec, streamCompressLzo, 0, written);
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
        return streamHadoop(counter, airliftLz4Codec, streamCompressLz4);
    }

    @Benchmark
    public int streamHadoopLz4(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopLz4Codec, streamCompressLz4);
    }

    @Benchmark
    public int streamAirliftLzo(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, airliftLzoCodec, streamCompressLzo);
    }

    @Benchmark
    public int streamHadoopLzo(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopLzoCodec, streamCompressLzo);
    }

    @Benchmark
    public int streamAirliftSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, airliftSnappyCodec, streamCompressSnappy);
    }

    @Benchmark
    public int streamHadoopSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopSnappyCodec, streamCompressSnappy);
    }

    private int streamHadoop(BytesCounter counter, CompressionCodec codec, byte[] compressed)
            throws IOException
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(compressed);

        OutputStream out = codec.createOutputStream(byteArrayOutputStream);
        out.write(data, 0, data.length);
        out.close();

        counter.add(uncompressedBytes.length);
        return byteArrayOutputStream.size();
    }

    public static void main(String[] args)
            throws Exception
    {
        StreamCompressBenchmark verifyDecompressor = new StreamCompressBenchmark();
        verifyDecompressor.prepare();
        verifyDecompressor.verify();

        Options opt = new OptionsBuilder()
//                .outputFormat(OutputFormatType.Silent)
                .include(".*\\." + StreamCompressBenchmark.class.getSimpleName() + ".*")
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

    private int hadoopDecompress(CompressionCodec codec, byte[] compressed, int compressedOffset, int compressedLength)
            throws IOException
    {
        InputStream in = codec.createInputStream(new ByteArrayInputStream(compressed, compressedOffset, compressedLength));

        int offset = 0;
        while (offset < uncompressedBytes.length) {
            offset += in.read(uncompressedBytes, offset, uncompressedBytes.length - offset);
        }

        in.close();
        return offset;
    }
}
