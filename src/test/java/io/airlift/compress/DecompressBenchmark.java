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
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.snappy.Snappy;
import io.airlift.compress.snappy.SnappyCodec;
import io.airlift.compress.snappy.SnappyDecompressor;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
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

    private final LZ4SafeDecompressor jpountzLz4JniDecompressor = LZ4Factory.fastestInstance().safeDecompressor();
    private final Lz4Decompressor airliftLz4Decompressor = new Lz4Decompressor();

    private SnappyCodec airliftSnappyCodec;
    private org.apache.hadoop.io.compress.SnappyCodec hadoopSnappyCodec;

    private byte[] blockCompressedSnappy;
    private byte[] streamCompressedAirliftSnappy;
    private byte[] streamCompressedHadoopSnappy;
    private byte[] blockCompressedLz4;

    @Setup
    public void prepare()
            throws IOException
    {
        data = getUncompressedData();
        uncompressedBytes = new byte[data.length];

        blockCompressedSnappy = compressBlockSnappy(data);

        airliftSnappyCodec = new SnappyCodec();
        streamCompressedAirliftSnappy = compressHadoopStream(airliftSnappyCodec, data, 0, data.length);

        hadoopSnappyCodec = new org.apache.hadoop.io.compress.SnappyCodec();
        hadoopSnappyCodec.setConf(HADOOP_CONF);
        streamCompressedHadoopSnappy = compressHadoopStream(hadoopSnappyCodec, data, 0, data.length);

        blockCompressedLz4 = compressBlockLz4(data);
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
        blockAirliftSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block airlift snappy");
        }

        Arrays.fill(uncompressedBytes, (byte) 0);
        blockXerialSnappy(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block serial snappy");
        }

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
        blockAirliftLz4(new BytesCounter());
        if (!Arrays.equals(data, uncompressedBytes)) {
            throw new IllegalStateException("broken decompressor: block airlift lz4");
        }
    }

    private static byte[] getUncompressedData()
            throws IOException
    {
        return Files.toByteArray(new File("testdata/html"));
    }

    @Benchmark
    public int blockAirliftLz4(BytesCounter counter)
    {
        int read = airliftLz4Decompressor.decompress(blockCompressedLz4, 0, blockCompressedLz4.length, uncompressedBytes, 0, uncompressedBytes.length);
        counter.add(uncompressedBytes.length);
        return read;
    }

    @Benchmark
    public int blockJpountzLz4Jni(BytesCounter counter)
    {
        int read = jpountzLz4JniDecompressor.decompress(blockCompressedLz4, uncompressedBytes);
        counter.add(uncompressedBytes.length);
        return read;
    }

    @Benchmark
    public int blockAirliftSnappy(BytesCounter counter)
    {
        int read = SnappyDecompressor.uncompress(blockCompressedSnappy, 0, blockCompressedSnappy.length, uncompressedBytes, 0);
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
        return streamHadoop(counter, airliftSnappyCodec, streamCompressedAirliftSnappy);
    }

    @Benchmark
    public int streamHadoopSnappy(BytesCounter counter)
            throws IOException
    {
        return streamHadoop(counter, hadoopSnappyCodec, streamCompressedHadoopSnappy);
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
        DecompressBenchmark verifyDecompressor = new DecompressBenchmark();
        verifyDecompressor.prepare();
        verifyDecompressor.verify();

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
            System.out.printf("  %-19s %10s ± %10s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
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

    private static byte[] compressBlockLz4(byte[] uncompressed)
            throws IOException
    {
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        byte[] compressedBytes = new byte[compressor.maxCompressedLength(uncompressed.length)];
        int compressedLength = compressor.compress(uncompressed, 0, uncompressed.length, compressedBytes, 0);

        return Arrays.copyOf(compressedBytes, compressedLength);
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
